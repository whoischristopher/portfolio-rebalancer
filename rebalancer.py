"""
rebalancer.py – Database adapter for the rebalancing optimiser.

The maths lives in services/optimizer.py (pure, unit-tested).  This module:
  1. snapshots a portfolio's accounts, holdings, targets and security preferences
     into an optimizer.Problem,
  2. runs the solve (twice in "Ask" mode – see below),
  3. replaces the portfolio's pending RebalanceTransactions with the new plan, and
  4. returns a JSON-serialisable summary for the Rebalance page.

Taxable-sell modes (Portfolio.taxable_sell_mode):
  never – sells only happen in registered accounts (plus, with Portfolio.allow_loss_sells,
          taxable holdings at a loss – they realise no gain)
  allow – taxable sells are allowed (still minimised via realised-gain cost)
  ask   – plan WITHOUT taxable sells; if some asset class is still outside its
          band, also solve WITH them and offer that plan (gain realised, trades,
          what it fixes) so the user can opt in from the Rebalance page.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import and_, or_

from extensions import db
from models import (Account, Holding, PendingRedemption, RebalanceTransaction,
                    Security, SecurityPreference, Target)
from services.fx import convert_to_base, get_exchange_rates
from services.optimizer import AccountIn, PositionIn, Problem, SecurityIn, solve
from services.prices import normalize_ticker

log = logging.getLogger(__name__)

SUPERFICIAL_LOSS_DAYS = 30
TAXABLE_MODES = ("never", "ask", "allow")


# --------------------------------------------------------------------------- #
# Snapshot
# --------------------------------------------------------------------------- #
def _buy_levels(portfolio, accounts, securities) -> dict:
    """(account_id, security_id) -> priority level (0 = best). Missing = may not buy.

    unrestricted / no preference   → level 0 everywhere
    restricted_to_accounts         → level 0 in allowed accounts, forbidden elsewhere
    prioritized_accounts           → priority_1/2/3 → level 0/1/2, unlisted accounts → 3
                                     (a soft preference, never a hard exclusion)
    """
    prefs = {p.security_id: p for p in SecurityPreference.query.filter_by(portfolio_id=portfolio.id).all()}
    levels = {}
    for sec in securities:
        pref = prefs.get(sec.id)
        rtype = pref.restriction_type if pref else "unrestricted"
        cfg = (pref.account_config or {}) if pref else {}
        for acc in accounts:
            if rtype == "restricted_to_accounts":
                if acc.id in cfg.get("allowed", []):
                    levels[(acc.id, sec.id)] = 0
            elif rtype == "prioritized_accounts":
                lvl = next((n - 1 for n in (1, 2, 3) if acc.id in cfg.get(f"priority_{n}", [])), 3)
                levels[(acc.id, sec.id)] = lvl
            else:
                levels[(acc.id, sec.id)] = 0
    return levels


def build_problem(portfolio, exchange_rates, allow_taxable_sells: bool) -> tuple[Problem, list[str]]:
    base = portfolio.base_currency or "CAD"
    notes: list[str] = []
    accounts = Account.query.filter_by(portfolio_id=portfolio.id).all()
    targets = Target.query.filter_by(portfolio_id=portfolio.id).all()
    class_ids = {t.asset_class_id for t in targets}

    # One price per security (Security.last_price); a held copy is the fallback.
    holding_price = {}
    for acc in accounts:
        for h in acc.holdings:
            if h.price and h.price > 0:
                holding_price.setdefault(h.security_id, h.price)

    sec_in, securities = [], []
    unpriced = []
    candidates = Security.query.filter(Security.portfolio_id == portfolio.id,
                                       Security.asset_class_id.in_(class_ids)).all() if class_ids else []
    for sec in candidates:
        native = sec.last_price or holding_price.get(sec.id)
        if not native or native <= 0:
            unpriced.append(sec.ticker)
            continue
        securities.append(sec)
        sec_in.append(SecurityIn(
            id=sec.id, ticker=sec.ticker, asset_class_id=sec.asset_class_id,
            price=convert_to_base(native, sec.currency, base, exchange_rates),
            price_native=native, currency=sec.currency,
            sellable=not bool(getattr(sec, "is_illiquid", False)),
            managed=bool(getattr(sec, "is_managed", False)),
        ))
    if unpriced:
        notes.append("No price for " + ", ".join(sorted(unpriced)) +
                     " (not held and not in your price sheet) – they can't be bought this time.")
    levels = _buy_levels(portfolio, accounts, securities)
    sl_groups = _apply_migrations(portfolio, sec_in, securities, accounts, levels, notes)

    acc_in = [AccountIn(
        id=a.id, name=a.name, taxable=not a.is_registered, currency=a.currency or base,
        cash=convert_to_base(a.cash_balance or 0.0, a.currency or base, base, exchange_rates),
    ) for a in accounts]

    # Units with a redemption request filed are on their way out: plan as if gone.
    pending = {}
    for r in PendingRedemption.query.filter_by(portfolio_id=portfolio.id, status="pending").all():
        pending[(r.account_id, r.security_id)] = pending.get((r.account_id, r.security_id), 0.0) + r.quantity
    pos_in = []
    for acc in accounts:
        for h in acc.holdings:
            q = h.quantity - pending.get((acc.id, h.security_id), 0.0)
            if q <= 1e-9:
                continue
            # Book value is stored in the security's currency; the optimiser works in base.
            bv = None if h.book_value is None else convert_to_base(
                h.book_value * q / h.quantity, h.security.currency, base, exchange_rates)
            pos_in.append(PositionIn(
                account_id=acc.id, security_id=h.security_id, quantity=q, book_value=bv,
            ))
    if pending:
        notes.append("Units with a pending redemption request are left out of this plan "
                     "(treated as already redeemed).")

    recent_buys, recent_loss_sells = _superficial_loss_history(portfolio, notes)
    # A phased-out security and its replacements treated as identical property: a recent
    # buy or loss sale of any of them counts for all.
    for group in sl_groups:
        for seen in (recent_buys, recent_loss_sells):
            if seen & group:
                seen |= group

    problem = Problem(
        accounts=acc_in, securities=sec_in, positions=pos_in,
        targets={t.asset_class_id: t.target_percentage for t in targets},
        class_names={t.asset_class_id: t.asset_class.name for t in targets},
        buy_levels=levels,
        threshold_pp=portfolio.balanced_threshold if portfolio.balanced_threshold is not None else 0.5,
        trade_cost=portfolio.trade_cost or 0.0,
        allow_taxable_sells=allow_taxable_sells,
        allow_loss_sells=portfolio.allow_loss_sells is not False,
        recent_buys=recent_buys,
        recent_loss_sells=recent_loss_sells,
        fractional_final=bool(portfolio.precision_rebalancing),
        base_currency=portfolio.base_currency or 'CAD',
    )
    return problem, notes


def replacement_map(portfolio, security, accounts, securities, levels=None, prefs=None, priced_ids=None,
                    auto_only=False) -> dict:
    """Where *security* is being phased out, and into what: {account_id: (replacement, problem)}.

    Per account: its own override wins (replacement None = keep it there); otherwise the
    fixed default (replaced_by); otherwise, with phase_out_auto, the security of the same
    asset class the account preferences favour in that account:
      lowest priority level there (Restricted-to → 0, Prioritized P1/P2/P3 → 0/1/2,
      unlisted → 3, no preference → 0), then one whose preference names this account
      (more specific than "unrestricted"), then the most already held in that account,
      then in the portfolio, then by ticker.
    *problem* says why an explicit choice can't be used this time (unpriced, other class,
    restricted there) or that automatic found nothing; accounts not phased out are left out.
    priced_ids, if given, limits replacements to securities that can be bought this time.
    auto_only: ignore the saved choices and show what the automatic choice would pick."""
    if levels is None:
        levels = _buy_levels(portfolio, accounts, securities)
    if prefs is None:
        prefs = {p.security_id: p for p in SecurityPreference.query.filter_by(portfolio_id=portfolio.id).all()}
    held_in: dict[tuple[int, int], float] = {}
    held_all: dict[int, float] = {}
    for acc in accounts:
        for h in acc.holdings:
            v = h.quantity * (h.security.last_price or h.price or 0.0)
            held_in[(acc.id, h.security_id)] = held_in.get((acc.id, h.security_id), 0.0) + v
            held_all[h.security_id] = held_all.get(h.security_id, 0.0) + v
    klass = security.asset_class.name if security.asset_class else "asset class"

    def names_account(sec, acc_id):
        pref = prefs.get(sec.id)
        if not pref:
            return False
        cfg = pref.account_config or {}
        if pref.restriction_type == "restricted_to_accounts":
            return acc_id in cfg.get("allowed", [])
        if pref.restriction_type == "prioritized_accounts":
            return any(acc_id in cfg.get(f"priority_{n}", []) for n in (1, 2, 3))
        return False

    out = {}
    for acc in accounts:
        ov = None if auto_only else security.override_for(acc.id)
        if not auto_only and (ov is not None or security.replaced_by_id):
            new = ov.replacement if ov is not None else security.replaced_by
            if new is None:
                continue                                            # kept in this account
            problem = None
            if new.asset_class_id != security.asset_class_id:
                problem = f"{new.ticker} isn't in the same asset class"
            elif priced_ids is not None and new.id not in priced_ids:
                problem = f"{new.ticker} has no price"
            elif (acc.id, new.id) not in levels:
                problem = f"{new.ticker} isn't allowed there (Securities → restrictions)"
            out[acc.id] = (new, problem)
        elif auto_only or security.phase_out_auto:
            cands = [s for s in securities
                     if s.id != security.id and s.asset_class_id == security.asset_class_id
                     and not s.is_phasing_out and (acc.id, s.id) in levels
                     and (priced_ids is None or s.id in priced_ids)]
            if not cands:
                out[acc.id] = (None, f"no other {klass} security can be bought there")
                continue
            best = min(cands, key=lambda s: (levels[(acc.id, s.id)], 0 if names_account(s, acc.id) else 1,
                                             -held_in.get((acc.id, s.id), 0.0), -held_all.get(s.id, 0.0),
                                             s.ticker))
            out[acc.id] = (best, None)
    return out


def _apply_migrations(portfolio, sec_in, securities, accounts, levels, notes) -> list[set]:
    """Mark phased-out securities on the optimiser's inputs, account by account (see
    replacement_map). An account whose replacement can't be used this time is paused:
    the old security is treated normally there and a note says why.
    Returns groups of security ids to treat as identical for the superficial-loss rule."""
    by_id = {s.id: s for s in sec_in}
    prefs = {p.security_id: p for p in SecurityPreference.query.filter_by(portfolio_id=portfolio.id).all()}
    acc_name = {a.id: a.name for a in accounts}
    parent: dict[int, int] = {}

    def find(x):
        while parent.get(x, x) != x:
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    for sec in securities:
        if not sec.is_phasing_out or sec.id not in by_id:
            continue
        old = by_id[sec.id]
        paused: dict[str, list[str]] = {}          # reason → account names
        for acc_id, (new, problem) in replacement_map(portfolio, sec, accounts, securities, levels, prefs,
                                                      priced_ids=set(by_id)).items():
            if problem:
                paused.setdefault(problem, []).append(acc_name[acc_id])
                continue
            old.replaced_in[acc_id] = new.id
            if sec.migration_sl_link is not False:
                union(old.id, new.id)
        for reason, names in sorted(paused.items()):
            notes.append(f"Phasing out {sec.ticker} is paused in {', '.join(sorted(names))}: {reason}, so "
                         f"{sec.ticker} is treated as a normal holding there this time.")
        old.switch_registered_now = bool(sec.migrate_registered_now)

    groups: dict[int, set] = {}
    for x in list(parent) + list(parent.values()):
        groups.setdefault(find(x), set()).add(x)
    for root, members in groups.items():
        for m in members:
            by_id[m].sl_group = root
    return [g for g in groups.values() if len(g) > 1]


def _managed_book_value_notes(portfolio) -> list[str]:
    """Warnings for provider-managed positions (e.g. direct indexing) in taxable accounts
    whose book value is missing, undated or old. The provider's own trades (tax-loss
    harvesting above all) lower the book value without anything being recorded here, so
    an old figure understates the gain a withdrawal would realise."""
    out = []
    for acc in Account.query.filter_by(portfolio_id=portfolio.id, is_registered=False).all():
        for h in acc.holdings:
            if not h.security.is_managed or not h.book_value_stale:
                continue
            where = f"{h.security.ticker} in {acc.name}"
            if h.book_value is None:
                out.append(f"{where}: no book value – a withdrawal is treated as 100% gain. "
                           "Enter the total book cost from your latest statement.")
            elif h.book_value_as_of is None:
                out.append(f"{where}: book value has no date, so it may be out of date. Re-enter it with "
                           "the date of the statement it came from.")
            else:
                out.append(f"{where}: book value is {h.book_value_age_days} days old "
                           f"(as of {h.book_value_as_of.isoformat()}). The provider's tax-loss harvesting "
                           "lowers it over time, so the gain on a withdrawal is probably understated – "
                           "update it from your latest statement.")
    return out


def _superficial_loss_history(portfolio, notes: list) -> tuple[set, set]:
    """Security ids (of THIS portfolio) bought, or sold at a loss, in the last 30 days.

    Optionally (Settings → superficial-loss link, off by default) also counts trades
    recorded in one other portfolio of the same login – e.g. a spouse's, who is an
    affiliated person under the rule. Portfolios have separate security lists, so the
    linked trades are matched to this portfolio's securities by ticker."""
    since = datetime.utcnow() - timedelta(days=SUPERFICIAL_LOSS_DAYS)
    T = RebalanceTransaction

    def recent(portfolio_id):
        # The window runs from the date the trade was placed (trade_date), which can be
        # earlier than when it was recorded here; older rows without one fall back.
        return T.query.filter(
            T.portfolio_id == portfolio_id,
            T.executed.is_(True),
            or_(T.trade_date >= since.date(),
                and_(T.trade_date.is_(None), T.executed_at >= since)),
        ).all()

    def split(txns, sec_id_of):
        buys, loss_sells = set(), set()
        for t in txns:
            sid = sec_id_of(t)
            if not sid:
                continue
            if t.action == "BUY":
                buys.add(sid)
            elif t.action == "SELL" and (t.realized_gain or 0) < 0:
                loss_sells.add(sid)
        return buys, loss_sells

    buys, loss_sells = split(recent(portfolio.id), lambda t: t.security_id)

    link = portfolio.sl_link_portfolio
    if link is not None and link.id != portfolio.id and link.user_id == portfolio.user_id:
        mine = {normalize_ticker(s.ticker): s.id
                for s in Security.query.filter_by(portfolio_id=portfolio.id).all()}
        b2, l2 = split(recent(link.id),
                       lambda t: t.security and mine.get(normalize_ticker(t.security.ticker)))
        if b2 or l2:
            notes.append(f'Superficial-loss check also covers recent trades recorded in "{link.name}".')
        buys |= b2
        loss_sells |= l2
    return buys, loss_sells


# --------------------------------------------------------------------------- #
# Plan generation
# --------------------------------------------------------------------------- #
def _summarise(result, problem) -> dict:
    names = problem.class_names
    return {
        "drift": [
            {"name": names[k], "before": round(result.drift_before[k], 2),
             "after": round(result.drift_after[k], 2), "target": problem.targets[k],
             "effective_target": round(result.effective_targets[k], 2),
             "out_of_band": k in result.out_of_band}
            for k in sorted(problem.targets, key=lambda k: -problem.targets[k])
        ],
        "trades": len(result.trades),
        "taxable_sells": result.taxable_sells,
        "realized_gain": round(result.realized_gain, 2),
        "realized_loss": round(getattr(result, "realized_loss", 0.0), 2),
        "notes": result.notes,
        "status": result.status,
        "max_out_of_band": round(result.max_out_of_band, 2),
    }


def _to_transactions(portfolio, result) -> list:
    acc_names = {a.id: a.name for a in Account.query.filter_by(portfolio_id=portfolio.id).all()}
    buy_volume = {}
    for t in result.trades:
        if t.action == "BUY":
            buy_volume[t.account_id] = buy_volume.get(t.account_id, 0.0) + t.amount_base
    # Accounts with the biggest buys first; within an account, sells before buys.
    ordered = sorted(result.trades, key=lambda t: (
        -buy_volume.get(t.account_id, 0.0), acc_names.get(t.account_id, ""),
        0 if t.action == "SELL" else 1, -t.amount_base))
    txns = []
    for n, t in enumerate(ordered, 1):
        txns.append(RebalanceTransaction(
            user_id=portfolio.user_id, portfolio_id=portfolio.id,
            account_id=t.account_id, security_id=t.security_id,
            action=t.action, quantity=t.quantity, price=t.price_native,
            amount=round(t.quantity * t.price_native, 2), currency=t.currency,
            execution_order=n, is_final_trade=t.is_final,
            realized_gain=t.realized_gain, requires_user_selection=False,
        ))
    return txns


def redemption_options(portfolio, result, problem, exchange_rates) -> list:
    """Turn per-class redemption amounts into concrete holdings to redeem from.
    Registered accounts first (no tax on redemption), then the taxable lots with the
    smallest gain per dollar; within that, larger holdings first."""
    base = portfolio.base_currency or "CAD"
    pending = {}
    for r in PendingRedemption.query.filter_by(portfolio_id=portfolio.id, status="pending").all():
        pending[(r.account_id, r.security_id)] = pending.get((r.account_id, r.security_id), 0.0) + r.quantity
    out = []
    for k, amount in result.redemptions.items():
        holdings = []
        for acc in Account.query.filter_by(portfolio_id=portfolio.id).all():
            for h in acc.holdings:
                sec = h.security
                if sec.asset_class_id != k or not sec.is_illiquid:
                    continue
                avail = h.quantity - pending.get((acc.id, sec.id), 0.0)
                price = sec.last_price or h.price
                if avail <= 1e-9 or not price:
                    continue
                price_b = convert_to_base(price, sec.currency, base, exchange_rates)
                gain_frac = 0.0
                if not acc.is_registered:
                    gain_frac = 1.0 if h.book_value is None else max(0.0, 1 - h.book_value / (h.quantity * price))
                holdings.append(dict(account_id=acc.id, account=acc.name, security_id=sec.id, ticker=sec.ticker,
                                     available=round(avail, 4), price=price, currency=sec.currency,
                                     price_base=price_b, registered=bool(acc.is_registered), gain_frac=gain_frac))
        holdings.sort(key=lambda o: (not o["registered"], o["gain_frac"], -o["available"] * o["price_base"]))
        left = amount
        for o in holdings:
            units = min(o["available"], left / o["price_base"]) if left > 1 else 0.0
            o["suggested_units"] = round(units, 4)
            o["suggested_value"] = round(units * o["price"], 2)
            left -= units * o["price_base"]
        out.append({"asset_class": problem.class_names[k], "amount": round(amount, 2),
                    "holdings": [o for o in holdings if o["suggested_units"] > 0]})
    return out


def generate_plan(portfolio, taxable_override: str | None = None) -> tuple[list, dict]:
    """Solve, persist the plan, and return (transactions, summary)."""
    mode = taxable_override or portfolio.taxable_sell_mode or "ask"
    if mode not in TAXABLE_MODES:
        mode = "ask"
    fx = get_exchange_rates(portfolio)

    problem, notes = build_problem(portfolio, fx, allow_taxable_sells=(mode == "allow"))
    if not problem.targets:
        raise ValueError("No targets defined – set asset-class targets first.")
    result = solve(problem)
    summary = _summarise(result, problem)
    summary["mode"] = mode
    summary["notes"] = notes + summary["notes"]
    summary["redemptions"] = redemption_options(portfolio, result, problem, fx)

    # Ask mode: would allowing taxable sells fix what's left?
    if mode == "ask" and result.out_of_band:
        alt_problem, _ = build_problem(portfolio, fx, allow_taxable_sells=True)
        alt = solve(alt_problem)
        if alt.taxable_sells and (len(alt.out_of_band) < len(result.out_of_band)
                                  or alt.max_out_of_band < result.max_out_of_band - 0.05):
            summary["offer"] = {
                "fixes": [problem.class_names[k] for k in result.out_of_band if k not in alt.out_of_band],
                "still_out": [problem.class_names[k] for k in alt.out_of_band],
                "taxable_sells": alt.taxable_sells,
                "realized_gain": round(alt.realized_gain, 2),
                "trades": len(alt.trades),
            }

    # Taxable sells in play (allowed, or offered): flag managed positions whose ACB is stale.
    if mode == "allow" or "offer" in summary:
        summary["notes"] += _managed_book_value_notes(portfolio)

    txns = _to_transactions(portfolio, result)
    RebalanceTransaction.query.filter_by(portfolio_id=portfolio.id, executed=False).delete()
    for t in txns:
        db.session.add(t)
    db.session.commit()

    log.info("Rebalance plan (portfolio %s): mode=%s trades=%d taxable_sells=%d gain=%.2f out_of_band=%s status=%s",
             portfolio.id, mode, len(txns), result.taxable_sells, result.realized_gain,
             [problem.class_names[k] for k in result.out_of_band], result.status)
    return txns, summary


def generate_rebalance_transactions(portfolio, taxable_override: str | None = None) -> list:
    """Backwards-compatible entry point."""
    return generate_plan(portfolio, taxable_override)[0]
