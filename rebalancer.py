"""
rebalancer.py – Database adapter for the rebalancing optimiser.

The maths lives in services/optimizer.py (pure, unit-tested).  This module:
  1. snapshots the user's accounts, holdings, targets and security preferences
     into an optimizer.Problem,
  2. runs the solve (twice in "Ask" mode – see below),
  3. replaces the user's pending RebalanceTransactions with the new plan, and
  4. returns a JSON-serialisable summary for the Rebalance page.

Taxable-sell modes (User.taxable_sell_mode):
  never – sells only happen in registered accounts
  allow – taxable sells are allowed (still minimised via realised-gain cost)
  ask   – plan WITHOUT taxable sells; if some asset class is still outside its
          band, also solve WITH them and offer that plan (gain realised, trades,
          what it fixes) so the user can opt in from the Rebalance page.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from extensions import db
from models import Account, Holding, RebalanceTransaction, Security, SecurityPreference, Target
from services.fx import convert_to_base, get_exchange_rates
from services.optimizer import AccountIn, PositionIn, Problem, SecurityIn, solve

log = logging.getLogger(__name__)

SUPERFICIAL_LOSS_DAYS = 30
TAXABLE_MODES = ("never", "ask", "allow")


# --------------------------------------------------------------------------- #
# Snapshot
# --------------------------------------------------------------------------- #
def _buy_levels(user, accounts, securities) -> dict:
    """(account_id, security_id) -> priority level (0 = best). Missing = may not buy.

    unrestricted / no preference   → level 0 everywhere
    restricted_to_accounts         → level 0 in allowed accounts, forbidden elsewhere
    prioritized_accounts           → priority_1/2/3 → level 0/1/2, unlisted accounts → 3
                                     (a soft preference, never a hard exclusion)
    """
    prefs = {p.security_id: p for p in SecurityPreference.query.filter_by(user_id=user.id).all()}
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


def build_problem(user, exchange_rates, allow_taxable_sells: bool) -> tuple[Problem, list[str]]:
    base = user.base_currency or "CAD"
    notes: list[str] = []
    accounts = Account.query.filter_by(user_id=user.id).all()
    targets = Target.query.filter_by(user_id=user.id).all()
    class_ids = {t.asset_class_id for t in targets}

    # One price per security (Security.last_price); a held copy is the fallback.
    holding_price = {}
    for acc in accounts:
        for h in acc.holdings:
            if h.price and h.price > 0:
                holding_price.setdefault(h.security_id, h.price)

    sec_in, securities = [], []
    unpriced = []
    for sec in Security.query.filter(Security.asset_class_id.in_(class_ids)).all() if class_ids else []:
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
        ))
    if unpriced:
        notes.append("No price for " + ", ".join(sorted(unpriced)) +
                     " (not held and not in your price sheet) – they can't be bought this time.")

    acc_in = [AccountIn(
        id=a.id, name=a.name, taxable=not a.is_registered, currency=a.currency or base,
        cash=convert_to_base(a.cash_balance or 0.0, a.currency or base, base, exchange_rates),
    ) for a in accounts]

    pos_in = []
    for acc in accounts:
        for h in acc.holdings:
            # Book value is stored in the security's currency; the optimiser works in base.
            bv = None if h.book_value is None else convert_to_base(
                h.book_value, h.security.currency, base, exchange_rates)
            pos_in.append(PositionIn(
                account_id=acc.id, security_id=h.security_id, quantity=h.quantity, book_value=bv,
            ))

    since = datetime.utcnow() - timedelta(days=SUPERFICIAL_LOSS_DAYS)
    recent = RebalanceTransaction.query.filter(
        RebalanceTransaction.user_id == user.id,
        RebalanceTransaction.executed.is_(True),
        RebalanceTransaction.executed_at >= since,
    ).all()
    recent_buys = {t.security_id for t in recent if t.action == "BUY" and t.security_id}
    recent_loss_sells = {t.security_id for t in recent
                         if t.action == "SELL" and t.security_id and (t.realized_gain or 0) < 0}

    problem = Problem(
        accounts=acc_in, securities=sec_in, positions=pos_in,
        targets={t.asset_class_id: t.target_percentage for t in targets},
        class_names={t.asset_class_id: t.asset_class.name for t in targets},
        buy_levels=_buy_levels(user, accounts, securities),
        threshold_pp=user.balanced_threshold if user.balanced_threshold is not None else 0.5,
        trade_cost=user.trade_cost or 0.0,
        allow_taxable_sells=allow_taxable_sells,
        recent_buys=recent_buys,
        recent_loss_sells=recent_loss_sells,
        fractional_final=bool(user.precision_rebalancing),
    )
    return problem, notes


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
        "notes": result.notes,
        "status": result.status,
        "max_out_of_band": round(result.max_out_of_band, 2),
    }


def _to_transactions(user, result) -> list:
    acc_names = {a.id: a.name for a in Account.query.filter_by(user_id=user.id).all()}
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
            user_id=user.id, account_id=t.account_id, security_id=t.security_id,
            action=t.action, quantity=t.quantity, price=t.price_native,
            amount=round(t.quantity * t.price_native, 2), currency=t.currency,
            execution_order=n, is_final_trade=t.is_final,
            realized_gain=t.realized_gain, requires_user_selection=False,
        ))
    return txns


def generate_plan(user, taxable_override: str | None = None) -> tuple[list, dict]:
    """Solve, persist the plan, and return (transactions, summary)."""
    mode = taxable_override or user.taxable_sell_mode or "ask"
    if mode not in TAXABLE_MODES:
        mode = "ask"
    fx = get_exchange_rates(user)

    problem, notes = build_problem(user, fx, allow_taxable_sells=(mode == "allow"))
    if not problem.targets:
        raise ValueError("No targets defined – set asset-class targets first.")
    result = solve(problem)
    summary = _summarise(result, problem)
    summary["mode"] = mode
    summary["notes"] = notes + summary["notes"]

    # Ask mode: would allowing taxable sells fix what's left?
    if mode == "ask" and result.out_of_band:
        alt_problem, _ = build_problem(user, fx, allow_taxable_sells=True)
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

    txns = _to_transactions(user, result)
    RebalanceTransaction.query.filter_by(user_id=user.id, executed=False).delete()
    for t in txns:
        db.session.add(t)
    db.session.commit()

    log.info("Rebalance plan: mode=%s trades=%d taxable_sells=%d gain=%.2f out_of_band=%s status=%s",
             mode, len(txns), result.taxable_sells, result.realized_gain,
             [problem.class_names[k] for k in result.out_of_band], result.status)
    return txns, summary


def generate_rebalance_transactions(user, taxable_override: str | None = None) -> list:
    """Backwards-compatible entry point."""
    return generate_plan(user, taxable_override)[0]
