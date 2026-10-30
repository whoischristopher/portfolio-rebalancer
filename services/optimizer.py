"""
services/optimizer.py – Rebalancing as a mixed-integer linear program (MILP).

This module is deliberately pure: no Flask, no SQLAlchemy.  The caller
(rebalancer.py) builds a `Problem` snapshot from the database, calls
`solve()`, and turns the returned `Trade`s into RebalanceTransaction rows.
That keeps the maths unit-testable without a database.

All money values are in the user's BASE currency unless the name says
`_native`.  FX is treated at spot rate (no conversion spread).

What the optimiser minimises (every term is in "dollar-equivalent pain"):

  drift_out  × $ an asset class sits OUTSIDE the balanced band
  drift_in   × percentage points a class sits inside the band (tiny – breaks ties;
               per point rather than per $, so a $6M portfolio isn't fussier than a $200k one)
  gain       × $ capital gain realised by selling in a taxable account
  ticket     × number of trades (+ the user's real per-trade cost)
  position   × number of open positions left after trading
               (discourages new positions, rewards consolidating
                small scattered positions when it is cheap to do so)
  priority   × $ bought in a lower-priority account, per priority level
  idle_cash  × $ cash left undeployed
  turnover   × $ traded (prefers the smallest moves that achieve the same result)

Hard constraints:
  * no account's cash goes negative (trades are funded inside the account)
  * cannot sell more than held; restricted securities only in allowed accounts
  * taxable sells only when allowed (the "Ask" flow solves twice)
  * superficial-loss rule: a security sold at a loss in a taxable account
    cannot be bought in ANY account in the same plan; securities bought in
    the last 30 days cannot be sold at a loss in a taxable account; and
    securities sold at a loss in the last 30 days cannot be bought.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field

import numpy as np
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.sparse import lil_matrix

log = logging.getLogger(__name__)

_GAP_ANCHOR = 10_000.0   # see solve(): turns the relative MIP gap into ~absolute dollars
_ABS_TOL = 2.0           # stop when provably within ~$2 of optimal (a ticket is $10)


# --------------------------------------------------------------------------- #
# Inputs / outputs
# --------------------------------------------------------------------------- #
@dataclass
class AccountIn:
    id: int
    name: str
    taxable: bool
    cash: float                       # base currency
    currency: str = "CAD"             # currency the account's cash is held in


@dataclass
class SecurityIn:
    id: int
    ticker: str
    asset_class_id: int
    price: float                      # base currency, per share
    price_native: float               # security currency, per share
    currency: str


@dataclass
class PositionIn:
    account_id: int
    security_id: int
    quantity: float
    book_value: float | None = None   # total ACB in base currency; None = unknown


@dataclass
class Weights:
    drift_out: float = 1.0
    drift_in_pp: float = 1.0           # $-equivalent per percentage point inside the band (scale-free)
    gain: float = 1.0
    unknown_gain_fraction: float = 1.0   # missing ACB → assume proceeds are all gain
    ticket: float = 10.0
    position: float = 30.0
    priority: float = 0.002
    idle_cash: float = 0.01
    turnover: float = 0.0005             # per $ traded – prefers small moves over big swaps
    # Stage 2 ("aim for target"): convex in-band cost per $, by quarter of the band.
    # Rising costs mean the furthest-from-target classes are fixed first.
    target_pull: tuple = (0.01, 0.02, 0.04, 0.08)


@dataclass
class Problem:
    accounts: list[AccountIn]
    securities: list[SecurityIn]
    positions: list[PositionIn]
    targets: dict[int, float]                 # asset_class_id -> target %
    class_names: dict[int, str]
    # (account_id, security_id) -> priority level (0 = best). Missing = not allowed to BUY.
    buy_levels: dict[tuple[int, int], int]
    threshold_pp: float = 0.5
    trade_cost: float = 0.0                   # base currency per trade
    allow_taxable_sells: bool = True
    recent_buys: set[int] = field(default_factory=set)        # sec ids bought ≤30d ago (any account)
    recent_loss_sells: set[int] = field(default_factory=set)  # sec ids sold at a loss ≤30d ago (taxable)
    fractional_final: bool = True
    aim_for_target: bool = True     # stage 2: same trade count & tax, sized toward target
    weights: Weights = field(default_factory=Weights)
    time_limit: float = 10.0


@dataclass
class Trade:
    account_id: int
    security_id: int
    action: str                       # BUY / SELL
    quantity: float
    price_native: float
    currency: str
    amount_base: float
    is_final: bool = False
    realized_gain: float | None = None   # base currency, taxable SELLs only


@dataclass
class Result:
    status: str
    trades: list[Trade]
    total_value: float
    drift_before: dict[int, float]    # asset_class_id -> percentage points vs target
    drift_after: dict[int, float]
    out_of_band: list[int]            # asset classes still outside the band after trading
    realized_gain: float
    taxable_sells: int
    cash_left: dict[int, float]
    notes: list[str]
    objective: float = 0.0

    @property
    def max_out_of_band(self) -> float:
        return max((abs(self.drift_after[k]) for k in self.out_of_band), default=0.0)


# --------------------------------------------------------------------------- #
# Solver
# --------------------------------------------------------------------------- #
class _Vars:
    """Tiny allocator so the constraint code can refer to variables by name."""

    def __init__(self):
        self.n = 0
        self.lb, self.ub, self.c, self.integ = [], [], [], []

    def add(self, lb=0.0, ub=np.inf, cost=0.0, integer=False):
        self.lb.append(lb); self.ub.append(ub); self.c.append(cost); self.integ.append(1 if integer else 0)
        self.n += 1
        return self.n - 1


def solve(p: Problem) -> Result:
    W = p.weights
    notes: list[str] = []
    acc = {a.id: a for a in p.accounts}
    sec = {s.id: s for s in p.securities}

    # ---- aggregate holdings per (account, security) --------------------- #
    held: dict[tuple[int, int], list] = {}
    for pos in p.positions:
        if pos.account_id not in acc or pos.security_id not in sec or pos.quantity <= 0:
            continue
        q, bv = held.get((pos.account_id, pos.security_id), [0.0, 0.0])
        bv = None if (bv is None or pos.book_value is None) else bv + pos.book_value
        held[(pos.account_id, pos.security_id)] = [q + pos.quantity, bv]

    # ---- current state ------------------------------------------------- #
    cur_by_class: dict[int, float] = {k: 0.0 for k in p.targets}
    untargeted: dict[int, float] = {}
    total = sum(a.cash for a in p.accounts)
    for (a_id, s_id), (q, _) in held.items():
        v = q * sec[s_id].price
        total += v
        k = sec[s_id].asset_class_id
        if k in cur_by_class:
            cur_by_class[k] += v
        else:
            untargeted[k] = untargeted.get(k, 0.0) + v
    if total <= 0:
        raise ValueError("Portfolio value is zero; cannot rebalance.")
    tsum = sum(p.targets.values())
    if abs(tsum - 100) > 0.01:
        notes.append(f"Targets add up to {tsum:.2f}%, not 100% – the plan cannot fully converge.")
    if untargeted:
        notes.append(f"{sum(untargeted.values()):,.0f} is held in asset classes with no target; "
                     "those holdings are left untouched.")

    target_val = {k: total * pct / 100 for k, pct in p.targets.items()}
    band = p.threshold_pp / 100 * total
    drift_before = {k: (cur_by_class[k] - target_val[k]) / total * 100 for k in p.targets}

    # Upper bound on what an account can buy: its cash plus what it may sell.
    capacity = {a.id: a.cash for a in p.accounts}
    for (a_id, s_id), (q, _) in held.items():
        if not acc[a_id].taxable or p.allow_taxable_sells:
            capacity[a_id] += q * sec[s_id].price

    # ---- decision variables per (account, security) pair --------------- #
    # Trade sizes are CONTINUOUS (shares); only the yes/no decisions are integer.
    # Whole-share rounding happens afterwards (sells up, buys down → cash stays ≥ 0)
    # and the fractional final buy absorbs the remainder.  This keeps solves fast.
    V = _Vars()
    pairs = []
    const_offset = 0.0
    keys = set(held) | _buy_candidates(p, acc, sec, held)
    missing_acb = set()
    for (a_id, s_id) in sorted(keys):
        a, s = acc[a_id], sec[s_id]
        if s.asset_class_id not in p.targets or s.price <= 0:
            continue
        q_held, bv = held.get((a_id, s_id), [0.0, 0.0])
        level = p.buy_levels.get((a_id, s_id))
        can_buy = level is not None and s_id not in p.recent_loss_sells

        # realised gain per share if sold (taxable accounts only)
        gain_ps = 0.0
        if a.taxable and q_held > 0:
            if bv is None:
                gain_ps = s.price * W.unknown_gain_fraction
                missing_acb.add(s.ticker)
            else:
                gain_ps = s.price - bv / q_held
        is_loss = a.taxable and q_held > 0 and bv is not None and gain_ps < 0
        can_sell = q_held > 0 and (not a.taxable or p.allow_taxable_sells)
        if is_loss and s_id in p.recent_buys:
            can_sell = False   # loss would be superficial
            notes.append(f"{s.ticker} in {a.name}: sale at a loss blocked – bought within the last 30 days.")

        max_buy = capacity[a_id] / s.price if can_buy else 0.0
        sell_cost = (W.idle_cash + W.turnover) * s.price + (W.gain * max(gain_ps, 0.0) if a.taxable else 0.0)

        i = dict(acct=a, sec=s, held=q_held, gain_ps=gain_ps,
                 is_loss=is_loss, level=level or 0)
        i["b"] = V.add(0, max_buy, cost=s.price * (W.priority * (level or 0) - W.idle_cash + W.turnover))
        i["s"] = V.add(0, q_held if can_sell else 0, cost=sell_cost)
        i["yb"] = V.add(0, 1 if max_buy > 0 else 0, cost=W.ticket + p.trade_cost, integer=True)
        i["ys"] = V.add(0, 1 if can_sell else 0, cost=W.ticket + p.trade_cost, integer=True)
        # z = position open after trading.  Held positions get a reward for closing
        # (cost −position on 1−z ≡ +position on z minus a constant) so the objective
        # carries no large constant and the solver's optimality gap means real dollars.
        i["z"] = V.add(0, 1, cost=W.position, integer=True)
        if q_held > 0:
            const_offset += W.position
        pairs.append(i)

    if missing_acb and p.allow_taxable_sells:
        notes.append("Book value missing for " + ", ".join(sorted(missing_acb)) +
                     " in taxable accounts – treated as 100% gain (most conservative).")

    drift_in = W.drift_in_pp * 100.0 / total      # per $ of in-band drift
    cls_vars = {}
    for k in p.targets:
        cls_vars[k] = dict(
            oo=V.add(cost=W.drift_out), uo=V.add(cost=W.drift_out),
            # in-band deviation split into 4 tiers (flat cost in stage 1, convex in stage 2)
            oi=[V.add(0, band / 4, cost=drift_in) for _ in W.target_pull],
            ui=[V.add(0, band / 4, cost=drift_in) for _ in W.target_pull],
        )

    # ---- constraints --------------------------------------------------- #
    rows, lo, hi = [], [], []

    def row(coefs, l, h):
        rows.append(coefs); lo.append(l); hi.append(h)

    for i in pairs:
        pr = i["sec"].price
        row({i["b"]: 1, i["yb"]: -V.ub[i["b"]]}, -np.inf, 0)                       # buy ⇒ ticket
        row({i["s"]: 1, i["ys"]: -i["held"]}, -np.inf, 0)                         # sell ⇒ ticket
        # position-open flag z (tight form): buying ⇒ open; closed ⇒ entire holding sold
        row({i["yb"]: 1, i["z"]: -1}, -np.inf, 0)
        row({i["yb"]: 1, i["ys"]: 1}, -np.inf, 1)                                 # never buy+sell same line
        if i["held"] > 0:
            row({i["s"]: 1, i["z"]: i["held"]}, i["held"], np.inf)

    for a in p.accounts:  # cash feasibility per account
        coefs = {}
        for i in pairs:
            if i["acct"].id != a.id:
                continue
            pr = i["sec"].price
            coefs[i["b"]] = pr
            coefs[i["s"]] = -pr
            coefs[i["yb"]] = p.trade_cost
            coefs[i["ys"]] = p.trade_cost
        if coefs:
            row(coefs, -np.inf, a.cash)

    for k, cv in cls_vars.items():  # drift definition per asset class
        coefs = {cv["oo"]: -1, cv["uo"]: 1, **{v: -1 for v in cv["oi"]}, **{v: 1 for v in cv["ui"]}}
        for i in pairs:
            if i["sec"].asset_class_id == k:
                pr = i["sec"].price
                coefs[i["b"]] = pr
                coefs[i["s"]] = -pr
        rhs = target_val[k] - cur_by_class[k]
        row(coefs, rhs, rhs)

    # superficial loss inside this plan: a loss sale of X forbids buying X anywhere
    for i in pairs:
        if not i["is_loss"]:
            continue
        for j in pairs:
            if j["sec"].id == i["sec"].id and j is not i:
                row({i["ys"]: 1, j["yb"]: 1}, -np.inf, 1)

    # ---- solve ---------------------------------------------------------- #
    # scipy's HiGHS wrapper only exposes a RELATIVE gap.  Pin the objective's scale
    # with a fixed variable so that rel_gap × anchor ≈ the absolute tolerance we want
    # (well under one trade ticket).
    anchor = V.add(1, 1, cost=_GAP_ANCHOR - const_offset)

    def run(costs, rows, lo, hi):
        A = lil_matrix((len(rows), V.n))
        for r, coefs in enumerate(rows):
            for v, c in coefs.items():
                A[r, v] = c
        return milp(
            np.array(costs), integrality=np.array(V.integ),
            bounds=Bounds(np.array(V.lb), np.array(V.ub)),
            constraints=LinearConstraint(A.tocsr(), np.array(lo), np.array(hi)),
            options={"time_limit": p.time_limit, "mip_rel_gap": _ABS_TOL / _GAP_ANCHOR},
        )

    # Stage 1: cheapest plan that gets every class inside its band.
    res = run(V.c, rows, lo, hi)
    if res.x is None:
        raise RuntimeError(f"Optimiser found no feasible plan: {res.message}")

    # Stage 2: if we're trading anyway, size the trades toward target – but with
    # no more trades and no more realised gain than stage 1.  Within-band portfolios
    # that need no trades are left alone.
    tickets = [v for i in pairs for v in (i["yb"], i["ys"])]
    n_trades = int(round(sum(res.x[v] for v in tickets)))
    if p.aim_for_target and n_trades > 0:
        gain_coef = {i["s"]: W.gain * max(i["gain_ps"], 0.0) for i in pairs
                     if i["acct"].taxable and i["gain_ps"] > 0}
        stage1_gain = sum(res.x[v] * c for v, c in gain_coef.items())
        costs2 = list(V.c)
        for cv in cls_vars.values():
            for tier, w in zip(range(len(W.target_pull)), W.target_pull):
                costs2[cv["oi"][tier]] = w
                costs2[cv["ui"][tier]] = w
        rows2, lo2, hi2 = list(rows), list(lo), list(hi)
        rows2.append({v: 1 for v in tickets}); lo2.append(-np.inf); hi2.append(n_trades)
        if gain_coef:
            rows2.append(gain_coef); lo2.append(-np.inf); hi2.append(stage1_gain + 1.0)
        res2 = run(costs2, rows2, lo2, hi2)
        if res2.x is not None:
            res = res2
        else:
            log.warning("Stage-2 (aim for target) solve failed: %s – keeping stage-1 plan", res2.message)
    if res.status != 0:
        notes.append("Optimiser hit its time limit – plan is good but may not be optimal.")
    x = res.x

    # ---- read back trades ---------------------------------------------- #
    trades: list[Trade] = []
    cash_left = {a.id: a.cash for a in p.accounts}
    after = dict(cur_by_class)
    for i in pairs:
        s, a = i["sec"], i["acct"]
        nb = math.floor(x[i["b"]] + 1e-6) if round(x[i["yb"]]) == 1 else 0
        ns = 0.0
        if round(x[i["ys"]]) == 1 and x[i["s"]] > 1e-6:
            ns = math.ceil(x[i["s"]] - 1e-6)
            if ns >= i["held"] - 1e-9 or round(x[i["z"]]) == 0:
                ns = i["held"]            # closing the position: sell everything, incl. fractions
        if ns > 1e-9:
            gain = ns * i["gain_ps"] if a.taxable else None
            trades.append(Trade(a.id, s.id, "SELL", ns, s.price_native, s.currency, ns * s.price,
                                realized_gain=gain))
            cash_left[a.id] += ns * s.price - p.trade_cost
            after[s.asset_class_id] -= ns * s.price
        if nb > 0:
            trades.append(Trade(a.id, s.id, "BUY", nb, s.price_native, s.currency, nb * s.price))
            cash_left[a.id] -= nb * s.price + p.trade_cost
            after[s.asset_class_id] += nb * s.price

    # ---- fractional final buy: put leftover cash into this account's buys -- #
    # Goes to the buy whose asset class is furthest below target, never pushing a
    # class past the top of its band.  No new tickets are created.
    if p.fractional_final:
        for a in p.accounts:
            buys = [t for t in trades if t.account_id == a.id and t.action == "BUY"]
            buys.sort(key=lambda t: after[sec[t.security_id].asset_class_id]
                      - target_val[sec[t.security_id].asset_class_id])
            for t in buys:
                s = sec[t.security_id]
                k = s.asset_class_id
                room = min(cash_left[a.id], target_val[k] + band - after[k])
                extra = math.floor(room / s.price * 1e4) / 1e4
                if extra <= 0:
                    continue
                t.quantity = round(t.quantity + extra, 4)
                t.amount_base = t.quantity * s.price
                t.is_final = True
                cash_left[a.id] -= extra * s.price
                after[k] += extra * s.price
                if cash_left[a.id] < 0.01:
                    break

    # ---- FX conversions the user must do before buying ------------------- #
    # Accounts hold cash in one currency in this app; flag net foreign-currency buys.
    for a in p.accounts:
        net: dict[str, float] = {}
        for t in trades:
            if t.account_id == a.id and t.currency != a.currency:
                net[t.currency] = net.get(t.currency, 0.0) + (1 if t.action == "BUY" else -1) * t.quantity * t.price_native
        for ccy, amt in net.items():
            if amt > 1:
                notes.append(f"{a.name}: convert about {amt:,.2f} {ccy} from {a.currency} before buying "
                             "(planned at spot rate).")

    drift_after = {k: (after[k] - target_val[k]) / total * 100 for k in p.targets}
    out = [k for k, d in drift_after.items() if abs(d) > p.threshold_pp + 1e-6]
    sells_tax = [t for t in trades if t.action == "SELL" and acc[t.account_id].taxable]
    trades.sort(key=lambda t: (acc[t.account_id].name, 0 if t.action == "SELL" else 1, -t.amount_base))
    return Result(
        status="optimal" if res.status == 0 else "time_limit",
        trades=trades, total_value=total,
        drift_before=drift_before, drift_after=drift_after, out_of_band=out,
        realized_gain=sum(t.realized_gain or 0 for t in sells_tax),
        taxable_sells=len(sells_tax), cash_left=cash_left, notes=_dedupe(notes),
        objective=float(res.fun) - _GAP_ANCHOR,
    )


def _buy_candidates(p: Problem, acc, sec, held) -> set:
    """Buy candidates: anything already held in that account, plus – per account and
    asset class – the single best NEW security (lowest priority level, then the one
    most held elsewhere in the portfolio, then cheapest price for fine granularity).
    Offering every eligible ticker as a new position adds nothing but solver symmetry."""
    held_elsewhere: dict[int, float] = {}
    for (a_id, s_id), (q, _) in held.items():
        held_elsewhere[s_id] = held_elsewhere.get(s_id, 0.0) + q * sec[s_id].price
    best: dict[tuple[int, int], tuple] = {}
    out = set()
    for (a_id, s_id), lvl in p.buy_levels.items():
        if a_id not in acc or s_id not in sec or sec[s_id].price <= 0:
            continue
        if s_id in p.recent_loss_sells and (a_id, s_id) not in held:
            continue
        if (a_id, s_id) in held:
            out.add((a_id, s_id))
            continue
        key = (a_id, sec[s_id].asset_class_id)
        rank = (lvl, -held_elsewhere.get(s_id, 0.0), sec[s_id].price, s_id)
        if key not in best or rank < best[key]:
            best[key] = rank
    out |= {(a_id, rank[3]) for (a_id, _), rank in best.items()}
    return out


def _dedupe(seq):
    seen, out = set(), []
    for s in seq:
        if s not in seen:
            seen.add(s); out.append(s)
    return out
