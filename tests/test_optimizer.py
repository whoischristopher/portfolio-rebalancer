"""Unit tests for services/optimizer.py – pure, no database needed.

Run:  python -m pytest tests/ -q
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.optimizer import AccountIn, PositionIn, Problem, SecurityIn, solve  # noqa: E402

FX = 1.38
CA, US, INTL, BD = 1, 2, 3, 4
NAMES = {CA: "CA Equity", US: "US Equity", INTL: "Intl Equity", BD: "Bonds"}
RRSP, TFSA, NR = 1, 2, 3
XIC, ITOT, XUU, XEF, ZAG = 10, 11, 12, 13, 14


def securities():
    return [
        SecurityIn(XIC, "XIC", CA, 40, 40, "CAD"),
        SecurityIn(ITOT, "ITOT", US, 130 * FX, 130, "USD"),
        SecurityIn(XUU, "XUU", US, 60, 60, "CAD"),
        SecurityIn(XEF, "XEF", INTL, 38, 38, "CAD"),
        SecurityIn(ZAG, "ZAG", BD, 14, 14, "CAD"),
    ]


def levels(itot_rrsp_only=True, zag_priority=None):
    """Default eligibility: everything everywhere at level 0, ITOT restricted to RRSP."""
    out = {}
    for a in (RRSP, TFSA, NR):
        for s in (XIC, ITOT, XUU, XEF, ZAG):
            if s == ITOT and itot_rrsp_only and a != RRSP:
                continue
            out[(a, s)] = 0
    if zag_priority:
        for a, lvl in zag_priority.items():
            out[(a, ZAG)] = lvl
    return out


def problem(accounts, positions, targets, **kw):
    kw.setdefault("buy_levels", levels())
    return Problem(accounts=accounts, securities=securities(), positions=positions,
                   targets=targets, class_names=NAMES, **kw)


def trades_by(res, action=None, account=None):
    return [t for t in res.trades
            if (action is None or t.action == action) and (account is None or t.account_id == account)]


# --------------------------------------------------------------------------- #
# The two portfolios from the code review
# --------------------------------------------------------------------------- #
def scenario_a(**kw):
    accts = [AccountIn(RRSP, "RRSP", False, 1000), AccountIn(TFSA, "TFSA", False, 5000),
             AccountIn(NR, "Non-Reg", True, 0)]
    pos = [PositionIn(RRSP, XIC, 2000), PositionIn(RRSP, ITOT, 300),
           PositionIn(TFSA, XEF, 1000), PositionIn(TFSA, XIC, 500),
           PositionIn(NR, XUU, 800, book_value=30000), PositionIn(NR, ZAG, 500, book_value=7500)]
    return problem(accts, pos, {CA: 30, US: 40, INTL: 20, BD: 10}, **kw)


def scenario_b(**kw):
    accts = [AccountIn(RRSP, "RRSP", False, 0), AccountIn(TFSA, "TFSA", False, 3000),
             AccountIn(NR, "Non-Reg", True, 0)]
    pos = [PositionIn(RRSP, ITOT, 600), PositionIn(RRSP, XIC, 500),
           PositionIn(TFSA, XEF, 500), PositionIn(TFSA, XIC, 500),
           PositionIn(NR, XUU, 500, book_value=20000), PositionIn(NR, ZAG, 100, book_value=1500)]
    kw.setdefault("buy_levels", levels(zag_priority={RRSP: 0, TFSA: 1, NR: 3}))
    return problem(accts, pos, {CA: 25, US: 45, INTL: 20, BD: 10}, **kw)


def test_scenario_a_reaches_band_without_taxable_sells():
    res = solve(scenario_a())
    assert not res.out_of_band
    assert res.taxable_sells == 0
    assert len(res.trades) <= 5
    assert all(c >= -0.01 for c in res.cash_left.values())


def test_scenario_b_sizes_usd_sell_correctly_and_never_buys_back():
    res = solve(scenario_b())
    assert not res.out_of_band
    itot_sells = [t for t in trades_by(res, "SELL") if t.security_id == ITOT]
    assert len(itot_sells) == 1
    # ~47.2k CAD overweight / (130 USD * 1.38) ≈ 263 shares, nowhere near the old engine's 505
    assert 240 <= itot_sells[0].quantity <= 290
    assert not any(t.security_id in (ITOT, XUU) for t in trades_by(res, "BUY"))
    # ZAG goes to the priority-1 account
    assert all(t.account_id == RRSP for t in trades_by(res, "BUY") if t.security_id == ZAG)


def test_cash_never_overspent_and_fractional_final_exhausts_cash():
    accts = [AccountIn(TFSA, "TFSA", False, 7777.77)]
    pos = [PositionIn(TFSA, XIC, 500), PositionIn(TFSA, XEF, 300)]
    res = solve(problem(accts, pos, {CA: 50, INTL: 50}, fractional_final=True))
    assert res.cash_left[TFSA] == pytest.approx(0, abs=0.05)
    finals = [t for t in res.trades if t.is_final]
    assert len(finals) == 1 and not float(finals[0].quantity).is_integer()
    for r in (solve(scenario_a()), solve(scenario_b())):
        assert all(left >= -0.01 for left in r.cash_left.values())
        assert not r.out_of_band          # top-up never pushes a class out of band


def test_integer_only_when_fractional_disabled():
    res = solve(scenario_a(fractional_final=False))
    assert all(float(t.quantity).is_integer() for t in res.trades if t.action == "BUY")


# --------------------------------------------------------------------------- #
# Tax behaviour
# --------------------------------------------------------------------------- #
def taxable_only_fix(**kw):
    """Only the taxable account can fix the drift: it holds the overweight class and has no cash."""
    accts = [AccountIn(TFSA, "TFSA", False, 0), AccountIn(NR, "Non-Reg", True, 0)]
    pos = [PositionIn(TFSA, XEF, 1000),                         # 38k intl
           PositionIn(NR, XIC, 1500, book_value=kw.pop("xic_acb", 30000)),  # 60k CA
           PositionIn(NR, ZAG, 100, book_value=1400)]           # 1.4k bonds
    return problem(accts, pos, {CA: 40, INTL: 40, BD: 20}, **kw)


def test_taxable_sells_blocked_when_not_allowed():
    res = solve(taxable_only_fix(allow_taxable_sells=False))
    assert res.taxable_sells == 0
    assert res.out_of_band          # honest: reports what it could not fix


def test_taxable_sells_used_when_allowed_and_gain_reported():
    res = solve(taxable_only_fix(allow_taxable_sells=True))
    sells = trades_by(res, "SELL", NR)
    assert sells and not res.out_of_band
    assert res.realized_gain == pytest.approx(sum(t.quantity for t in sells) * (40 - 20))


def test_taxable_sell_stops_at_band_edge_not_target():
    """With a big gain, it should sell only enough to get inside the band."""
    res = solve(taxable_only_fix(allow_taxable_sells=True, threshold_pp=3.0, xic_acb=3000))
    assert abs(res.drift_after[CA]) == pytest.approx(3.0, abs=0.1)


def test_prefers_selling_the_loss_lot():
    accts = [AccountIn(NR, "Non-Reg", True, 0), AccountIn(TFSA, "TFSA", False, 0)]
    pos = [PositionIn(NR, XIC, 500, book_value=10000),      # gain lot (ACB 20)
           PositionIn(NR, XUU, 500, book_value=40000),      # loss lot (ACB 80 vs 60)
           PositionIn(NR, ZAG, 100, book_value=1400),
           PositionIn(TFSA, XEF, 100)]
    res = solve(problem(accts, pos, {CA: 30, US: 30, BD: 30, INTL: 10},
                        buy_levels=levels(itot_rrsp_only=True)))
    sold = {t.security_id for t in trades_by(res, "SELL")}
    assert XUU in sold or res.realized_gain <= 0


# --------------------------------------------------------------------------- #
# Superficial loss
# --------------------------------------------------------------------------- #
def loss_case(**kw):
    accts = [AccountIn(NR, "Non-Reg", True, 0), AccountIn(TFSA, "TFSA", False, 20000)]
    pos = [PositionIn(NR, XUU, 1000, book_value=80000),      # 60k value, 20k unrealised loss
           PositionIn(NR, ZAG, 100, book_value=1400),
           PositionIn(TFSA, XIC, 10)]
    return problem(accts, pos, {US: 40, BD: 40, CA: 20}, **kw)


def test_loss_sale_and_rebuy_of_same_security_never_in_same_plan():
    res = solve(loss_case())
    loss_sold = {t.security_id for t in trades_by(res, "SELL", NR) if (t.realized_gain or 0) < 0}
    bought = {t.security_id for t in trades_by(res, "BUY")}
    assert not (loss_sold & bought)


def test_recent_purchase_blocks_loss_sale():
    res = solve(loss_case(recent_buys={XUU}))
    assert not [t for t in trades_by(res, "SELL", NR) if t.security_id == XUU]


def test_recent_loss_sale_blocks_rebuy():
    res = solve(loss_case(recent_loss_sells={ZAG}))
    assert not [t for t in trades_by(res, "BUY") if t.security_id == ZAG]


# --------------------------------------------------------------------------- #
# Trade count, new positions, consolidation, priority
# --------------------------------------------------------------------------- #
def test_balanced_portfolio_produces_no_trades():
    accts = [AccountIn(RRSP, "RRSP", False, 100)]
    pos = [PositionIn(RRSP, XIC, 500), PositionIn(RRSP, XEF, 526)]
    res = solve(problem(accts, pos, {CA: 50, INTL: 50}))
    assert res.trades == []


def test_tops_up_existing_position_rather_than_opening_new():
    accts = [AccountIn(RRSP, "RRSP", False, 10000)]
    pos = [PositionIn(RRSP, XIC, 1000), PositionIn(RRSP, XUU, 100)]
    res = solve(problem(accts, pos, {CA: 50, US: 50}, buy_levels=levels(itot_rrsp_only=False)))
    us_buys = [t for t in trades_by(res, "BUY") if t.security_id in (ITOT, XUU)]
    assert us_buys and all(t.security_id == XUU for t in us_buys)


def test_consolidates_small_scattered_position_when_cheap():
    """A small XIC lot in the TFSA, a big one in the RRSP; CA is slightly overweight.
    Selling the whole TFSA lot (instead of trimming the RRSP) both fixes the drift and
    removes a position."""
    accts = [AccountIn(RRSP, "RRSP", False, 0), AccountIn(TFSA, "TFSA", False, 0)]
    pos = [PositionIn(RRSP, XIC, 1500), PositionIn(TFSA, XIC, 60),
           PositionIn(TFSA, XEF, 1000), PositionIn(RRSP, XEF, 450)]
    res = solve(problem(accts, pos, {CA: 50, INTL: 50}, threshold_pp=0.5))
    tfsa_xic = [t for t in trades_by(res, "SELL", TFSA) if t.security_id == XIC]
    assert tfsa_xic and tfsa_xic[0].quantity == 60


def test_priority_falls_back_when_priority_account_has_no_cash():
    """RRSP is priority 1 for ZAG but has no cash and nothing it should sell; TFSA has the cash.
    Old engine: priority-2 accounts were skipped, so bonds were never bought."""
    accts = [AccountIn(RRSP, "RRSP", False, 0), AccountIn(TFSA, "TFSA", False, 5000)]
    pos = [PositionIn(RRSP, XEF, 1000), PositionIn(TFSA, XIC, 1000)]
    res = solve(problem(accts, pos, {INTL: 46, CA: 48, BD: 6},
                        buy_levels={**levels(), (RRSP, ZAG): 0, (TFSA, ZAG): 1}))
    assert any(t.security_id == ZAG and t.account_id == TFSA for t in trades_by(res, "BUY"))
    assert len(res.trades) == 1


def test_priority_account_preferred_when_it_has_cash():
    accts = [AccountIn(RRSP, "RRSP", False, 5000), AccountIn(TFSA, "TFSA", False, 5000)]
    pos = [PositionIn(RRSP, XEF, 1000), PositionIn(TFSA, XIC, 1000)]
    res = solve(problem(accts, pos, {INTL: 44, CA: 45, BD: 11},
                        buy_levels={**levels(), (RRSP, ZAG): 0, (TFSA, ZAG): 1}))
    zag = [t for t in trades_by(res, "BUY") if t.security_id == ZAG]
    assert zag and zag[0].account_id == RRSP


def test_usd_buy_in_cad_account_flags_conversion():
    accts = [AccountIn(RRSP, "RRSP", False, 20000, currency="CAD")]
    pos = [PositionIn(RRSP, XIC, 1000), PositionIn(RRSP, ITOT, 10)]
    res = solve(problem(accts, pos, {CA: 60, US: 40}))
    assert any("convert" in n and "USD" in n for n in res.notes)


# --------------------------------------------------------------------------- #
# Stage 2: aim for target (user's real allocation, Oct 2026)
# --------------------------------------------------------------------------- #
def user_portfolio(**kw):
    vals = {1: (2143848, 36), 2: (1316316, 22), 3: (1053793, 18), 4: (501756, 9),
            5: (315384, 5), 6: (242867, 4), 7: (236277, 4), 8: (117934, 2)}
    secs = [SecurityIn(100 + k, f"S{k}", k, 50.0, 50.0, "CAD") for k in vals]
    accts = [AccountIn(1, "RRSP", False, 0.0)]
    pos = [PositionIn(1, 100 + k, v / 50.0) for k, (v, _) in vals.items()]
    return Problem(accounts=accts, securities=secs, positions=pos,
                   targets={k: t for k, (_, t) in vals.items()}, class_names={k: f"C{k}" for k in vals},
                   buy_levels={(1, 100 + k): 0 for k in vals}, **kw)


def test_aims_for_target_with_same_trade_count():
    edge = solve(user_portfolio(aim_for_target=False))
    aimed = solve(user_portfolio(aim_for_target=True))
    assert not edge.out_of_band and not aimed.out_of_band
    assert len(aimed.trades) <= len(edge.trades)
    assert abs(aimed.drift_after[4]) < abs(edge.drift_after[4])     # EM closer to 9%
    assert abs(aimed.drift_after[4]) < 0.15


def test_inside_band_still_means_no_trades():
    p = user_portfolio()
    p.positions = [PositionIn(1, 104, (501756 + 30000) / 50.0) if q.security_id == 104 else q for q in p.positions]
    p.accounts[0].cash = 0
    assert solve(p).trades == [] or solve(p).out_of_band == []
