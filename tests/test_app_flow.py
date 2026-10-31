"""End-to-end flow through the Flask app with a throwaway SQLite database:
Ask mode → opt in to taxable sells → execute (ACB + realised gain) → regenerate.

Needs the app's real dependencies (pip install -r requirements.txt).
Run:  python -m pytest tests/ -q
"""
import json
import os
import sys
import tempfile
from datetime import datetime

import pytest

pytest.importorskip("flask_sqlalchemy")
pytest.importorskip("flask_login")
pytest.importorskip("authlib")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
_DB = os.path.join(tempfile.mkdtemp(), "portfolio.db")
os.environ.setdefault("SECRET_KEY", "test")
os.environ["DATABASE_URL"] = f"sqlite:///{_DB}"


def test_rebalance_flow():
    from app import app, db
    from models import (Account, AssetClass, ExchangeRate, Holding, RebalanceTransaction,
                        Security, SecurityPreference, Target, User)

    with app.app_context():
        for frm, to, r in (("USD", "CAD", 1.38), ("CAD", "USD", 1 / 1.38)):
            db.session.add(ExchangeRate(from_currency=frm, to_currency=to, rate=r, date=datetime.utcnow(), source="test"))
        u = User(google_id="g1", email="t@example.com", name="Tester", base_currency="CAD", taxable_sell_mode="ask")
        db.session.add(u); db.session.flush()
        acs = {n: AssetClass(name=n) for n in ("CA Equity", "US Equity", "Intl Equity", "Bonds")}
        db.session.add_all(acs.values()); db.session.flush()
        for n, pct in (("CA Equity", 30), ("US Equity", 40), ("Intl Equity", 20), ("Bonds", 10)):
            db.session.add(Target(user_id=u.id, asset_class_id=acs[n].id, target_percentage=pct))
        S = {}
        for t, ac, ccy, lp in (("XIC", "CA Equity", "CAD", None), ("ITOT", "US Equity", "USD", None), ("XUU", "US Equity", "CAD", None),
                               ("XEF", "Intl Equity", "CAD", None), ("ZAG", "Bonds", "CAD", None), ("VAB", "Bonds", "CAD", 23.0)):
            S[t] = Security(ticker=t, name=t, asset_class_id=acs[ac].id, currency=ccy, last_price=lp)
        db.session.add_all(S.values()); db.session.flush()
        rrsp = Account(user_id=u.id, name="RRSP", is_registered=True, cash_balance=0, currency="CAD")
        tfsa = Account(user_id=u.id, name="TFSA", is_registered=True, cash_balance=1000, currency="CAD")
        nr = Account(user_id=u.id, name="Non-Reg", is_registered=False, cash_balance=0, currency="CAD")
        db.session.add_all([rrsp, tfsa, nr]); db.session.flush()
        db.session.add(SecurityPreference(user_id=u.id, security_id=S["ITOT"].id, restriction_type="restricted_to_accounts",
                                          account_config={"allowed": [rrsp.id]}))
        # Registered accounts are balanced-ish; the taxable account holds the big CA overweight at a gain
        for acc, t, q, p, bv in ((rrsp, "ITOT", 300, 130, None), (rrsp, "XEF", 800, 38, None),
                                 (tfsa, "XUU", 300, 60, None), (tfsa, "ZAG", 300, 14, None),
                                 (nr, "XIC", 2000, 40, 50000.0), (nr, "XUU", 200, 60, 14000.0)):
            db.session.add(Holding(account_id=acc.id, security_id=S[t].id, quantity=q, price=p, book_value=bv))
        db.session.commit()
        uid, ids = u.id, {k: v.id for k, v in S.items()}
        acc_ids = {a.name: a.id for a in (rrsp, tfsa, nr)}

    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(uid); s["_fresh"] = True

    def plan():
        with app.app_context():
            return json.loads(User.query.get(uid).last_plan_summary)

    def txns():
        with app.app_context():
            return [(t.id, t.action, t.account.name, t.security.ticker, t.quantity, t.amount, t.realized_gain)
                    for t in RebalanceTransaction.query.filter_by(user_id=uid, executed=False).order_by(RebalanceTransaction.execution_order)]

    # 3. Ask mode
    r = c.post("/rebalance/generate"); assert r.status_code == 302, r.status_code
    p = plan()
    print("\nASK plan:", p["trades"], "trades,", p["taxable_sells"], "taxable sells; drift after:",
          {d["name"]: d["after"] for d in p["drift"]}, "\n offer:", p.get("offer"), "\n notes:", p["notes"])
    for t in txns(): print("  ", t)
    assert p["taxable_sells"] == 0 and "offer" in p
    page = c.get("/rebalance").get_data(as_text=True)
    assert "Plan Outcome" in page and "Use the plan with non-registered sells" in page

    # 4. opt in
    r = c.post("/rebalance/generate", data={"taxable": "allow"}); assert r.status_code == 302
    p = plan()
    print("\nALLOW plan:", p["trades"], "trades,", p["taxable_sells"], "taxable sells, gain", p["realized_gain"],
          {d["name"]: d["after"] for d in p["drift"]})
    for t in txns(): print("  ", t)
    assert p["taxable_sells"] >= 1 and p["override"] == "allow" and not any(d["out_of_band"] for d in p["drift"])
    page = c.get("/rebalance").get_data(as_text=True)
    assert "includes non-registered sells" in page and "est. gain" in page

    # 5. execute the taxable sell, check ACB + cash + realised gain, and that regen kept the opt-in
    sell = next(t for t in txns() if t[1] == "SELL" and t[2] == "Non-Reg")
    with app.app_context():
        h = Holding.query.filter_by(account_id=acc_ids["Non-Reg"], security_id=ids[sell[3]]).first()
        q0, bv0 = h.quantity, h.book_value
    r = c.post(f"/rebalance/execute/{sell[0]}"); assert r.status_code == 302
    with app.app_context():
        h = Holding.query.filter_by(account_id=acc_ids["Non-Reg"], security_id=ids[sell[3]]).first()
        done = RebalanceTransaction.query.get(sell[0])
        exp_acb = bv0 * sell[4] / q0
        assert abs((h.book_value if h else 0) - (bv0 - exp_acb)) < 0.01
        assert abs(done.realized_gain - (sell[5] - exp_acb)) < 0.01
        assert abs(Account.query.get(acc_ids["Non-Reg"]).cash_balance - sell[5]) < 0.01
        print(f"\nexecuted {sell[3]} sell: realised gain {done.realized_gain:,.2f}, remaining ACB {h.book_value if h else 0:,.2f}")
    assert plan()["override"] == "allow", "regen after execute must keep the opt-in"

    # 6. execute a USD buy in a CAD account → cash converted at spot
    usd_buy = next((t for t in txns() if t[1] == "BUY" and t[3] == "ITOT"), None)
    if usd_buy:
        with app.app_context(): cash0 = Account.query.get(acc_ids[usd_buy[2]]).cash_balance
        c.post(f"/rebalance/execute/{usd_buy[0]}")
        with app.app_context():
            assert abs(cash0 - Account.query.get(acc_ids[usd_buy[2]]).cash_balance - usd_buy[5] * 1.38) < 0.05
        print("USD buy debited in CAD at spot ✓")

    # 7. other pages
    for url in ("/settings", "/holdings", "/dashboard", "/rebalance"):
        r = c.get(url); assert r.status_code == 200, (url, r.status_code)
    with app.app_context():
        hid = Holding.query.filter_by(account_id=acc_ids["Non-Reg"]).first().id
    assert c.get(f"/holdings/{hid}/edit").status_code == 200
    r = c.post("/settings/update", data={"base_currency": "CAD", "balanced_threshold": "1.0", "trade_cost": "4.95",
                                          "taxable_sell_mode": "never", "precision_rebalancing": "on"})
    with app.app_context():
        u = User.query.get(uid); assert (u.trade_cost, u.taxable_sell_mode, u.balanced_threshold) == (4.95, "never", 1.0)
    c.post("/rebalance/generate")
    p = plan(); assert p["taxable_sells"] == 0 and "offer" not in p and p["mode"] == "never"

    # --- prices: one price per security, new holdings inherit it, sheet update propagates
    import app as appmod
    c.post("/holdings/add", data={"account_id": acc_ids["TFSA"], "security_id": ids["VAB"], "quantity": "10"})
    with app.app_context():
        h = Holding.query.filter_by(account_id=acc_ids["TFSA"], security_id=ids["VAB"]).first()
        assert h.price == 23.0, "new holding must take the security's price, not 0"
    appmod.fetch_prices_from_user_sheet = lambda user: {"TSE:XIC": 41.5, "NYSEARCA:ITOT": 131.0, "TSE:VAB": 23.4}
    c.post("/update_prices")
    with app.app_context():
        assert Security.query.get(ids["XIC"]).last_price == 41.5
        assert all(h.price == 41.5 for h in Holding.query.filter_by(security_id=ids["XIC"]))
        assert Holding.query.filter_by(security_id=ids["VAB"]).first().price == 23.4
        assert all(h.price == 131.0 for h in Holding.query.filter_by(security_id=ids["ITOT"]))
    assert c.get("/securities").status_code == 200
    assert c.get(f"/securities/{ids['XIC']}/edit").status_code == 200

    # --- book value in SECURITY currency: a USD buy adds the USD amount to ACB
    with app.app_context():
        itot = Holding.query.filter_by(account_id=acc_ids["RRSP"], security_id=ids["ITOT"]).first()
        itot.book_value = 30000.0          # USD
        Account.query.get(acc_ids["RRSP"]).cash_balance = 2000.0
        t = RebalanceTransaction(user_id=uid, account_id=acc_ids["RRSP"], security_id=ids["ITOT"], action="BUY",
                                 quantity=10, price=131.0, amount=1310.0, currency="USD", execution_order=99)
        db.session.add(t); db.session.commit(); tid = t.id
    c.post(f"/rebalance/execute/{tid}")
    with app.app_context():
        itot = Holding.query.filter_by(account_id=acc_ids["RRSP"], security_id=ids["ITOT"]).first()
        assert abs(itot.book_value - 31310.0) < 0.01, itot.book_value                              # stays USD
        assert abs(Account.query.get(acc_ids["RRSP"]).cash_balance - (2000 - 1310 * 1.38)) < 0.05  # CAD cash at spot

    # --- orphaned pending trades (account/security deleted) must not crash /rebalance
    with app.app_context():
        db.session.add(RebalanceTransaction(user_id=uid, account_id=9999, security_id=ids["XIC"], action="BUY",
                                            quantity=1, price=40, amount=40, currency="CAD", execution_order=1))
        db.session.add(RebalanceTransaction(user_id=uid, account_id=acc_ids["TFSA"], security_id=8888, action="BUY",
                                            quantity=1, price=40, amount=40, currency="CAD", execution_order=2))
        db.session.commit()
    assert c.get("/rebalance").status_code == 200
    with app.app_context():   # one bad trade invalidates the whole plan
        assert RebalanceTransaction.query.filter_by(user_id=uid, executed=False).count() == 0
    c.post(f"/accounts/{acc_ids['Non-Reg']}/delete")
    with app.app_context():
        assert not RebalanceTransaction.query.filter_by(account_id=acc_ids["Non-Reg"]).count()
    assert c.get("/rebalance").status_code == 200

    # --- illiquid holding far over target → redemption suggested → record → receive
    from models import PendingRedemption
    with app.app_context():
        Security.query.get(ids["XEF"]).is_illiquid = True
        xef = Holding.query.filter_by(account_id=acc_ids["RRSP"], security_id=ids["XEF"]).first()
        xef.quantity, xef.book_value = 6000, 200000.0          # Intl far over its 20% target
        u = User.query.get(uid); u.taxable_sell_mode = "ask"; u.balanced_threshold = 0.5
        db.session.commit()
    c.post("/rebalance/generate")
    p = plan()
    assert p["redemptions"] and p["redemptions"][0]["holdings"], p.get("redemptions")
    opt = p["redemptions"][0]["holdings"][0]
    assert opt["ticker"] == "XEF" and 0 < opt["suggested_units"] <= 6000
    with app.app_context():
        assert not RebalanceTransaction.query.filter_by(user_id=uid, executed=False, action="SELL",
                                                        security_id=ids["XEF"]).count()   # never sold
    page = c.get("/rebalance").get_data(as_text=True)
    assert "Action needed: file a redemption request" in page

    c.post("/redemptions/add", data={"account_id": opt["account_id"], "security_id": opt["security_id"],
                                     "quantity": opt["suggested_units"], "expected_on": "2027-03-31"})
    with app.app_context():
        r = PendingRedemption.query.filter_by(user_id=uid, status="pending").one()
        rid, units = r.id, r.quantity
    p = plan()
    assert not p["redemptions"], "recorded units must be left out of planning – no repeat suggestion"
    page = c.get("/rebalance").get_data(as_text=True)
    assert "Pending Redemptions" in page and "Action needed" not in page

    # can't over-request
    c.post("/redemptions/add", data={"account_id": opt["account_id"], "security_id": opt["security_id"],
                                     "quantity": 6000})
    with app.app_context():
        assert PendingRedemption.query.filter_by(user_id=uid, status="pending").count() == 1
        cash0 = Account.query.get(acc_ids["RRSP"]).cash_balance

    c.post(f"/redemptions/{rid}/receive", data={"proceeds": "10000"})
    with app.app_context():
        r = PendingRedemption.query.get(rid)
        xef = Holding.query.filter_by(account_id=acc_ids["RRSP"], security_id=ids["XEF"]).first()
        assert r.status == "received" and r.proceeds == 10000
        assert abs(xef.quantity - (6000 - units)) < 1e-6
        assert abs(xef.book_value - 200000.0 * (6000 - units) / 6000) < 0.01      # average-cost ACB
        assert abs(Account.query.get(acc_ids["RRSP"]).cash_balance - (cash0 + 10000)) < 0.01
        rec = RebalanceTransaction.query.filter_by(user_id=uid, executed=True, security_id=ids["XEF"],
                                                   action="SELL").order_by(RebalanceTransaction.id.desc()).first()
        assert rec.amount == 10000 and rec.realized_gain is None                 # registered: no gain

    # cancel path
    c.post("/redemptions/add", data={"account_id": acc_ids["RRSP"], "security_id": ids["XEF"], "quantity": 10})
    with app.app_context():
        rid2 = PendingRedemption.query.filter_by(user_id=uid, status="pending").one().id
    c.post(f"/redemptions/{rid2}/cancel")
    with app.app_context():
        assert PendingRedemption.query.get(rid2).status == "cancelled"
    assert c.get("/rebalance").status_code == 200
