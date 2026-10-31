"""Multiple portfolios under one login: isolation, switching, shared prices,
superficial-loss link, copy/delete, and the in-place migration of a pre-portfolio
database (where securities and asset classes were global).

Run:  python -m pytest tests/ -q
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime

import pytest

pytest.importorskip("flask_sqlalchemy")
pytest.importorskip("flask_login")
pytest.importorskip("authlib")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
if "app" not in sys.modules:                       # share the app DB if another test set it up
    os.environ.setdefault("SECRET_KEY", "test")
    os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(tempfile.mkdtemp(), 'portfolio.db')}"


@pytest.fixture()
def two_portfolios():
    """One login with 'Me' and 'Spouse' portfolios that both hold XEQT."""
    from app import app, db
    from models import (Account, AssetClass, ExchangeRate, Holding, Portfolio, Security, Target, User)
    with app.app_context():
        if not ExchangeRate.query.count():
            for frm, to, r in (("USD", "CAD", 1.38), ("CAD", "USD", 1 / 1.38)):
                db.session.add(ExchangeRate(from_currency=frm, to_currency=to, rate=r,
                                            date=datetime.utcnow(), source="test"))
        n = User.query.count() + 1
        u = User(google_id=f"pf-{n}", email=f"pf{n}@example.com", name="Two Portfolios")
        db.session.add(u); db.session.flush()
        ids = {"user": u.id}
        for key, name, acct, cash, qty in (("me", "Me", "Me-TFSA", 0, 100), ("sp", "Spouse", "Sp-RRSP", 1000, 50)):
            p = Portfolio(user_id=u.id, name=name, taxable_sell_mode="ask", balanced_threshold=0.5)
            db.session.add(p); db.session.flush()
            eq, bd = AssetClass(portfolio_id=p.id, name="Equity"), AssetClass(portfolio_id=p.id, name="Bonds")
            db.session.add_all([eq, bd]); db.session.flush()
            db.session.add_all([Target(user_id=u.id, portfolio_id=p.id, asset_class_id=eq.id, target_percentage=80),
                                Target(user_id=u.id, portfolio_id=p.id, asset_class_id=bd.id, target_percentage=20)])
            x = Security(portfolio_id=p.id, ticker="XEQT", asset_class_id=eq.id, currency="CAD", last_price=30.0)
            z = Security(portfolio_id=p.id, ticker="ZAG", asset_class_id=bd.id, currency="CAD", last_price=14.0)
            db.session.add_all([x, z]); db.session.flush()
            a = Account(user_id=u.id, portfolio_id=p.id, name=acct, is_registered=True, cash_balance=cash)
            db.session.add(a); db.session.flush()
            h = Holding(account_id=a.id, security_id=x.id, quantity=qty, price=30.0, book_value=qty * 25.0)
            db.session.add(h); db.session.flush()
            ids[key] = dict(p=p.id, xeqt=x.id, zag=z.id, eq=eq.id, acct=a.id, holding=h.id)
        db.session.commit()
    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(ids["user"]); s["_fresh"] = True
    return app, c, ids


def _switch(c, pid):
    r = c.post(f"/portfolios/{pid}/switch", headers={"Referer": "http://localhost/holdings"})
    assert r.status_code == 302 and r.headers["Location"].endswith("/holdings")


def test_pages_show_only_the_active_portfolio(two_portfolios):
    app, c, ids = two_portfolios
    page = c.get("/holdings").get_data(as_text=True)            # first portfolio is the default
    assert "Me-TFSA" in page and "Sp-RRSP" not in page
    assert "Spouse" in page                                      # ...but the switcher lists both
    _switch(c, ids["sp"]["p"])
    page = c.get("/holdings").get_data(as_text=True)
    assert "Sp-RRSP" in page and "Me-TFSA" not in page
    for url in ("/dashboard", "/accounts", "/securities", "/targets", "/rebalance", "/settings", "/portfolios"):
        assert c.get(url).status_code == 200, url
    summary = c.get("/api/portfolio/summary").get_json()
    assert summary["portfolio"] == "Spouse" and summary["num_accounts"] == 1


def test_other_portfolios_rows_are_unreachable(two_portfolios):
    app, c, ids = two_portfolios
    from app import db
    from models import Account, Holding
    me, sp = ids["me"], ids["sp"]                                # "Me" is active
    assert c.get(f"/holdings/{sp['holding']}/edit").status_code == 404
    assert c.get(f"/securities/{sp['xeqt']}/edit").status_code == 404
    assert c.post(f"/accounts/{sp['acct']}/delete").status_code == 404
    assert c.post(f"/securities/{sp['zag']}/delete").status_code == 404
    # mixing: my account + spouse's security
    assert c.post("/holdings/add", data={"account_id": me["acct"], "security_id": sp["zag"],
                                         "quantity": "5"}).status_code == 404
    # moving my holding into the spouse's account
    assert c.post(f"/holdings/{me['holding']}/edit", data={"account_id": sp["acct"], "security_id": me["xeqt"],
                                                           "quantity": "1", "price": "30"}).status_code == 404
    assert c.post("/targets/update", data={"action": "add", "asset_class_id": sp["eq"],
                                           "percentage": "5"}).status_code == 404
    assert c.post("/portfolios/999999/switch").status_code == 404
    with app.app_context():
        assert db.session.get(Account, sp["acct"]) is not None
        assert db.session.get(Holding, me["holding"]).account_id == me["acct"]


def test_plans_and_settings_are_per_portfolio(two_portfolios):
    app, c, ids = two_portfolios
    from app import db
    from models import Portfolio, RebalanceTransaction
    _switch(c, ids["sp"]["p"])
    c.post("/settings/update", data={"base_currency": "CAD", "balanced_threshold": "2.0", "trade_cost": "9.99",
                                     "taxable_sell_mode": "never"})
    c.post("/rebalance/generate")
    with app.app_context():
        me, sp = db.session.get(Portfolio, ids["me"]["p"]), db.session.get(Portfolio, ids["sp"]["p"])
        assert (sp.trade_cost, sp.balanced_threshold, sp.taxable_sell_mode) == (9.99, 2.0, "never")
        assert (me.trade_cost, me.balanced_threshold, me.taxable_sell_mode) == (0.0, 0.5, "ask")
        assert sp.last_plan_summary and not me.last_plan_summary
        txns = RebalanceTransaction.query.filter_by(executed=False).filter(
            RebalanceTransaction.portfolio_id.in_([me.id, sp.id])).all()
        assert txns and all(t.portfolio_id == sp.id and t.account_id == ids["sp"]["acct"] for t in txns)
        assert all(t.security.portfolio_id == sp.id for t in txns)
    # executing a trade in the spouse's portfolio leaves mine alone
    tid = txns[0].id
    assert c.post(f"/rebalance/execute/{tid}").status_code == 302
    _switch(c, ids["me"]["p"])
    assert c.post(f"/rebalance/execute/{tid}").status_code == 404


def test_one_price_feed_for_all_portfolios(two_portfolios, monkeypatch):
    app, c, ids = two_portfolios
    import app as appmod
    from app import db
    from models import Holding, Security
    monkeypatch.setattr(appmod, "fetch_prices_from_user_sheet", lambda user: {"TSE:XEQT": 31.0, "TSE:ZAG": 14.2})
    c.post("/update_prices")
    with app.app_context():
        for k in ("me", "sp"):
            assert db.session.get(Security, ids[k]["xeqt"]).last_price == 31.0
            assert db.session.get(Security, ids[k]["zag"]).last_price == 14.2
            assert db.session.get(Holding, ids[k]["holding"]).price == 31.0
    # a hand-typed price is the price of that ticker everywhere
    c.post(f"/securities/{ids['me']['xeqt']}/edit", data={"ticker": "XEQT", "asset_class_id": ids["me"]["eq"],
                                                         "currency": "CAD", "price": "32.5"})
    with app.app_context():
        assert db.session.get(Security, ids["sp"]["xeqt"]).last_price == 32.5
        assert db.session.get(Holding, ids["sp"]["holding"]).price == 32.5
    # adding a ticker the other portfolio already prices reuses that price
    _switch(c, ids["sp"]["p"])
    c.post("/securities/add", data={"ticker": "VFV", "asset_class_id": ids["sp"]["eq"], "currency": "CAD",
                                    "price": "140"})
    _switch(c, ids["me"]["p"])
    c.post("/securities/add", data={"ticker": "VFV", "asset_class_id": ids["me"]["eq"], "currency": "CAD"})
    c.post("/securities/add", data={"ticker": "vfv", "asset_class_id": ids["me"]["eq"], "currency": "CAD"})
    with app.app_context():
        mine = Security.query.filter_by(portfolio_id=ids["me"]["p"]).filter(
            db.func.upper(Security.ticker) == "VFV").all()
        assert len(mine) == 1 and mine[0].last_price == 140.0          # duplicate in-portfolio rejected


def test_superficial_loss_link_is_opt_in(two_portfolios):
    app, c, ids = two_portfolios
    from app import db
    from models import Portfolio, RebalanceTransaction
    from rebalancer import _superficial_loss_history
    with app.app_context():
        u = db.session.get(Portfolio, ids["sp"]["p"]).user_id
        db.session.add(RebalanceTransaction(user_id=u, portfolio_id=ids["sp"]["p"], account_id=ids["sp"]["acct"],
                                            security_id=ids["sp"]["xeqt"], action="BUY", quantity=1, price=30,
                                            amount=30, executed=True, executed_at=datetime.utcnow()))
        db.session.commit()
        me = db.session.get(Portfolio, ids["me"]["p"])
        notes = []
        buys, _ = _superficial_loss_history(me, notes)
        assert ids["me"]["xeqt"] not in buys and not notes                 # default: not linked
    others = c.get("/settings").get_data(as_text=True)
    assert "Also check trades recorded in" in others
    c.post("/settings/update", data={"base_currency": "CAD", "balanced_threshold": "0.5", "trade_cost": "0",
                                     "taxable_sell_mode": "ask", "sl_link_portfolio_id": str(ids["sp"]["p"])})
    with app.app_context():
        me = db.session.get(Portfolio, ids["me"]["p"])
        assert me.sl_link_portfolio_id == ids["sp"]["p"]
        notes = []
        buys, _ = _superficial_loss_history(me, notes)
        assert buys == {ids["me"]["xeqt"]} and notes            # matched by ticker into MY security ids
    # can't link to a portfolio that isn't yours, or to itself
    for bad in ("999999", str(ids["me"]["p"])):
        c.post("/settings/update", data={"base_currency": "CAD", "balanced_threshold": "0.5", "trade_cost": "0",
                                         "taxable_sell_mode": "ask", "sl_link_portfolio_id": bad})
        with app.app_context():
            assert db.session.get(Portfolio, ids["me"]["p"]).sl_link_portfolio_id is None


def test_create_copy_and_delete(two_portfolios):
    app, c, ids = two_portfolios
    from app import db
    from models import Account, AssetClass, Portfolio, Security, Target
    c.post("/portfolios/add", data={"name": "Kid", "copy_setup": "on"})
    with app.app_context():
        kid = Portfolio.query.filter_by(user_id=ids["user"], name="Kid").one()
        kid_id = kid.id
        assert {s.ticker for s in kid.securities} == {"XEQT", "ZAG"}
        assert {s.id for s in kid.securities}.isdisjoint({ids["me"]["xeqt"], ids["me"]["zag"]})
        assert all(s.asset_class.portfolio_id == kid_id for s in kid.securities)
        assert sorted(t.target_percentage for t in kid.targets) == [20, 80]
        assert all(t.asset_class.portfolio_id == kid_id for t in kid.targets)
        assert not kid.accounts
    page = c.get("/accounts").get_data(as_text=True)          # new portfolio became active
    assert "Me-TFSA" not in page
    c.post("/accounts/add", data={"name": "Kid-RESP", "is_registered": "on"})
    c.post(f"/portfolios/{ids['sp']['p']}/delete", data={"confirm_name": "wrong"})
    with app.app_context():
        assert db.session.get(Portfolio, ids["sp"]["p"]) is not None
        me = db.session.get(Portfolio, ids["me"]["p"]); me.sl_link_portfolio_id = kid_id; db.session.commit()
    c.post(f"/portfolios/{kid_id}/delete", data={"confirm_name": "Kid"})
    with app.app_context():
        assert db.session.get(Portfolio, kid_id) is None
        assert not Security.query.filter_by(portfolio_id=kid_id).count()
        assert not AssetClass.query.filter_by(portfolio_id=kid_id).count()
        assert not Target.query.filter_by(portfolio_id=kid_id).count()
        assert not Account.query.filter_by(portfolio_id=kid_id).count()
        assert db.session.get(Portfolio, ids["me"]["p"]).sl_link_portfolio_id is None
    assert c.get("/dashboard").status_code == 200              # falls back to a remaining portfolio
    c.post(f"/portfolios/{ids['sp']['p']}/delete", data={"confirm_name": "Spouse"})
    c.post(f"/portfolios/{ids['me']['p']}/delete", data={"confirm_name": "Me"})
    with app.app_context():
        assert Portfolio.query.filter_by(user_id=ids["user"]).count() == 1      # the last one stays


def test_new_login_gets_a_default_portfolio():
    from app import app, db
    from models import Portfolio, User
    with app.app_context():
        u = User(google_id="brand-new", email="new@example.com", name="New")
        db.session.add(u); db.session.commit(); uid = u.id
    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(uid); s["_fresh"] = True
    assert c.get("/dashboard").status_code == 200
    with app.app_context():
        assert [p.name for p in Portfolio.query.filter_by(user_id=uid)] == ["My Portfolio"]


# --------------------------------------------------------------------------- #
# Migration of a database created before portfolios existed
# --------------------------------------------------------------------------- #
LEGACY_DATA = """
INSERT INTO users (id, google_id, email, name, base_currency, trade_cost, taxable_sell_mode,
                   balanced_threshold, precision_rebalancing, last_plan_summary)
VALUES (1, 'g1', 'me@x.com', 'Me', 'CAD', 4.95, 'never', 1.5, 0, '{"trades": 3}'),
       (2, 'g2', 'other@x.com', 'Other', 'USD', 0, 'ask', 0.5, 1, NULL);
INSERT INTO asset_classes (id, name) VALUES (1, 'Equity'), (2, 'Bonds');
INSERT INTO securities (id, ticker, name, asset_class_id, currency, is_public, auto_update_price, is_illiquid, last_price)
VALUES (1, 'XEQT', 'iShares', 1, 'CAD', 1, 1, 0, 30), (2, 'ZAG', 'BMO', 2, 'CAD', 1, 1, 0, 14);
INSERT INTO targets (id, user_id, asset_class_id, target_percentage) VALUES (1, 1, 1, 80), (2, 1, 2, 20), (3, 2, 1, 100);
INSERT INTO accounts (id, user_id, name, account_type, currency, is_registered, priority, cash_balance)
VALUES (1, 1, 'TFSA', 'TFSA', 'CAD', 1, 0, 500), (2, 2, 'Other-RRSP', 'RRSP', 'CAD', 1, 0, 0);
INSERT INTO holdings (id, account_id, security_id, quantity, price, book_value)
VALUES (1, 1, 1, 100, 30, 2500), (2, 2, 1, 40, 30, NULL), (3, 2, 2, 10, 14, 130);
INSERT INTO security_preferences (id, security_id, user_id, restriction_type, account_config)
VALUES (1, 2, 2, 'restricted_to_accounts', '{"allowed": [2]}');
INSERT INTO rebalance_transactions (id, user_id, account_id, security_id, action, quantity, price, amount, executed)
VALUES (1, 2, 2, 1, 'BUY', 1, 30, 30, 1);
"""

CHECK = r"""
import json
from sqlalchemy import inspect, text
from app import app, db
from models import *
with app.app_context():
    out = {}
    out['portfolios'] = [(p.user_id, p.name, p.base_currency, p.trade_cost, p.taxable_sell_mode,
                          p.balanced_threshold, bool(p.precision_rebalancing), p.last_plan_summary)
                         for p in Portfolio.query.order_by(Portfolio.user_id)]
    leaks = []
    for h in Holding.query.all():
        if h.security.portfolio_id != h.account.portfolio_id: leaks.append(('holding', h.id))
        if h.security.asset_class.portfolio_id != h.account.portfolio_id: leaks.append(('class', h.id))
    for M in (Target, SecurityPreference, RebalanceTransaction):
        for r in M.query.all():
            ref = getattr(r, 'asset_class', None) or getattr(r, 'security', None)
            if r.portfolio_id is None or ref.portfolio_id != r.portfolio_id: leaks.append((M.__tablename__, r.id))
    out['leaks'] = leaks
    out['holdings'] = sorted((h.account.name, h.security.ticker, h.quantity, h.book_value) for h in Holding.query.all())
    out['securities'] = sorted((s.portfolio.user_id, s.ticker, s.last_price) for s in Security.query.all())
    out['uniques'] = inspect(db.engine).get_unique_constraints('securities')
    out['integrity'] = db.session.execute(text('PRAGMA integrity_check')).scalar()
    out['fk'] = db.session.execute(text('PRAGMA foreign_key_check')).fetchall() == []
    print('RESULT' + json.dumps(out))
"""


def _run(db_path, code):
    env = dict(os.environ, SECRET_KEY="x", DATABASE_URL=f"sqlite:///{db_path}")
    r = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env, capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, r.stderr[-3000:]
    return r


def test_migrates_pre_portfolio_database():
    db_path = os.path.join(tempfile.mkdtemp(), "legacy.db")
    con = sqlite3.connect(db_path)
    with open(os.path.join(ROOT, "tests", "fixtures", "legacy_schema.sql")) as f:
        con.executescript(f.read())
    con.executescript(LEGACY_DATA)
    con.commit(); con.close()

    r = _run(db_path, CHECK)
    out = json.loads(r.stdout.split("RESULT", 1)[1])
    # settings moved from the user row to each user's default portfolio
    assert out["portfolios"] == [
        [1, "My Portfolio", "CAD", 4.95, "never", 1.5, False, '{"trades": 3}'],
        [2, "My Portfolio", "USD", 0.0, "ask", 0.5, True, None],
    ]
    # global securities split: user 2 gets their own copies; nothing points across portfolios
    assert out["leaks"] == []
    assert out["securities"] == [[1, "XEQT", 30.0], [1, "ZAG", 14.0], [2, "XEQT", 30.0], [2, "ZAG", 14.0]]
    assert out["holdings"] == [["Other-RRSP", "XEQT", 40.0, None], ["Other-RRSP", "ZAG", 10.0, None],  # registered: ACB cleared
                               ["TFSA", "XEQT", 100.0, None]]
    assert [u["column_names"] for u in out["uniques"]] == [["portfolio_id", "ticker"]]
    assert out["integrity"] == "ok" and out["fk"]

    # second start: nothing left to do, same result
    r2 = _run(db_path, CHECK)
    assert "Migrated" not in r2.stderr
    assert json.loads(r2.stdout.split("RESULT", 1)[1]) == out
