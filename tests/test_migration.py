"""Gradual migration ("phase out X in favour of Y"): the edit-security rules, the plan it
produces, the superficial-loss link and what happens on delete / copy.

Run:  python -m pytest tests/ -q
"""
import os
import sys
import tempfile
import uuid
from datetime import date, datetime, timedelta

import pytest

pytest.importorskip("flask_sqlalchemy")
pytest.importorskip("flask_login")
pytest.importorskip("authlib")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
if "app" not in sys.modules:
    os.environ.setdefault("SECRET_KEY", "test")
    os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(tempfile.mkdtemp(), 'portfolio.db')}"


@pytest.fixture()
def mig():
    """TFSA + taxable account; Equity holds OLD (to be phased out) and NEW; Bonds holds ZAG."""
    from app import app, db
    from models import Account, AssetClass, ExchangeRate, Holding, Portfolio, Security, Target, User
    with app.app_context():
        if not ExchangeRate.query.count():
            for frm, to, r in (("USD", "CAD", 1.38), ("CAD", "USD", 1 / 1.38)):
                db.session.add(ExchangeRate(from_currency=frm, to_currency=to, rate=r,
                                            date=datetime.utcnow(), source="test"))
        tag = uuid.uuid4().hex[:8]
        u = User(google_id=f"mg-{tag}", email=f"mg-{tag}@example.com", name="Mig")
        db.session.add(u); db.session.flush()
        p = Portfolio(user_id=u.id, name="Mine", base_currency="CAD", taxable_sell_mode="ask", balanced_threshold=0.5)
        db.session.add(p); db.session.flush()
        eq, bd = AssetClass(portfolio_id=p.id, name="Equity"), AssetClass(portfolio_id=p.id, name="Bonds")
        db.session.add_all([eq, bd]); db.session.flush()
        old = Security(portfolio_id=p.id, ticker="OLD", asset_class_id=eq.id, currency="CAD", last_price=40.0)
        new = Security(portfolio_id=p.id, ticker="NEW", asset_class_id=eq.id, currency="CAD", last_price=30.0)
        zag = Security(portfolio_id=p.id, ticker="ZAG", asset_class_id=bd.id, currency="CAD", last_price=14.0)
        db.session.add_all([old, new, zag]); db.session.flush()
        tfsa = Account(user_id=u.id, portfolio_id=p.id, name="TFSA", account_type="TFSA", is_registered=True,
                       cash_balance=0)
        nr = Account(user_id=u.id, portfolio_id=p.id, name="Joint", account_type="Non-Registered",
                     is_registered=False, cash_balance=0)
        db.session.add_all([tfsa, nr]); db.session.flush()
        db.session.add_all([
            Holding(account_id=tfsa.id, security_id=old.id, quantity=200, price=40.0),
            Holding(account_id=tfsa.id, security_id=zag.id, quantity=143, price=14.0),
            Holding(account_id=nr.id, security_id=old.id, quantity=100, price=40.0, book_value=2000,
                    book_value_as_of=date.today()),
            Target(user_id=u.id, portfolio_id=p.id, asset_class_id=eq.id, target_percentage=85.7),
            Target(user_id=u.id, portfolio_id=p.id, asset_class_id=bd.id, target_percentage=14.3),
        ])
        db.session.commit()
        ids = dict(user=u.id, p=p.id, old=old.id, new=new.id, zag=zag.id, eq=eq.id, bd=bd.id,
                   tfsa=tfsa.id, nr=nr.id)
    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(ids["user"]); s["_fresh"] = True; s["portfolio_id"] = ids["p"]
    return app, db, c, ids


def _edit(c, sec_id, ticker, asset_class, **extra):
    data = {"ticker": ticker, "name": "", "asset_class_name": asset_class, "currency": "CAD",
            "auto_update_price": "on", **extra}
    return c.post(f"/securities/{sec_id}/edit", data=data)


def _plan(app, ids):
    from models import RebalanceTransaction
    with app.app_context():
        return [(t.account_id, t.action, t.security_id, t.quantity)
                for t in RebalanceTransaction.query.filter_by(portfolio_id=ids["p"], executed=False)]


def test_edit_form_rules(mig):
    app, db, c, ids = mig
    from models import Security
    assert b"Phase out" in c.get(f"/securities/{ids['old']}/edit").data
    _edit(c, ids["old"], "OLD", "Equity", replaced_by_id=str(ids["zag"]))          # other asset class
    _edit(c, ids["old"], "OLD", "Equity", replaced_by_id=str(ids["old"]))          # itself
    with app.app_context():
        assert db.session.get(Security, ids["old"]).replaced_by_id is None
    _edit(c, ids["old"], "OLD", "Equity", replaced_by_id=str(ids["new"]), migrate_registered_now="on")
    with app.app_context():
        o = db.session.get(Security, ids["old"])
        assert o.replaced_by_id == ids["new"] and o.migrate_registered_now
        assert o.migration_sl_link is False                     # box not posted = unticked
    assert b"phasing out" in c.get("/securities").data
    # A replacement has to stay in the same asset class as what it replaces.
    _edit(c, ids["new"], "NEW", "Bonds")
    with app.app_context():
        assert db.session.get(Security, ids["new"]).asset_class_id == ids["eq"]
    # Clearing the select ends the migration and resets its options.
    _edit(c, ids["old"], "OLD", "Equity", replaced_by_id="", migrate_registered_now="on")
    with app.app_context():
        o = db.session.get(Security, ids["old"])
        assert o.replaced_by_id is None and not o.migrate_registered_now and o.migration_sl_link


def test_plan_switches_registered_now_and_leaves_taxable(mig):
    app, db, c, ids = mig
    from models import Security
    with app.app_context():
        o = db.session.get(Security, ids["old"])
        o.replaced_by_id, o.migrate_registered_now = ids["new"], True
        db.session.commit()
    c.post("/rebalance/generate")
    plan = _plan(app, ids)
    assert (ids["tfsa"], "SELL", ids["old"], 200) in plan
    assert any(a == ids["tfsa"] and act == "BUY" and s == ids["new"] for a, act, s, _ in plan)
    assert not [t for t in plan if t[0] == ids["nr"]]                   # taxable: no switch just for its own sake
    assert not [t for t in plan if t[1] == "BUY" and t[2] == ids["old"]]
    page = c.get("/rebalance").get_data(as_text=True)
    assert "Phasing out OLD" in page


def test_gradual_mode_leaves_a_balanced_portfolio_alone(mig):
    app, db, c, ids = mig
    from models import Security
    with app.app_context():
        db.session.get(Security, ids["old"]).replaced_by_id = ids["new"]
        db.session.commit()
    c.post("/rebalance/generate")
    assert _plan(app, ids) == []
    assert "switch registered accounts now" in c.get("/rebalance").get_data(as_text=True)


def test_migration_paused_when_replacement_has_no_price(mig):
    app, db, c, ids = mig
    from models import Account, Security
    with app.app_context():
        db.session.get(Security, ids["old"]).replaced_by_id = ids["new"]
        db.session.get(Security, ids["new"]).last_price = None
        db.session.get(Account, ids["tfsa"]).cash_balance = 5000          # equity now under target
        db.session.commit()
    c.post("/rebalance/generate")
    assert any(t[1] == "BUY" and t[2] == ids["old"] for t in _plan(app, ids))   # behaves normally
    assert "Phasing out OLD is paused" in c.get("/rebalance").get_data(as_text=True)


def test_superficial_loss_link_shares_recent_history(mig):
    """NEW bought in the last 30 days → OLD can't be sold at a loss in the taxable account."""
    app, db, c, ids = mig
    from models import Holding, Portfolio, RebalanceTransaction, Security
    from rebalancer import build_problem
    from services.fx import get_exchange_rates
    with app.app_context():
        db.session.get(Security, ids["old"]).replaced_by_id = ids["new"]
        h = Holding.query.filter_by(account_id=ids["nr"], security_id=ids["old"]).one()
        h.book_value = 6000                                                # OLD at a loss in the taxable account
        db.session.add(RebalanceTransaction(user_id=ids["user"], portfolio_id=ids["p"], account_id=ids["tfsa"],
                                            security_id=ids["new"], action="BUY", quantity=1, price=30.0,
                                            amount=30.0, currency="CAD", executed=True,
                                            executed_at=datetime.utcnow(), trade_date=date.today() - timedelta(days=3)))
        db.session.commit()
        p = db.session.get(Portfolio, ids["p"])
        prob, _ = build_problem(p, get_exchange_rates(p), allow_taxable_sells=True)
        assert {ids["old"], ids["new"]} <= prob.recent_buys
        old_in = next(s for s in prob.securities if s.id == ids["old"])
        assert old_in.replaced_by == ids["new"] and old_in.sl_group == ids["old"]
        # Unlinked: only NEW's own history counts.
        db.session.get(Security, ids["old"]).migration_sl_link = False
        db.session.commit()
        prob, _ = build_problem(p, get_exchange_rates(p), allow_taxable_sells=True)
        assert ids["old"] not in prob.recent_buys


def test_delete_replacement_ends_migration_and_copy_keeps_it_inside(mig):
    app, db, c, ids = mig
    from models import Portfolio, Security
    with app.app_context():
        db.session.get(Security, ids["old"]).replaced_by_id = ids["new"]
        db.session.commit()
    c.post("/portfolios/add", data={"name": "Copy", "copy_setup": "on"})
    with app.app_context():
        cp = Portfolio.query.filter_by(user_id=ids["user"], name="Copy").one()
        secs = {s.ticker: s for s in Security.query.filter_by(portfolio_id=cp.id)}
        assert secs["OLD"].replaced_by_id == secs["NEW"].id
    with c.session_transaction() as s:
        s["portfolio_id"] = ids["p"]
    c.post(f"/securities/{ids['new']}/delete")
    with app.app_context():
        assert db.session.get(Security, ids["old"]).replaced_by_id is None
