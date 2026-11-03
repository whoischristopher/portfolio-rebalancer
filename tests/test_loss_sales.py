"""Selling at a loss: in Ask mode the plan may sell a non-registered holding that's at a
loss (it realises no gain) and prefers it over a registered holding; the Settings box
turns that off.

Run:  python -m pytest tests/ -q
"""
import os
import sys
import tempfile
import uuid
from datetime import date, datetime

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
def losses():
    """Bonds over target, equity under. Both the TFSA and the taxable account hold both;
    the taxable ZAG is at a loss."""
    from app import app, db
    from models import Account, AssetClass, ExchangeRate, Holding, Portfolio, Security, Target, User
    with app.app_context():
        if not ExchangeRate.query.count():
            for frm, to, r in (("USD", "CAD", 1.38), ("CAD", "USD", 1 / 1.38)):
                db.session.add(ExchangeRate(from_currency=frm, to_currency=to, rate=r,
                                            date=datetime.utcnow(), source="test"))
        tag = uuid.uuid4().hex[:8]
        u = User(google_id=f"ls-{tag}", email=f"ls-{tag}@example.com", name="Loss")
        db.session.add(u); db.session.flush()
        p = Portfolio(user_id=u.id, name="Mine", base_currency="CAD", taxable_sell_mode="ask", balanced_threshold=0.5)
        db.session.add(p); db.session.flush()
        eq, bd = AssetClass(portfolio_id=p.id, name="Equity"), AssetClass(portfolio_id=p.id, name="Bonds")
        db.session.add_all([eq, bd]); db.session.flush()
        xic = Security(portfolio_id=p.id, ticker="XIC", asset_class_id=eq.id, currency="CAD", last_price=40.0)
        zag = Security(portfolio_id=p.id, ticker="ZAG", asset_class_id=bd.id, currency="CAD", last_price=14.0)
        db.session.add_all([xic, zag]); db.session.flush()
        tfsa = Account(user_id=u.id, portfolio_id=p.id, name="TFSA", account_type="TFSA", is_registered=True)
        nr = Account(user_id=u.id, portfolio_id=p.id, name="Joint", account_type="Non-Registered",
                     is_registered=False)
        db.session.add_all([tfsa, nr]); db.session.flush()
        today = date.today()
        db.session.add_all([
            Holding(account_id=tfsa.id, security_id=zag.id, quantity=100, price=14.0),
            Holding(account_id=tfsa.id, security_id=xic.id, quantity=50, price=40.0),
            Holding(account_id=nr.id, security_id=zag.id, quantity=100, price=14.0, book_value=2000,
                    book_value_as_of=today),
            Holding(account_id=nr.id, security_id=xic.id, quantity=50, price=40.0, book_value=2000,
                    book_value_as_of=today),
            Target(user_id=u.id, portfolio_id=p.id, asset_class_id=eq.id, target_percentage=70),
            Target(user_id=u.id, portfolio_id=p.id, asset_class_id=bd.id, target_percentage=30),
        ])
        db.session.commit()
        ids = dict(user=u.id, p=p.id, tfsa=tfsa.id, nr=nr.id, zag=zag.id, xic=xic.id)
    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(ids["user"]); s["_fresh"] = True; s["portfolio_id"] = ids["p"]
    return app, db, c, ids


def _plan(app, ids):
    from models import RebalanceTransaction
    with app.app_context():
        return [(t.account_id, t.action, t.security_id, t.realized_gain)
                for t in RebalanceTransaction.query.filter_by(portfolio_id=ids["p"], executed=False)]


def test_ask_mode_sells_the_taxable_loser(losses):
    app, db, c, ids = losses
    c.post("/rebalance/generate")
    sells = [t for t in _plan(app, ids) if t[1] == "SELL"]
    assert sells and all(a == ids["nr"] and s == ids["zag"] and g < 0 for a, _, s, g in sells)
    page = c.get("/rebalance").get_data(as_text=True)
    assert "capital losses" in page and "30 days" in page
    assert "est. realised loss" in page


def test_setting_off_keeps_ask_mode_out_of_taxable_accounts(losses):
    app, db, c, ids = losses
    from models import Portfolio
    assert b"Sell non-registered holdings at a loss without asking" in c.get("/settings").data
    c.post("/settings/update", data={"base_currency": "CAD", "balanced_threshold": "0.5", "trade_cost": "0",
                                     "taxable_sell_mode": "ask", "precision_rebalancing": "on"})
    with app.app_context():
        assert db.session.get(Portfolio, ids["p"]).allow_loss_sells is False
    c.post("/rebalance/generate")
    assert not [t for t in _plan(app, ids) if t[0] == ids["nr"]]
