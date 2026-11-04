"""Recording an executed trade with its actual fill: price, quantity, commission, trade
date and (taxable, foreign-currency) the exchange rate on the trade date.

Run:  python -m pytest tests/ -q
"""
import os
import sys
import tempfile
from datetime import date, datetime, timedelta

import pytest

pytest.importorskip("flask_sqlalchemy")
pytest.importorskip("flask_login")
pytest.importorskip("authlib")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
if "app" not in sys.modules:                       # share the app DB if another test set it up
    os.environ.setdefault("SECRET_KEY", "test")
    os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(tempfile.mkdtemp(), 'portfolio.db')}"

SPOT = 1.38


@pytest.fixture()
def env(monkeypatch):
    """A CAD portfolio with a taxable and a registered account, a CAD and a USD security."""
    import app as appmod
    from app import app, db
    from models import (Account, AssetClass, ExchangeRate, Holding, Portfolio, Security, Target, User)

    # Never hit the network for Bank of Canada rates in tests.
    def no_boc(*a, **k):
        raise appmod.BocRateError("offline")
    monkeypatch.setattr(appmod, "boc_rate", no_boc)

    with app.app_context():
        ExchangeRate.query.delete()
        for frm, to, r in (("USD", "CAD", SPOT), ("CAD", "USD", 1 / SPOT)):
            db.session.add(ExchangeRate(from_currency=frm, to_currency=to, rate=r, date=datetime.utcnow(), source="test"))
        n = User.query.count() + 1
        u = User(google_id=f"exec-{n}", email=f"exec{n}@example.com", name="Exec")
        db.session.add(u); db.session.flush()
        p = Portfolio(user_id=u.id, name="Exec", base_currency="CAD", trade_cost=0.0)
        db.session.add(p); db.session.flush()
        eq = AssetClass(portfolio_id=p.id, name="Equity")
        db.session.add(eq); db.session.flush()
        db.session.add(Target(user_id=u.id, portfolio_id=p.id, asset_class_id=eq.id, target_percentage=100))
        xic = Security(portfolio_id=p.id, ticker="XIC", asset_class_id=eq.id, currency="CAD", last_price=40.0)
        vti = Security(portfolio_id=p.id, ticker="VTI", asset_class_id=eq.id, currency="USD", last_price=300.0)
        db.session.add_all([xic, vti]); db.session.flush()
        nr = Account(user_id=u.id, portfolio_id=p.id, name="Non-Reg", is_registered=False, cash_balance=10000.0, currency="CAD")
        rr = Account(user_id=u.id, portfolio_id=p.id, name="RRSP", is_registered=True, cash_balance=10000.0, currency="CAD")
        db.session.add_all([nr, rr]); db.session.flush()
        db.session.add_all([
            Holding(account_id=nr.id, security_id=xic.id, quantity=100, price=40.0, book_value=3000.0),
            Holding(account_id=nr.id, security_id=vti.id, quantity=10, price=300.0, book_value=2500.0),
        ])
        db.session.commit()
        ids = dict(user=u.id, p=p.id, xic=xic.id, vti=vti.id, nr=nr.id, rr=rr.id)

    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(ids["user"]); s["_fresh"] = True
    with app.app_context():                         # make this portfolio the current one
        pass
    c.post(f"/portfolios/{ids['p']}/switch")

    def add_trade(account, security, action, qty, price, ccy, final=False):
        from models import RebalanceTransaction
        with app.app_context():
            t = RebalanceTransaction(user_id=ids["user"], portfolio_id=ids["p"], account_id=ids[account],
                                     security_id=ids[security], action=action, quantity=qty, price=price,
                                     amount=round(qty * price, 2), currency=ccy, execution_order=1, is_final_trade=final)
            db.session.add(t); db.session.commit()
            return t.id

    # Re-planning after each trade isn't what these tests are about (and would replace
    # the hand-made trades); keep it a no-op.
    monkeypatch.setattr(appmod, "_regenerate_plan", lambda override=None, **kw: [])
    return app, db, c, ids, add_trade


def _get(app, model, id_):
    from app import db
    with app.app_context():
        obj = db.session.get(model, id_)
        db.session.expunge(obj)
        return obj


def _holding(app, account_id, security_id):
    from models import Holding
    with app.app_context():
        return Holding.query.filter_by(account_id=account_id, security_id=security_id).first()


def test_sell_records_actual_fill_commission_and_date(env):
    from models import Account, RebalanceTransaction
    app, db, c, ids, add_trade = env
    tid = add_trade("nr", "xic", "SELL", 50, 40.0, "CAD")
    day = (date.today() - timedelta(days=2)).isoformat()
    r = c.post(f"/rebalance/execute/{tid}", data={"fill_price": "41.10", "quantity": "48", "commission": "4.95",
                                                  "trade_date": day})
    assert r.status_code == 302
    t = _get(app, RebalanceTransaction, tid)
    assert t.executed and (t.quantity, t.price) == (48, 41.10) and (t.planned_quantity, t.planned_price) == (50, 40.0)
    assert t.commission == 4.95 and t.trade_date.isoformat() == day and t.fx_rate is None
    acb_sold = 3000.0 * 48 / 100
    assert abs(t.realized_gain - (48 * 41.10 - 4.95 - acb_sold)) < 0.01
    h = _holding(app, ids["nr"], ids["xic"])
    assert h.quantity == 52 and abs(h.book_value - (3000.0 - acb_sold)) < 0.01
    assert abs(_get(app, Account, ids["nr"]).cash_balance - (10000 + 48 * 41.10 - 4.95)) < 0.01


def test_posting_nothing_means_as_planned(env):
    from models import RebalanceTransaction
    app, db, c, ids, add_trade = env
    tid = add_trade("nr", "xic", "BUY", 10, 40.0, "CAD")
    c.post(f"/rebalance/execute/{tid}")
    t = _get(app, RebalanceTransaction, tid)
    assert t.executed and (t.quantity, t.price, t.commission) == (10, 40.0, 0.0) and t.trade_date == date.today()
    assert _holding(app, ids["nr"], ids["xic"]).book_value == 3400.0


def test_taxable_usd_buy_uses_entered_rate_and_adds_commission_to_acb(env):
    from models import Account, RebalanceTransaction
    app, db, c, ids, add_trade = env
    tid = add_trade("nr", "vti", "BUY", 5, 300.0, "USD")
    c.post(f"/rebalance/execute/{tid}", data={"fill_price": "301", "quantity": "5", "commission": "1",
                                              "trade_date": date.today().isoformat(),
                                              "fx_rate": "1.3912", "fx_rate_source": "boc"})
    t = _get(app, RebalanceTransaction, tid)
    assert (t.fx_rate, t.fx_rate_source) == (1.3912, "boc")
    h = _holding(app, ids["nr"], ids["vti"])
    assert abs(h.book_value - (2500.0 + 5 * 301 + 1)) < 1e-6           # USD, commission included
    # cash still moves at spot (the broker's own conversion isn't known)
    assert abs(_get(app, Account, ids["nr"]).cash_balance - (10000 - (5 * 301 + 1) * SPOT)) < 0.01


def test_taxable_usd_sell_gain_converted_at_trade_date_rate(env, monkeypatch):
    import app as appmod
    from models import RebalanceTransaction
    app, db, c, ids, add_trade = env
    seen = {}

    def fake_boc(frm, to, on):
        seen["args"] = (frm, to, on)
        return 1.40, on
    monkeypatch.setattr(appmod, "boc_rate", fake_boc)
    tid = add_trade("nr", "vti", "SELL", 4, 300.0, "USD")
    day = date.today() - timedelta(days=3)
    c.post(f"/rebalance/execute/{tid}", data={"fill_price": "310", "quantity": "4", "commission": "0",
                                              "trade_date": day.isoformat()})          # no rate entered → BoC lookup
    t = _get(app, RebalanceTransaction, tid)
    assert seen["args"] == ("USD", "CAD", day) and (t.fx_rate, t.fx_rate_source) == (1.40, "boc")
    assert abs(t.realized_gain - (4 * 310 - 2500.0 * 4 / 10) * 1.40) < 0.01


def test_no_boc_rate_falls_back_to_spot(env):
    from models import RebalanceTransaction
    app, db, c, ids, add_trade = env
    tid = add_trade("nr", "vti", "SELL", 1, 300.0, "USD")
    c.post(f"/rebalance/execute/{tid}", data={"fill_price": "300", "quantity": "1"})
    t = _get(app, RebalanceTransaction, tid)
    assert t.fx_rate_source == "spot" and abs(t.fx_rate - SPOT) < 1e-9


def test_registered_usd_trade_has_no_rate_but_keeps_date(env):
    from models import RebalanceTransaction
    app, db, c, ids, add_trade = env
    tid = add_trade("rr", "vti", "BUY", 2, 300.0, "USD")
    day = (date.today() - timedelta(days=1)).isoformat()
    c.post(f"/rebalance/execute/{tid}", data={"fill_price": "299", "quantity": "2", "trade_date": day,
                                              "fx_rate": "9.99"})                       # ignored: registered
    t = _get(app, RebalanceTransaction, tid)
    assert t.fx_rate is None and t.trade_date.isoformat() == day
    assert _holding(app, ids["rr"], ids["vti"]).book_value is None


@pytest.mark.parametrize("data", [
    {"trade_date": (date.today() + timedelta(days=1)).isoformat()},
    {"quantity": "0"},
    {"fill_price": "-1"},
    {"commission": "-2"},
    {"fill_price": "abc"},
])
def test_bad_input_is_rejected_and_nothing_changes(env, data):
    from models import RebalanceTransaction
    app, db, c, ids, add_trade = env
    tid = add_trade("nr", "xic", "BUY", 10, 40.0, "CAD")
    c.post(f"/rebalance/execute/{tid}", data=data)
    t = _get(app, RebalanceTransaction, tid)
    assert not t.executed and t.quantity == 10
    assert _holding(app, ids["nr"], ids["xic"]).quantity == 100


def test_cannot_record_twice(env):
    app, db, c, ids, add_trade = env
    tid = add_trade("nr", "xic", "BUY", 10, 40.0, "CAD")
    c.post(f"/rebalance/execute/{tid}")
    c.post(f"/rebalance/execute/{tid}")
    assert _holding(app, ids["nr"], ids["xic"]).quantity == 110


def test_small_cash_shortfall_on_last_buy_is_absorbed(env):
    from models import Account
    app, db, c, ids, add_trade = env
    with app.app_context():
        db.session.get(Account, ids["nr"]).cash_balance = 400.0; db.session.commit()
    tid = add_trade("nr", "xic", "BUY", 10, 40.0, "CAD", final=True)
    c.post(f"/rebalance/execute/{tid}", data={"fill_price": "40.03", "quantity": "10"})   # 0.30 over
    assert _get(app, Account, ids["nr"]).cash_balance == 0.0
    assert _holding(app, ids["nr"], ids["xic"]).quantity == 110


def test_large_cash_shortfall_is_rejected(env):
    from models import Account
    app, db, c, ids, add_trade = env
    with app.app_context():
        db.session.get(Account, ids["nr"]).cash_balance = 100.0; db.session.commit()
    tid = add_trade("nr", "xic", "BUY", 10, 40.0, "CAD")
    c.post(f"/rebalance/execute/{tid}")
    assert _get(app, Account, ids["nr"]).cash_balance == 100.0
    assert _holding(app, ids["nr"], ids["xic"]).quantity == 100


def test_superficial_loss_window_counts_from_trade_date(env):
    from models import Portfolio, RebalanceTransaction
    from rebalancer import _superficial_loss_history
    app, db, c, ids, add_trade = env
    with app.app_context():
        recorded_now = datetime.utcnow()
        for sec, days_ago in (("xic", 40), ("vti", 10)):
            db.session.add(RebalanceTransaction(
                user_id=ids["user"], portfolio_id=ids["p"], account_id=ids["nr"], security_id=ids[sec], action="SELL",
                quantity=1, price=1, amount=1, currency="CAD", executed=True, executed_at=recorded_now,
                trade_date=date.today() - timedelta(days=days_ago), realized_gain=-50.0))
        # an old row without a trade date still uses executed_at
        db.session.add(RebalanceTransaction(
            user_id=ids["user"], portfolio_id=ids["p"], account_id=ids["rr"], security_id=ids["xic"], action="BUY",
            quantity=1, price=1, amount=1, currency="CAD", executed=True, executed_at=recorded_now - timedelta(days=5)))
        db.session.commit()
        buys, loss_sells = _superficial_loss_history(db.session.get(Portfolio, ids["p"]), [])
    assert loss_sells == {ids["vti"]}             # 40-day-old loss sale is outside the window
    assert buys == {ids["xic"]}                   # registered buy counts too


def test_rebalance_page_dialog_fields(env):
    app, db, c, ids, add_trade = env
    add_trade("nr", "vti", "BUY", 1, 300.0, "USD")
    add_trade("rr", "xic", "BUY", 1, 40.0, "CAD")
    page = c.get("/rebalance").get_data(as_text=True)
    assert page.count("data-exec-form") == 2
    assert page.count('name="fx_rate"') == 1               # taxable USD only
    assert page.count('name="commission"') == 1            # taxable only, since trade cost is 0
    assert page.count('name="trade_date"') == 2            # always


def test_boc_api_endpoint(env, monkeypatch):
    import app as appmod
    app, db, c, ids, add_trade = env
    monkeypatch.setattr(appmod, "boc_rate", lambda f, t, d: (1.3655, d - timedelta(days=1)))
    r = c.get("/api/fx/boc?currency=USD&date=2026-10-31")
    assert r.status_code == 200 and r.json["rate"] == 1.3655 and r.json["observed_on"] == "2026-10-30"
    assert c.get("/api/fx/boc?currency=XYZ&date=2026-10-31").status_code == 400


def test_boc_rate_parsing_and_weekend_fallback(monkeypatch):
    from services import fx

    class Resp:
        def __init__(self, ccy):
            self.ccy = ccy
        def raise_for_status(self):
            pass
        def json(self):
            s = f"FX{self.ccy}CAD"
            return {"observations": [{"d": "2026-10-29", s: {"v": "1.3901"}},
                                     {"d": "2026-10-30", s: {"v": "1.3925"}}]}
    calls = []

    def fake_get(url, timeout, params):
        calls.append((url, params))
        return Resp("EUR" if "EUR" in url else "USD")
    monkeypatch.setattr(fx.requests, "get", fake_get)
    fx._boc_cache.clear()
    rate, obs = fx.boc_rate("USD", "CAD", date(2026, 11, 1))          # Sunday → Friday's rate
    assert (rate, obs) == (1.3925, date(2026, 10, 30))
    assert calls[0][1]["end_date"] == "2026-11-01"
    fx.boc_rate("USD", "CAD", date(2026, 11, 1))
    assert len(calls) == 1                                             # past dates are cached
    rate, _ = fx.boc_rate("CAD", "USD", date(2026, 11, 1))
    assert abs(rate - 1 / 1.3925) < 1e-12

    monkeypatch.setattr(fx.requests, "get", lambda *a, **k: (_ for _ in ()).throw(OSError("down")))
    fx._boc_cache.clear()
    with pytest.raises(fx.BocRateError):
        fx.boc_rate("USD", "CAD", date(2026, 10, 1))
