"""Price freshness: 'prices as of', background refresh when stale, refresh before planning,
and the per-login refresh interval setting. The Google Sheet read is stubbed out.

Run:  python -m pytest tests/ -q
"""
import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timedelta

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
def priced(monkeypatch):
    """A login with a price sheet, one portfolio (XEQT auto-priced, PRIV typed by hand),
    and a stubbed sheet whose prices and failures the test controls."""
    import app as appmod
    from app import app, db
    from models import (Account, AssetClass, ExchangeRate, Holding, Portfolio, Security, Target, User)
    sheet = {"prices": {"XEQT": 31.0, "PRIV": 99.0}, "calls": 0, "error": None}

    def fake_fetch(user):
        sheet["calls"] += 1
        if sheet["error"]:
            raise appmod.PriceSheetError(sheet["error"])
        return dict(sheet["prices"])
    monkeypatch.setattr(appmod, "fetch_prices_from_user_sheet", fake_fetch)

    with app.app_context():
        if not ExchangeRate.query.count():
            for frm, to, r in (("USD", "CAD", 1.38), ("CAD", "USD", 1 / 1.38)):
                db.session.add(ExchangeRate(from_currency=frm, to_currency=to, rate=r,
                                            date=datetime.utcnow(), source="test"))
        tag = uuid.uuid4().hex[:8]
        u = User(google_id=f"pr-{tag}", email=f"pr-{tag}@example.com", name="Prices", price_sheet_id="sheet-1")
        db.session.add(u); db.session.flush()
        p = Portfolio(user_id=u.id, name="Mine", base_currency="CAD")
        db.session.add(p); db.session.flush()
        eq, pe = AssetClass(portfolio_id=p.id, name="Equity"), AssetClass(portfolio_id=p.id, name="Private")
        db.session.add_all([eq, pe]); db.session.flush()
        x = Security(portfolio_id=p.id, ticker="XEQT", asset_class_id=eq.id, currency="CAD", last_price=30.0)
        v = Security(portfolio_id=p.id, ticker="PRIV", asset_class_id=pe.id, currency="CAD", last_price=10.0,
                     auto_update_price=False)
        a = Account(user_id=u.id, portfolio_id=p.id, name="TFSA", account_type="TFSA", is_registered=True,
                    cash_balance=1000)
        db.session.add_all([x, v, a]); db.session.flush()
        db.session.add_all([Holding(account_id=a.id, security_id=x.id, quantity=100, price=30.0),
                            Holding(account_id=a.id, security_id=v.id, quantity=10, price=10.0),
                            Target(user_id=u.id, portfolio_id=p.id, asset_class_id=eq.id, target_percentage=95),
                            Target(user_id=u.id, portfolio_id=p.id, asset_class_id=pe.id, target_percentage=5)])
        db.session.commit()
        ids = {"user": u.id, "p": p.id, "xeqt": x.id, "priv": v.id}
    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(ids["user"]); s["_fresh"] = True; s["portfolio_id"] = ids["p"]
    return app, db, c, ids, sheet


def _set_user(app, db, uid, **kw):
    from models import User
    with app.app_context():
        u = db.session.get(User, uid)
        for k, v in kw.items():
            setattr(u, k, v)
        db.session.commit()


def test_staleness_rules(priced):
    app, db, c, ids, sheet = priced
    from app import prices_stale
    from models import User
    now = datetime.utcnow()
    cases = [  # (price_sheet_id, minutes, updated_at, expected)
        ("sheet-1", 15, None, True),                               # never updated
        ("sheet-1", 15, now - timedelta(minutes=5), False),
        ("sheet-1", 15, now - timedelta(minutes=20), True),
        ("sheet-1", 0, now - timedelta(days=3), False),            # 0 = auto-refresh off
        (None, 15, None, False),                                   # no sheet: nothing to refresh from
        ("sheet-1", None, now - timedelta(minutes=20), True),      # unset → default 15
    ]
    for sheet_id, minutes, at, expected in cases:
        _set_user(app, db, ids["user"], price_sheet_id=sheet_id, price_refresh_minutes=minutes, prices_updated_at=at)
        with app.app_context():
            assert prices_stale(db.session.get(User, ids["user"])) is expected, (sheet_id, minutes, at)


def test_background_refresh_only_when_stale(priced):
    app, db, c, ids, sheet = priced
    from models import Security, User
    page = c.get("/dashboard").get_data(as_text=True)
    assert 'data-stale="1"' in page and "Prices as of" in page

    j = c.post("/api/prices/refresh-if-stale").get_json()
    assert j["refreshed"] and j["updated"] == 1 and j["as_of"].endswith("Z")
    with app.app_context():
        assert db.session.get(Security, ids["xeqt"]).last_price == 31.0
        assert db.session.get(Security, ids["priv"]).last_price == 10.0     # auto-update off: left alone
        assert db.session.get(User, ids["user"]).prices_updated_at is not None

    assert c.post("/api/prices/refresh-if-stale").get_json() == {"refreshed": False}   # fresh now
    assert sheet["calls"] == 1
    assert 'data-stale="0"' in c.get("/dashboard").get_data(as_text=True)


def test_failed_refresh_keeps_old_prices_and_timestamp(priced):
    app, db, c, ids, sheet = priced
    from models import Security, User
    sheet["error"] = "Sheet not found"
    j = c.post("/api/prices/refresh-if-stale").get_json()
    assert j == {"refreshed": False, "error": "Sheet not found"}
    with app.app_context():
        assert db.session.get(Security, ids["xeqt"]).last_price == 30.0
        assert db.session.get(User, ids["user"]).prices_updated_at is None
    # The manual button reports it and goes back to the page it came from.
    r = c.post("/update_prices", headers={"Referer": "http://localhost/dashboard"})
    assert r.headers["Location"].endswith("/dashboard")


def test_plan_refreshes_stale_prices_first_and_records_them(priced):
    app, db, c, ids, sheet = priced
    from models import Portfolio, User
    c.post("/rebalance/generate")
    assert sheet["calls"] == 1
    with app.app_context():
        summary = json.loads(db.session.get(Portfolio, ids["p"]).last_plan_summary)
        as_of = db.session.get(User, ids["user"]).prices_updated_at
        assert summary["prices_as_of"] == as_of.isoformat() + "Z" and summary["generated_at"].endswith("Z")

    c.post("/rebalance/generate")                      # prices fresh → no second read
    assert sheet["calls"] == 1

    # Sheet failing while stale: the plan is still made, on the old prices, with a warning.
    old = datetime.utcnow() - timedelta(hours=2)
    _set_user(app, db, ids["user"], prices_updated_at=old)
    sheet["error"] = "Quota exceeded"
    r = c.post("/rebalance/generate", follow_redirects=True)
    assert b"Prices could not be updated (Quota exceeded)" in r.data
    with app.app_context():
        # …and says which prices: the last successful read, not now
        assert json.loads(db.session.get(Portfolio, ids["p"]).last_plan_summary)["prices_as_of"] == old.isoformat() + "Z"


def test_rebalance_page_flags_plan_older_than_prices(priced):
    app, db, c, ids, sheet = priced
    sheet["prices"]["XEQT"] = 25.0            # big drop so the plan has trades
    c.post("/rebalance/generate")
    page = c.get("/rebalance").get_data(as_text=True)
    assert "using prices from" in page and "Prices have been updated since this plan" not in page
    _set_user(app, db, ids["user"], prices_updated_at=datetime.utcnow() + timedelta(minutes=1))
    assert "Prices have been updated since this plan" in c.get("/rebalance").get_data(as_text=True)


def test_refresh_interval_setting(priced):
    app, db, c, ids, sheet = priced
    from models import User
    form = {"base_currency": "CAD", "balanced_threshold": "0.5", "trade_cost": "0", "taxable_sell_mode": "ask"}
    for raw, expected in (("30", 30), ("0", 0), ("99999", 1440), ("-5", 0)):
        c.post("/settings/update", data={**form, "price_refresh_minutes": raw})
        with app.app_context():
            assert db.session.get(User, ids["user"]).price_refresh_minutes == expected, raw
    r = c.post("/settings/update", data={**form, "price_refresh_minutes": "soon"}, follow_redirects=True)
    assert b"whole number of minutes" in r.data
    assert 'name="price_refresh_minutes"' in c.get("/settings").get_data(as_text=True)


def test_sheet_gaps_banner_after_a_read(priced):
    """After a read the app remembers the sheet's tickers; a security with no row gets a
    banner with rows to paste – including one added after the read, without re-reading."""
    app, db, c, ids, sheet = priced
    from models import AssetClass, Security
    from services.prices import SheetPrices
    sheet["prices"] = SheetPrices({"TSE:XEQT": 31.0, "TSE:OLD": 5.0}, ["TSE:XEQT", "TSE:OLD", "TSE:BRKN"])
    c.post("/update_prices")
    page = c.get("/dashboard").get_data(as_text=True)
    assert "Not in your price sheet" not in page                  # XEQT has a row; PRIV isn't auto-priced

    with app.app_context():
        eq = AssetClass.query.filter_by(portfolio_id=ids["p"], name="Equity").first()
        db.session.add_all([Security(portfolio_id=ids["p"], ticker="ZAG", asset_class_id=eq.id, currency="CAD",
                                     last_price=14.0),
                            Security(portfolio_id=ids["p"], ticker="BRKN", asset_class_id=eq.id, currency="CAD",
                                     last_price=1.0)])
        db.session.commit()
    assert sheet["calls"] == 1
    page = c.get("/securities").get_data(as_text=True)
    assert "Not in your price sheet:</strong> ZAG" in page
    assert 'TSE:ZAG\t=GOOGLEFINANCE(INDIRECT(&#34;A&#34;&amp;ROW()),&#34;price&#34;)' in page
    assert "In the sheet but no price" in page and "TSE:BRKN" in page
    assert "TSE:OLD" not in page                                  # unused rows: Settings only
    assert "TSE:OLD" in c.get("/settings").get_data(as_text=True)
    assert "Not in your price sheet" not in c.get("/accounts").get_data(as_text=True)


def test_no_banner_before_the_first_read(priced):
    app, db, c, ids, sheet = priced
    assert "Not in your price sheet" not in c.get("/dashboard").get_data(as_text=True)
