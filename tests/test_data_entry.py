"""Data entry: registered-type auto flag, account editing, ticker-first holding rows that
create securities/asset classes inline, and the quick-start wizard.

Run:  python -m pytest tests/ -q
"""
import os
import sys
import tempfile
import uuid
from datetime import datetime

import pytest

pytest.importorskip("flask_sqlalchemy")
pytest.importorskip("flask_login")
pytest.importorskip("authlib")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
if "app" not in sys.modules:
    os.environ.setdefault("SECRET_KEY", "test")
    os.environ["DATABASE_URL"] = f"sqlite:///{os.path.join(tempfile.mkdtemp(), 'portfolio.db')}"


def test_registered_flag_rules():
    from services.entry import registered_flag, suggest_currency
    assert registered_flag("RRSP", None) is True           # registered type wins over an unticked box
    assert registered_flag("LIRA", "") is True
    assert registered_flag("FHSA", None) is True
    assert registered_flag("Non-Registered", "on") is False
    assert registered_flag("Other", "on") is True            # free choice for anything else
    assert registered_flag("", None) is False
    assert suggest_currency("XIC.TO", "USD") == "CAD"
    assert suggest_currency("NYSEARCA:ITOT") == "USD"
    assert suggest_currency("DLR.U") == "USD"
    assert suggest_currency("VFV", "CAD") == "CAD"


@pytest.fixture()
def fresh_user():
    """A brand-new login with one (empty) portfolio, and a logged-in client."""
    from app import app, db
    from models import ExchangeRate, Portfolio, User
    with app.app_context():
        if not ExchangeRate.query.count():
            for frm, to, r in (("USD", "CAD", 1.38), ("CAD", "USD", 1 / 1.38)):
                db.session.add(ExchangeRate(from_currency=frm, to_currency=to, rate=r,
                                            date=datetime.utcnow(), source="test"))
        tag = uuid.uuid4().hex[:8]
        u = User(google_id=f"de-{tag}", email=f"de-{tag}@example.com", name="Entry")
        db.session.add(u); db.session.flush()
        p = Portfolio(user_id=u.id, name="Mine", base_currency="CAD")
        db.session.add(p); db.session.commit()
        ids = {"user": u.id, "p": p.id}
    c = app.test_client()
    with c.session_transaction() as s:
        s["_user_id"] = str(ids["user"]); s["_fresh"] = True; s["portfolio_id"] = ids["p"]
    return app, db, c, ids


def test_accounts_add_and_edit(fresh_user):
    app, db, c, ids = fresh_user
    from models import Account
    c.post("/accounts/add", data={"name": "My LIRA", "account_type": "LIRA", "currency": "CAD"})   # box unticked
    c.post("/accounts/add", data={"name": "Cash", "account_type": "Non-Registered", "is_registered": "on"})
    with app.app_context():
        lira = Account.query.filter_by(portfolio_id=ids["p"], name="My LIRA").one()
        cash = Account.query.filter_by(portfolio_id=ids["p"], name="Cash").one()
        assert lira.is_registered and not cash.is_registered
        lira_id = lira.id
    r = c.post(f"/accounts/{lira_id}/edit", data={"name": "Locked-in", "account_type": "Non-Registered",
                                                   "currency": "USD", "is_registered": "on"})
    assert r.status_code == 302
    with app.app_context():
        a = db.session.get(Account, lira_id)
        assert (a.name, a.account_type, a.currency, a.is_registered) == ("Locked-in", "Non-Registered", "USD", False)


def _rows(**cols):
    """Build parallel-list form data for holding rows."""
    n = max(len(v) for v in cols.values())
    data = {}
    for k in ("ticker", "quantity", "book_value", "price", "name", "asset_class", "currency"):
        data[f"h_{k}[]"] = list(cols.get(k, [""] * n)) + [""] * (n - len(cols.get(k, [""] * n)))
    return data


def test_holding_rows_create_securities_and_merge(fresh_user):
    app, db, c, ids = fresh_user
    from models import Account, AssetClass, Holding, Security
    with app.app_context():
        acc = Account(user_id=ids["user"], portfolio_id=ids["p"], name="Taxable", is_registered=False, currency="CAD")
        db.session.add(acc); db.session.commit(); acc_id = acc.id

    data = _rows(ticker=["XEQT", "VFV", ""], quantity=["100", "1,000", ""], book_value=["2,900", "", ""],
                 price=["30", "", ""], name=["iShares equity", "Vanguard S&P", ""],
                 asset_class=["Equity", "US Equity", ""], currency=["CAD", "", ""])
    data["account_id"] = acc_id
    assert c.post("/holdings/add", data=data).status_code == 302
    with app.app_context():
        secs = {s.ticker: s for s in Security.query.filter_by(portfolio_id=ids["p"])}
        assert set(secs) == {"XEQT", "VFV"}
        assert secs["XEQT"].last_price == 30 and secs["VFV"].last_price is None
        assert {a.name for a in AssetClass.query.filter_by(portfolio_id=ids["p"])} == {"Equity", "US Equity"}
        h = Holding.query.filter_by(account_id=acc_id, security_id=secs["VFV"].id).one()
        assert h.quantity == 1000 and h.book_value is None
        xeqt_id = secs["XEQT"].id

    # Same ticker again (different case, exchange suffix) tops up the position instead of duplicating it,
    # and "equity" reuses the existing asset class.
    data = _rows(ticker=["xeqt.to"], quantity=["50"], book_value=["1600"])
    data["account_id"] = acc_id
    c.post("/holdings/add", data=data)
    with app.app_context():
        hs = Holding.query.filter_by(account_id=acc_id, security_id=xeqt_id).all()
        assert len(hs) == 1 and hs[0].quantity == 150 and abs(hs[0].book_value - 4500) < 1e-6
        assert Security.query.filter_by(portfolio_id=ids["p"]).count() == 2


def test_bad_row_writes_nothing(fresh_user):
    app, db, c, ids = fresh_user
    from models import Account, Holding, Security
    with app.app_context():
        acc = Account(user_id=ids["user"], portfolio_id=ids["p"], name="RRSP", is_registered=True)
        db.session.add(acc); db.session.commit(); acc_id = acc.id
    # Row 2 is a new ticker without an asset class → the whole batch is rejected.
    data = _rows(ticker=["ZAG", "NEWONE"], quantity=["10", "5"], asset_class=["Bonds", ""])
    data["account_id"] = acc_id
    r = c.post("/holdings/add", data=data, follow_redirects=True)
    assert b"NEWONE is a new security" in r.data
    with app.app_context():
        assert Holding.query.filter_by(account_id=acc_id).count() == 0
        assert Security.query.filter_by(portfolio_id=ids["p"]).count() == 0
    data = _rows(ticker=["ZAG"], quantity=["abc"], asset_class=["Bonds"])
    data["account_id"] = acc_id
    assert b"Quantity must be a number" in c.post("/holdings/add", data=data, follow_redirects=True).data


def test_wizard_end_to_end(fresh_user):
    app, db, c, ids = fresh_user
    from models import Account, Holding, Target
    assert c.get("/setup").headers["Location"].endswith("/setup/accounts")
    assert c.get("/setup/accounts").status_code == 200
    r = c.post("/setup/accounts", data={"a_name[]": ["TFSA", "Joint", ""], "a_type[]": ["TFSA", "Non-Registered", ""],
                                        "a_currency[]": ["CAD", "CAD", "CAD"], "a_registered[]": ["", "", ""],
                                        "next": "holdings"})
    assert r.headers["Location"].endswith("/setup/holdings")
    with app.app_context():
        accs = {a.name: a for a in Account.query.filter_by(portfolio_id=ids["p"])}
        assert accs["TFSA"].is_registered and not accs["Joint"].is_registered
        joint, tfsa = accs["Joint"].id, accs["TFSA"].id
    # Alphabetical order: Joint first, then TFSA.
    assert c.get("/setup/holdings").headers["Location"].endswith(f"/setup/holdings/{joint}")
    assert c.get(f"/setup/holdings/{joint}").status_code == 200
    r = c.post(f"/setup/holdings/{joint}", data={"cash_balance": "250", "next": "next",
                                                 **_rows(ticker=["XEQT"], quantity=["10"], book_value=["300"],
                                                         price=["31"], asset_class=["Equity"])})
    assert r.headers["Location"].endswith(f"/setup/holdings/{tfsa}")
    # A bad row re-renders the page with what was typed (no redirect).
    r = c.post(f"/setup/holdings/{tfsa}", data={"next": "next", **_rows(ticker=["ZZZ"], quantity=["1"])})
    assert r.status_code == 200 and b'value="ZZZ"' in r.data
    r = c.post(f"/setup/holdings/{tfsa}", data={"next": "next", **_rows(ticker=["XEQT", "ZAG"], quantity=["5", "20"],
                                                                         price=["", "14"], asset_class=["", "Bonds"])})
    assert r.headers["Location"].endswith("/setup/targets")
    with app.app_context():
        assert db.session.get(Account, joint).cash_balance == 250
        assert Holding.query.join(Account).filter(Account.portfolio_id == ids["p"]).count() == 3
    page = c.get("/setup/targets").get_data(as_text=True)
    assert "Equity" in page and "Bonds" in page
    with app.app_context():
        from models import AssetClass
        acs = {a.name: a.id for a in AssetClass.query.filter_by(portfolio_id=ids["p"])}
    r = c.post("/setup/targets", data={f"t_{acs['Equity']}": "70", f"t_{acs['Bonds']}": "20",
                                       "new_ac_name[]": ["Gold"], "new_ac_pct[]": ["5"]})
    assert r.headers["Location"].endswith("/setup/targets")        # 95% → stay and warn
    r = c.post("/setup/targets", data={f"t_{acs['Equity']}": "75", f"t_{acs['Bonds']}": "20",
                                       "new_ac_name[]": ["gold"], "new_ac_pct[]": ["5"]})
    assert r.headers["Location"].endswith("/setup/done")
    with app.app_context():
        ts = Target.query.filter_by(portfolio_id=ids["p"]).all()
        assert sorted(t.target_percentage for t in ts) == [5, 20, 75]
    assert c.get("/setup").headers["Location"].endswith("/setup/done")
    assert b"Ready to rebalance" in c.get("/setup/done").data
    assert c.get("/dashboard").status_code == 200


def test_lookup_api_and_isolation(fresh_user):
    app, db, c, ids = fresh_user
    from models import Account, AssetClass, Portfolio, Security
    with app.app_context():
        other = Portfolio(user_id=ids["user"], name="Spouse")
        db.session.add(other); db.session.flush()
        ac = AssetClass(portfolio_id=other.id, name="Canadian Equity")
        db.session.add(ac); db.session.flush()
        db.session.add(Security(portfolio_id=other.id, ticker="XIC", name="iShares TSX", asset_class_id=ac.id,
                                currency="CAD", last_price=40.0))
        sp_acct = Account(user_id=ids["user"], portfolio_id=other.id, name="Spouse RRSP", is_registered=True)
        db.session.add(sp_acct); db.session.commit(); sp_acct_id = sp_acct.id
    j = c.get("/api/securities/lookup?ticker=xic.to").get_json()
    assert j["found"] == "elsewhere" and j["asset_class"] == "Canadian Equity" and j["price"] == 40.0
    assert c.get("/api/securities/lookup?ticker=QQQ&currency=USD").get_json() == {"found": None, "currency": "USD"}
    # Can't add holdings to, or wizard-edit, the other portfolio's account.
    data = _rows(ticker=["XIC"], quantity=["1"], asset_class=["Equity"]); data["account_id"] = sp_acct_id
    assert c.post("/holdings/add", data=data).status_code == 404
    assert c.get(f"/setup/holdings/{sp_acct_id}").status_code == 404
    assert c.post(f"/accounts/{sp_acct_id}/edit", data={"name": "x"}).status_code == 404
