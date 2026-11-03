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
        assert old_in.replaced_in == {ids["tfsa"]: ids["new"], ids["nr"]: ids["new"]}
        assert old_in.sl_group == ids["old"]
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


def _add_alt(app, db, ids, restrict_to=None):
    """A second replacement candidate in Equity, optionally restricted to some accounts."""
    from models import Security, SecurityPreference
    with app.app_context():
        alt = Security(portfolio_id=ids["p"], ticker="ALT", asset_class_id=ids["eq"], currency="CAD", last_price=50.0)
        db.session.add(alt); db.session.flush()
        if restrict_to:
            db.session.add(SecurityPreference(user_id=ids["user"], portfolio_id=ids["p"], security_id=alt.id,
                                              restriction_type="restricted_to_accounts",
                                              account_config={"allowed": restrict_to}))
        db.session.commit()
        return alt.id


def test_per_account_replacements_form_and_plan(mig):
    app, db, c, ids = mig
    from models import Security, SecurityReplacement
    alt = _add_alt(app, db, ids, restrict_to=[ids["tfsa"]])
    # Default NEW, but ALT in the TFSA; the taxable account keeps OLD.
    _edit(c, ids["old"], "OLD", "Equity", replaced_by_id=str(ids["new"]), migrate_registered_now="on",
          migration_sl_link="on", **{f"acct_repl_{ids['tfsa']}": str(alt), f"acct_repl_{ids['nr']}": "keep"})
    with app.app_context():
        o = db.session.get(Security, ids["old"])
        assert o.replaced_by_id == ids["new"]
        assert {r.account_id: r.replacement_id for r in o.account_replacements} == {ids["tfsa"]: alt, ids["nr"]: None}
        assert o.replacement_for(ids["tfsa"]).id == alt and o.replacement_for(ids["nr"]) is None
        assert {s.ticker for s in o.replacement_securities} == {"NEW", "ALT"}
    page = c.get(f"/securities/{ids['old']}/edit").get_data(as_text=True)
    assert "Different replacement in some accounts (2 set)" in page
    assert "phasing out → NEW, ALT" in c.get("/securities").get_data(as_text=True)

    c.post("/rebalance/generate")
    plan = _plan(app, ids)
    assert (ids["tfsa"], "SELL", ids["old"], 200) in plan
    assert any(a == ids["tfsa"] and act == "BUY" and s == alt for a, act, s, _ in plan)
    assert not [t for t in plan if t[2] == ids["new"]]
    assert not [t for t in plan if t[0] == ids["nr"]]
    with app.app_context():
        from models import Portfolio
        from rebalancer import build_problem
        from services.fx import get_exchange_rates
        p = db.session.get(Portfolio, ids["p"])
        prob, _ = build_problem(p, get_exchange_rates(p), allow_taxable_sells=True)
        old_in = next(s for s in prob.securities if s.id == ids["old"])
        assert old_in.replaced_in == {ids["tfsa"]: alt}               # Joint keeps OLD
        # Only replacements actually in use are linked (NEW is the default but no account uses it).
        assert {s.id for s in prob.securities if s.sl_group == ids["old"]} == {ids["old"], alt}

    # Choosing the default again for an account drops its override; "keep" with no default is a no-op.
    _edit(c, ids["old"], "OLD", "Equity", replaced_by_id="", **{f"acct_repl_{ids['tfsa']}": str(alt),
                                                                f"acct_repl_{ids['nr']}": "keep"})
    with app.app_context():
        o = db.session.get(Security, ids["old"])
        assert o.replaced_by_id is None
        assert {r.account_id: r.replacement_id for r in o.account_replacements} == {ids["tfsa"]: alt}
        assert o.is_phasing_out and o.migration_sl_link is False and not o.migrate_registered_now

    # Deleting ALT: the TFSA keeps OLD rather than being switched to something else.
    c.post(f"/securities/{alt}/delete")
    with app.app_context():
        o = db.session.get(Security, ids["old"])
        assert [(r.account_id, r.replacement_id) for r in o.account_replacements] == [(ids["tfsa"], None)]
        assert not o.is_phasing_out
    # Deleting the account removes its override.
    c.post(f"/accounts/{ids['tfsa']}/delete")
    with app.app_context():
        assert SecurityReplacement.query.filter_by(account_id=ids["tfsa"]).count() == 0


def test_restricted_replacement_pauses_that_account(mig):
    app, db, c, ids = mig
    alt = _add_alt(app, db, ids, restrict_to=[ids["nr"]])          # ALT not allowed in the TFSA
    r = _edit(c, ids["old"], "OLD", "Equity", replaced_by_id=str(alt), migrate_registered_now="on")
    page = c.get("/securities").get_data(as_text=True)
    assert "restricted from TFSA" in page                            # warned on save
    c.post("/rebalance/generate")
    assert not [t for t in _plan(app, ids) if t[0] == ids["tfsa"]]   # TFSA paused, nothing forced
    assert "Phasing out OLD is paused in TFSA" in c.get("/rebalance").get_data(as_text=True)


def test_replacement_cannot_itself_be_phased_out(mig):
    app, db, c, ids = mig
    from models import Security
    alt = _add_alt(app, db, ids)
    _edit(c, ids["old"], "OLD", "Equity", **{f"acct_repl_{ids['tfsa']}": str(ids["new"])})
    _edit(c, ids["new"], "NEW", "Equity", replaced_by_id=str(alt))          # NEW replaces OLD in the TFSA
    _edit(c, alt, "ALT", "Equity", **{f"acct_repl_{ids['nr']}": str(ids["old"])})   # OLD is phasing out
    with app.app_context():
        assert db.session.get(Security, ids["new"]).replaced_by_id is None
        assert not db.session.get(Security, alt).account_replacements


def _pref(app, db, ids, sec_id, rtype, cfg):
    from models import SecurityPreference
    with app.app_context():
        db.session.add(SecurityPreference(user_id=ids["user"], portfolio_id=ids["p"], security_id=sec_id,
                                          restriction_type=rtype, account_config=cfg))
        db.session.commit()


def test_automatic_replacement_follows_account_preferences(mig):
    """The example: B is restricted to the registered account, C is prioritized for the
    non-registered one → OLD is replaced by B in the TFSA and by C in Joint."""
    app, db, c, ids = mig
    from models import Portfolio, Security
    from rebalancer import build_problem
    from services.fx import get_exchange_rates
    with app.app_context():
        b = Security(portfolio_id=ids["p"], ticker="BBB", asset_class_id=ids["eq"], currency="CAD", last_price=20.0)
        cc = Security(portfolio_id=ids["p"], ticker="CCC", asset_class_id=ids["eq"], currency="CAD", last_price=25.0)
        db.session.add_all([b, cc]); db.session.commit()
        b_id, c_id = b.id, cc.id
    _pref(app, db, ids, b_id, "restricted_to_accounts", {"allowed": [ids["tfsa"]]})
    _pref(app, db, ids, c_id, "prioritized_accounts", {"priority_1": [ids["nr"]]})
    # NEW (no preference) would be allowed everywhere at level 0 – but BBB names the TFSA, so it wins there;
    # in Joint, CCC (P1) and NEW (no preference) are both level 0 and CCC names Joint.
    page = c.get(f"/securities/{ids['old']}/edit").get_data(as_text=True)
    assert "By account preferences (automatic)" in page and "BBB" in page and "CCC" in page
    _edit(c, ids["old"], "OLD", "Equity", replaced_by_id="auto", migrate_registered_now="on", migration_sl_link="on")
    with app.app_context():
        o = db.session.get(Security, ids["old"])
        assert o.phase_out_auto and o.replaced_by_id is None and o.is_phasing_out
        p = db.session.get(Portfolio, ids["p"])
        prob, notes = build_problem(p, get_exchange_rates(p), allow_taxable_sells=True)
        old_in = next(s for s in prob.securities if s.id == ids["old"])
        assert old_in.replaced_in == {ids["tfsa"]: b_id, ids["nr"]: c_id}
        assert {s.id for s in prob.securities if s.sl_group == ids["old"]} == {ids["old"], b_id, c_id}
    assert "phasing out → BBB, CCC" in c.get("/securities").get_data(as_text=True)
    c.post("/rebalance/generate")
    plan = _plan(app, ids)
    assert (ids["tfsa"], "SELL", ids["old"], 200) in plan
    assert any(a == ids["tfsa"] and act == "BUY" and s == b_id for a, act, s, _ in plan)
    assert not [t for t in plan if t[0] == ids["nr"]]                  # taxable: gradual
    # An exception still wins over automatic: keep OLD in Joint.
    _edit(c, ids["old"], "OLD", "Equity", replaced_by_id="auto", **{f"acct_repl_{ids['nr']}": "keep"})
    with app.app_context():
        p = db.session.get(Portfolio, ids["p"])
        prob, _ = build_problem(p, get_exchange_rates(p), allow_taxable_sells=True)
        assert next(s for s in prob.securities if s.id == ids["old"]).replaced_in == {ids["tfsa"]: b_id}


def test_automatic_with_nothing_allowed_pauses_and_warns(mig):
    app, db, c, ids = mig
    from models import Security
    # NEW is the only other Equity security and it's restricted to Joint.
    _pref(app, db, ids, ids["new"], "restricted_to_accounts", {"allowed": [ids["nr"]]})
    _edit(c, ids["old"], "OLD", "Equity", replaced_by_id="auto")
    page = c.get("/securities").get_data(as_text=True)
    assert "be phased out in TFSA" in page and "no other Equity security" in page
    c.post("/rebalance/generate")
    assert "Phasing out OLD is paused in TFSA" in c.get("/rebalance").get_data(as_text=True)
