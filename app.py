import os
import json
import logging
from urllib.parse import urlparse
from flask import Flask, render_template, redirect, url_for, request, jsonify, flash, session, g, abort
from flask_login import LoginManager, login_required, current_user
from extensions import db
from datetime import datetime
from collections import defaultdict
from sqlalchemy import inspect

log = logging.getLogger(__name__)

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_secret = os.getenv('SECRET_KEY')
if not _secret:
    raise RuntimeError("SECRET_KEY environment variable must be set before starting.")

app.config['SECRET_KEY'] = _secret
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///data/portfolio.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SESSION_COOKIE_SECURE'] = os.getenv('FLASK_ENV') == 'production'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

db.init_app(app)

from models import User, Portfolio, Account, Holding, Security, SecurityPreference
from models import Target, AssetClass, ExchangeRate, RebalanceTransaction, PendingRedemption
import migrations
from auth import auth_bp, init_oauth
from services.fx import get_exchange_rates
from services.portfolio import (
    calculate_portfolio_allocation,
    calculate_asset_class_deltas,
)
from services.prices import (
    PriceSheetError, fetch_prices_from_user_sheet, match_prices_across_portfolios, set_security_price,
    same_security_elsewhere,
)
from rebalancer import generate_plan
from services.fx import convert_to_base

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# ---------------------------------------------------------------------------
# Flask-Login
# ---------------------------------------------------------------------------
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Please log in to access this page.'
login_manager.login_message_category = 'info'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

init_oauth(app)
app.register_blueprint(auth_bp, url_prefix='/auth')

# ---------------------------------------------------------------------------
# Active portfolio
# ---------------------------------------------------------------------------
# One login can manage several fully separate portfolios (e.g. yours and your
# spouse's). Every page works on the "active" one, kept in the session. All data
# access goes through current_portfolio(), and anything addressed by id is checked
# against it with _require(), so one portfolio's rows can never leak into another.
def current_portfolio():
    if 'portfolio' in g:
        return g.portfolio
    p = None
    pid = session.get('portfolio_id')
    if pid:
        p = Portfolio.query.filter_by(id=pid, user_id=current_user.id).first()
    if p is None:
        p = Portfolio.query.filter_by(user_id=current_user.id).order_by(Portfolio.id).first()
    if p is None:                                   # brand-new login
        p = migrations.new_default_portfolio(current_user)
        db.session.add(p)
        db.session.commit()
    session['portfolio_id'] = p.id
    g.portfolio = p
    return p


def _require(obj):
    """404 unless *obj* exists and belongs to the active portfolio."""
    if obj is None:
        abort(404)
    pid = obj.account.portfolio_id if isinstance(obj, Holding) else obj.portfolio_id
    if pid != current_portfolio().id:
        abort(404)
    return obj


def _get(model, obj_id):
    try:
        obj = db.session.get(model, int(obj_id))
    except (TypeError, ValueError):
        obj = None
    return _require(obj)


def _p_accounts():
    return Account.query.filter_by(portfolio_id=current_portfolio().id).order_by(Account.name).all()


def _p_asset_classes():
    return AssetClass.query.filter_by(portfolio_id=current_portfolio().id).order_by(AssetClass.name.asc()).all()


def _p_securities():
    return Security.query.filter_by(portfolio_id=current_portfolio().id).order_by(Security.ticker.asc()).all()


@app.context_processor
def _inject_portfolios():
    if not current_user.is_authenticated:
        return {}
    return {'active_portfolio': current_portfolio(),
            'all_portfolios': Portfolio.query.filter_by(user_id=current_user.id).order_by(Portfolio.id).all()}


# ---------------------------------------------------------------------------
# Routes – General
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return render_template('index.html')


@app.route('/dashboard')
@login_required
def dashboard():
    P = current_portfolio()
    accounts = _p_accounts()
    targets = Target.query.filter_by(portfolio_id=P.id).all()
    exchange_rates = get_exchange_rates(P)
    allocation, allocation_pct, total_value = calculate_portfolio_allocation(P, exchange_rates)
    return render_template(
        'dashboard.html',
        user=current_user,
        accounts=accounts,
        targets=targets,
        total_value=total_value,
        allocation=allocation,
        allocation_pct=allocation_pct,
        exchange_rates=exchange_rates,
        base_currency=P.base_currency,
    )


# ---------------------------------------------------------------------------
# Routes – Accounts
# ---------------------------------------------------------------------------
@app.route('/accounts')
@login_required
def accounts():
    return render_template('accounts.html', accounts=_p_accounts())


@app.route('/accounts/add', methods=['POST'])
@login_required
def add_account():
    name = request.form.get('name', '').strip()
    if not name:
        flash('Account name is required', 'error')
        return redirect(url_for('accounts'))
    account = Account(
        user_id=current_user.id,
        portfolio_id=current_portfolio().id,
        name=name,
        account_type=request.form.get('account_type'),
        currency=request.form.get('currency', 'CAD'),
        is_registered=request.form.get('is_registered') == 'on',
    )
    db.session.add(account)
    db.session.commit()
    flash(f'Account "{name}" added successfully', 'success')
    return redirect(url_for('accounts'))


@app.route('/accounts/<int:account_id>/delete', methods=['POST'])
@login_required
def delete_account(account_id):
    account = _get(Account, account_id)
    # No FK cascade from transactions to accounts – remove them explicitly.
    RebalanceTransaction.query.filter_by(account_id=account.id).delete()
    PendingRedemption.query.filter_by(account_id=account.id).delete()
    db.session.delete(account)
    db.session.commit()
    flash(f'Account "{account.name}" deleted successfully', 'success')
    return redirect(url_for('accounts'))


@app.route('/accounts/<int:account_id>/cash', methods=['POST'])
@login_required
def update_cash_balance(account_id):
    account = _get(Account, account_id)
    cash_balance = request.form.get('cash_balance')
    if cash_balance:
        account.cash_balance = float(cash_balance)
        db.session.commit()
        flash(f'Updated cash balance for {account.name}', 'success')
    return redirect(url_for('holdings'))


# ---------------------------------------------------------------------------
# Routes – Holdings
# ---------------------------------------------------------------------------
@app.route('/holdings')
@login_required
def holdings():
    P = current_portfolio()
    exchange_rates = get_exchange_rates(P)
    all_holdings = (
        db.session.query(Holding)
        .join(Account)
        .outerjoin(Security)
        .filter(Account.portfolio_id == P.id)
        .order_by(Account.name.asc(), Security.ticker.asc())
        .all()
    )
    return render_template(
        'holdings.html',
        holdings=all_holdings,
        accounts=_p_accounts(),
        exchange_rates=exchange_rates,
        base_currency=P.base_currency,
        asset_classes=_p_asset_classes(),
        securities=_p_securities(),
    )


@app.route('/holdings/add', methods=['POST'])
@login_required
def add_holding():
    account_id = request.form.get('account_id')
    security_id = request.form.get('security_id')
    quantity = request.form.get('quantity')
    price = request.form.get('price')
    if not all([account_id, security_id, quantity]):
        flash('Account, security, and quantity are required', 'error')
        return redirect(url_for('holdings'))
    account = _get(Account, account_id)
    security = _get(Security, security_id)
    book_value = request.form.get('book_value')
    if price:
        set_security_price(security, float(price))     # a typed price becomes the security's price
    holding = Holding(
        account_id=account.id,
        security_id=security.id,
        quantity=float(quantity),
        price=security.last_price or 0.0,
        book_value=float(book_value) if book_value else None,
    )
    db.session.add(holding)
    db.session.commit()
    if security.last_price:
        flash(f'Holding {security.ticker} added successfully', 'success')
    else:
        flash(f'Holding {security.ticker} added, but it has no price yet – run "Update Prices" '
              f'or set a price on the security.', 'warning')
    return redirect(url_for('holdings'))


@app.route('/holdings/<int:holding_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_holding(holding_id):
    holding = _get(Holding, holding_id)
    if request.method == 'POST':
        holding.account_id = _get(Account, request.form.get('account_id')).id
        holding.quantity = float(request.form.get('quantity', 0))
        sec = _get(Security, request.form.get('security_id'))
        holding.security_id = sec.id
        new_price = float(request.form.get('price') or 0)
        if new_price > 0 and new_price != sec.last_price:
            set_security_price(sec, new_price)           # one price per security
        holding.price = sec.last_price or new_price
        holding.notes = request.form.get('notes')
        book_value = request.form.get('book_value', '').strip()
        holding.book_value = float(book_value) if book_value else None
        db.session.commit()
        flash('Holding updated successfully.', 'success')
        return redirect(url_for('holdings'))
    return render_template(
        'edit_holding.html',
        holding=holding,
        accounts=_p_accounts(),
        securities=_p_securities(),
        asset_classes=_p_asset_classes(),
    )


@app.route('/holdings/<int:holding_id>/delete', methods=['POST'])
@login_required
def delete_holding(holding_id):
    holding = _get(Holding, holding_id)
    ticker = holding.security.ticker
    db.session.delete(holding)
    db.session.commit()
    flash(f'Holding {ticker} deleted successfully', 'success')
    return redirect(url_for('holdings'))


@app.route('/update_prices', methods=['POST'])
@login_required
def update_prices():
    try:
        prices = fetch_prices_from_user_sheet(current_user)
    except PriceSheetError as exc:
        flash(f'Price update failed: {exc}', 'error')
        return redirect(url_for('holdings'))
    # The price sheet belongs to the login: one pass fills every portfolio.
    candidates = (Security.query.join(Portfolio, Security.portfolio_id == Portfolio.id)
                  .filter(Portfolio.user_id == current_user.id, Security.auto_update_price.is_(True)).all())
    matched, unmatched = match_prices_across_portfolios(prices, candidates)
    by_id = {s.id: s for s in candidates}
    for sec_id, price in matched.items():
        set_security_price(by_id[sec_id], price, propagate=False)
    db.session.commit()
    n_portfolios = len({s.portfolio_id for s in candidates})
    flash(f'Updated prices for {len({by_id[i].ticker for i in matched})} securities'
          + (f' across {n_portfolios} portfolios.' if n_portfolios > 1 else '.'), 'success')
    missing = sorted({s.ticker for s in candidates if s.id not in matched})
    if missing:
        flash('No price in the sheet for: ' + ', '.join(missing), 'warning')
    if unmatched:
        flash('Sheet tickers not matching any security: ' + ', '.join(sorted(unmatched)[:15]), 'warning')
    return redirect(url_for('holdings'))


# ---------------------------------------------------------------------------
# Routes – Targets
# ---------------------------------------------------------------------------
@app.route('/targets')
@login_required
def targets():
    return render_template(
        'targets.html',
        targets=Target.query.filter_by(portfolio_id=current_portfolio().id).all(),
        accounts=_p_accounts(),
        asset_classes=_p_asset_classes(),
    )


@app.route('/targets/update', methods=['POST'])
@login_required
def update_targets():
    P = current_portfolio()
    action = request.form.get('action')
    if action == 'add':
        asset_class_id = request.form.get('asset_class_id')
        if asset_class_id == 'new':
            name = request.form.get('new_asset_class_name', '').strip()
            if not name:
                flash('Asset class name is required', 'danger')
                return redirect(url_for('targets'))
            ac = AssetClass.query.filter_by(portfolio_id=P.id, name=name).first()
            if ac is None:
                ac = AssetClass(portfolio_id=P.id, name=name)
                db.session.add(ac)
                db.session.flush()
            asset_class_id = ac.id
        else:
            asset_class_id = _get(AssetClass, asset_class_id).id
        db.session.add(Target(
            user_id=current_user.id,
            portfolio_id=P.id,
            asset_class_id=asset_class_id,
            target_percentage=float(request.form.get('percentage', 0)),
        ))
        db.session.commit()
        flash('Target added successfully!', 'success')
    elif action == 'update':
        target = _get(Target, request.form.get('target_id'))
        target.asset_class_id = _get(AssetClass, request.form.get('asset_class_id')).id
        target.target_percentage = float(request.form.get('percentage', 0))
        db.session.commit()
        flash('Target updated successfully!', 'success')
    elif action == 'delete':
        target = _get(Target, request.form.get('target_id'))
        db.session.delete(target)
        db.session.commit()
        flash('Target deleted successfully!', 'success')
    return redirect(url_for('targets'))


# ---------------------------------------------------------------------------
# Routes – Securities
# ---------------------------------------------------------------------------
@app.route('/securities')
@login_required
def securities():
    P = current_portfolio()
    all_securities = (
        db.session.query(Security)
        .outerjoin(AssetClass)
        .filter(Security.portfolio_id == P.id)
        .order_by(AssetClass.name.asc(), Security.ticker.asc())
        .all()
    )
    prefs = SecurityPreference.query.filter_by(portfolio_id=P.id).all()
    return render_template(
        'securities.html',
        securities=all_securities,
        preferences={p.security_id: p for p in prefs},
        accounts=_p_accounts(),
        restriction_types=[
            ('unrestricted', 'Unrestricted - Can be held in any account'),
            ('restricted_to_accounts', 'Restricted - Only specific accounts allowed'),
            ('prioritized_accounts', 'Prioritized - Prefer certain accounts over others'),
        ],
        asset_classes=_p_asset_classes(),
    )


@app.route('/securities/add', methods=['GET', 'POST'])
@login_required
def add_security():
    P = current_portfolio()
    if request.method == 'POST':
        ticker = request.form.get('ticker', '').strip()
        asset_class_id = request.form.get('asset_class_id')
        if not ticker:
            flash('Ticker is required', 'error')
        elif not asset_class_id:
            flash('Asset class is required', 'error')
        elif _ticker_taken(P.id, ticker):
            flash(f'{ticker} is already in this portfolio.', 'error')
        else:
            sec = Security(
                portfolio_id=P.id,
                ticker=ticker,
                name=request.form.get('name'),
                asset_class_id=_get(AssetClass, asset_class_id).id,
                currency=request.form.get('currency', 'CAD'),
                is_public=request.form.get('is_public') == 'on',
                auto_update_price=request.form.get('auto_update_price') == 'on',
                is_illiquid=request.form.get('is_illiquid') == 'on',
            )
            db.session.add(sec)
            db.session.flush()
            if request.form.get('price'):
                set_security_price(sec, float(request.form['price']))
            else:   # same listing already priced in another of your portfolios → reuse it
                sec.last_price = next((o.last_price for o in same_security_elsewhere(sec) if o.last_price), None)
            db.session.commit()
            flash(f'Security {ticker} added.', 'success')
            return redirect(url_for('securities'))
    return render_template('add_security.html', asset_classes=_p_asset_classes())


def _ticker_taken(portfolio_id, ticker, exclude_id=None):
    q = Security.query.filter(Security.portfolio_id == portfolio_id,
                              db.func.upper(Security.ticker) == ticker.strip().upper())
    if exclude_id:
        q = q.filter(Security.id != exclude_id)
    return db.session.query(q.exists()).scalar()


@app.route('/securities/<int:security_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_security(security_id):
    security = _get(Security, security_id)
    if request.method == 'POST':
        ticker = (request.form.get('ticker') or '').strip()
        if not ticker or _ticker_taken(security.portfolio_id, ticker, exclude_id=security.id):
            flash(f'{ticker or "Ticker"} is missing or already in this portfolio.', 'error')
            return redirect(url_for('edit_security', security_id=security.id))
        security.ticker = ticker
        security.name = request.form.get('name')
        security.asset_class_id = _get(AssetClass, request.form.get('asset_class_id')).id
        security.currency = request.form.get('currency', 'CAD')
        security.is_public = request.form.get('is_public') == 'on'
        security.auto_update_price = request.form.get('auto_update_price') == 'on'
        security.is_illiquid = request.form.get('is_illiquid') == 'on'
        if request.form.get('price'):
            set_security_price(security, float(request.form['price']))
        db.session.commit()
        flash(f'Security {security.ticker} updated successfully.', 'success')
        return redirect(url_for('securities'))
    return render_template(
        'edit_security.html',
        security=security,
        asset_classes=_p_asset_classes(),
    )


@app.route('/securities/<int:security_id>/delete', methods=['POST'])
@login_required
def delete_security(security_id):
    s = _get(Security, security_id)
    RebalanceTransaction.query.filter_by(security_id=s.id, executed=False).delete()
    PendingRedemption.query.filter_by(security_id=s.id).delete()
    db.session.delete(s)
    db.session.commit()
    flash(f'Deleted {s.ticker}.', 'success')
    return redirect(url_for('securities'))


@app.route('/securities/<int:security_id>/preference', methods=['POST'])
@login_required
def update_security_preference(security_id):
    P = current_portfolio()
    security = _get(Security, security_id)
    mine = {a.id for a in _p_accounts()}
    restriction_type = request.form.get('restriction_type')
    if not restriction_type:
        flash('Restriction type is required', 'error')
        return redirect(url_for('securities'))
    account_config = None
    if restriction_type == 'restricted_to_accounts':
        allowed = request.form.getlist('allowed_accounts[]')
        if allowed:
            account_config = {'allowed': [int(a) for a in allowed if int(a) in mine]}
    elif restriction_type == 'prioritized_accounts':
        account_config = {}
        for level in (1, 2, 3):
            ids = request.form.getlist(f'priority_{level}[]')
            if ids:
                account_config[f'priority_{level}'] = [int(i) for i in ids if int(i) in mine]
    pref = SecurityPreference.query.filter_by(
        security_id=security.id, portfolio_id=P.id
    ).first()
    if pref:
        pref.restriction_type = restriction_type
        pref.account_config = account_config
        pref.notes = request.form.get('notes')
    else:
        db.session.add(SecurityPreference(
            security_id=security.id,
            user_id=current_user.id,
            portfolio_id=P.id,
            restriction_type=restriction_type,
            account_config=account_config,
            notes=request.form.get('notes'),
        ))
    db.session.commit()
    flash(f'Preference for {security.ticker} updated successfully', 'success')
    return redirect(url_for('securities'))


# ---------------------------------------------------------------------------
# Routes – Rebalance
# ---------------------------------------------------------------------------
@app.route('/rebalance')
@login_required
def rebalance():
    P = current_portfolio()
    transactions = RebalanceTransaction.query.filter_by(
        portfolio_id=P.id, executed=False
    ).order_by(RebalanceTransaction.execution_order).all()

    # A plan is one coordinated set of trades: if any of them refers to an account or
    # security that has since been deleted, the whole plan is invalid – discard it.
    orphans = [t for t in transactions
               if t.account is None or (t.security is None and not t.requires_user_selection)]
    if orphans:
        _discard_pending_plan()
        log.warning('Discarded plan for portfolio %s: %d trade(s) referred to deleted accounts/securities',
                    P.id, len(orphans))
        flash('Your plan referred to an account or security that has since been deleted, so it was '
              'discarded. Click "Generate Rebalance Plan" for a new one.', 'warning')
        transactions = []

    # Group by account and calculate totals
    from collections import defaultdict
    grouped = defaultdict(lambda: {'account': None, 'sells': [], 'buys': [], 'total_buy': 0, 'total_sell': 0})

    for txn in transactions:
        acc_name = txn.account.name
        grouped[acc_name]['account'] = txn.account
        if txn.action == 'SELL':
            grouped[acc_name]['sells'].append(txn)
            grouped[acc_name]['total_sell'] += txn.amount
        else:
            grouped[acc_name]['buys'].append(txn)
            grouped[acc_name]['total_buy'] += txn.amount

    # Sort by total buy amount descending
    sorted_groups = sorted(grouped.items(), key=lambda x: x[1]['total_buy'], reverse=True)

    exchange_rates = get_exchange_rates(P)
    allocation_by_id, allocation_pct_by_id, total_portfolio = calculate_portfolio_allocation(
        P, exchange_rates
    )
    targets = Target.query.filter_by(portfolio_id=P.id).all()
    comparison_data = sorted([
        {
            'asset_class_name': t.asset_class.name,
            'current_value': allocation_by_id.get(t.asset_class_id, 0),
            'current_pct': allocation_pct_by_id.get(t.asset_class_id, 0),
            'target_pct': t.target_percentage,
            'difference': allocation_pct_by_id.get(t.asset_class_id, 0) - t.target_percentage,
        }
        for t in targets
    ], key=lambda x: x['current_pct'], reverse=True)
    return render_template(
        'rebalance.html',
        transactions=transactions,
        securities=_p_securities(),
        comparison_data=comparison_data,
        total_portfolio=total_portfolio,
        base_currency=P.base_currency,
        accounts=P.accounts,
        balanced_threshold=P.balanced_threshold,
        plan=_plan_summary(),
        pending_redemptions=PendingRedemption.query.filter_by(
            portfolio_id=P.id, status='pending').order_by(PendingRedemption.requested_on).all(),
        today=datetime.utcnow().date(),
    )


@app.route('/rebalance/details/<int:asset_class_id>')
@login_required
def rebalance_details(asset_class_id):
    P = current_portfolio()
    asset_class = _get(AssetClass, asset_class_id)
    all_securities = Security.query.filter_by(portfolio_id=P.id, asset_class_id=asset_class.id).all()
    prefs = {
        p.security_id: p
        for p in SecurityPreference.query.filter_by(portfolio_id=P.id).all()
    }
    user_accounts = _p_accounts()
    security_restrictions = []
    for security in all_securities:
        pref = prefs.get(security.id)
        rtype = pref.restriction_type if pref else 'unrestricted'
        if rtype == 'restricted_to_accounts':
            allowed_ids = (pref.account_config or {}).get('allowed', [])
            allowed_accounts = [a for a in user_accounts if a.id in allowed_ids]
            priority_accounts = []
        elif rtype == 'prioritized_accounts':
            allowed_accounts = user_accounts
            cfg = pref.account_config or {}
            priority_accounts = [
                {'level': lvl, 'accounts': [a for a in user_accounts if a.id in cfg.get(f'priority_{lvl}', [])]}
                for lvl in (1, 2, 3)
            ]
        else:
            allowed_accounts = user_accounts
            priority_accounts = []
        security_restrictions.append({
            'security': security,
            'restriction_type': rtype,
            'allowed_accounts': allowed_accounts,
            'priority_accounts': priority_accounts,
            'notes': pref.notes if pref else None,
        })
    return render_template(
        'rebalance_details.html',
        asset_class=asset_class,
        security_restrictions=security_restrictions,
        base_currency=P.base_currency,
    )


def _discard_pending_plan():
    P = current_portfolio()
    RebalanceTransaction.query.filter_by(portfolio_id=P.id, executed=False).delete()
    P.last_plan_summary = None
    db.session.commit()


def _plan_summary():
    P = current_portfolio()
    try:
        return json.loads(P.last_plan_summary) if P.last_plan_summary else None
    except ValueError:
        return None


def _regenerate_plan(override=None):
    """Run the optimiser and remember the summary (and any taxable-sell opt-in)."""
    P = current_portfolio()
    txns, summary = generate_plan(P, taxable_override=override)
    summary['override'] = override
    P.last_plan_summary = json.dumps(summary)
    db.session.commit()
    return txns


@app.route('/rebalance/generate', methods=['POST'])
@login_required
def generate_rebalance():
    # 'allow' comes from the "include taxable sells" button on the Ask-mode offer
    override = request.form.get('taxable') if request.form.get('taxable') in ('allow', 'never') else None
    try:
        txns = _regenerate_plan(override)
        flash(f'Generated {len(txns)} rebalance transactions', 'success')
    except Exception as exc:
        log.exception('Error generating rebalance plan')
        flash(f'Error generating rebalance plan: {exc}', 'error')
    return redirect(url_for('rebalance'))


# ---------------------------------------------------------------------------
# Routes – Pending redemptions (illiquid holdings)
# ---------------------------------------------------------------------------
def _regenerate_after_change(message):
    try:
        prev = _plan_summary() or {}
        _regenerate_plan(prev.get('override'))
        flash(message, 'success')
    except Exception as exc:
        log.exception('Plan regeneration failed: %s', exc)
        db.session.rollback()
        _discard_pending_plan()
        flash(message + ' The plan could not be regenerated – click "Generate Rebalance Plan".', 'warning')


def _parse_date(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d').date() if value else None
    except ValueError:
        return None


@app.route('/redemptions/add', methods=['POST'])
@login_required
def add_redemption():
    account = _get(Account, request.form.get('account_id'))
    security_id = _get(Security, request.form.get('security_id')).id
    holding = Holding.query.filter_by(account_id=account.id, security_id=security_id).first()
    try:
        units = float(request.form.get('quantity') or 0)
    except ValueError:
        units = 0
    already = sum(r.quantity for r in PendingRedemption.query.filter_by(
        account_id=account.id, security_id=security_id, status='pending'))
    if not holding or units <= 0 or units > holding.quantity - already + 1e-6:
        flash('Redemption quantity must be more than 0 and no more than the units not already being redeemed.',
              'error')
        return redirect(url_for('rebalance'))
    db.session.add(PendingRedemption(
        user_id=current_user.id, portfolio_id=account.portfolio_id, account_id=account.id, security_id=security_id, quantity=units,
        requested_on=_parse_date(request.form.get('requested_on')) or datetime.utcnow().date(),
        expected_on=_parse_date(request.form.get('expected_on')),
    ))
    db.session.commit()
    _regenerate_after_change(f'Recorded redemption request: {units:g} units of {holding.security.ticker} '
                             f'in {account.name}. Those units are now left out of the plan.')
    return redirect(url_for('rebalance'))


@app.route('/redemptions/<int:redemption_id>/receive', methods=['POST'])
@login_required
def receive_redemption(redemption_id):
    r = _get(PendingRedemption, redemption_id)
    if r.status != 'pending':
        flash('Redemption not found', 'error')
        return redirect(url_for('rebalance'))
    account, sec = r.account, r.security
    holding = Holding.query.filter_by(account_id=r.account_id, security_id=r.security_id).first()
    if not holding or holding.quantity < r.quantity - 1e-6:
        flash(f'You no longer hold {r.quantity:g} units of {sec.ticker} in {account.name} – '
              'update the holding first.', 'error')
        return redirect(url_for('rebalance'))
    try:
        proceeds = float(request.form.get('proceeds') or 0) or r.quantity * (sec.last_price or holding.price)
    except ValueError:
        flash('Proceeds must be a number', 'error')
        return redirect(url_for('rebalance'))
    P = current_portfolio()
    fx = get_exchange_rates(P)
    base = P.base_currency or 'CAD'
    # Book value leaves with the units (average cost); taxable accounts record the gain.
    gain_base = None
    if holding.book_value is not None and holding.quantity > 0:
        acb = holding.book_value * r.quantity / holding.quantity
        holding.book_value -= acb
        if not account.is_registered:
            gain_base = convert_to_base(proceeds - acb, sec.currency, base, fx)
    holding.quantity -= r.quantity
    if holding.quantity <= 1e-9:
        db.session.delete(holding)
    account.cash_balance = (account.cash_balance or 0) + convert_to_base(
        proceeds, sec.currency, account.currency or base, fx)
    # Keep a record like any other sale (also feeds the superficial-loss check).
    db.session.add(RebalanceTransaction(
        user_id=current_user.id, portfolio_id=P.id, account_id=account.id, security_id=sec.id, action='SELL',
        quantity=r.quantity, price=proceeds / r.quantity, amount=proceeds, currency=sec.currency,
        executed=True, executed_at=datetime.utcnow(), realized_gain=gain_base, execution_order=0,
    ))
    r.status, r.received_on, r.proceeds = 'received', datetime.utcnow().date(), proceeds
    db.session.commit()
    _regenerate_after_change(f'Redemption of {sec.ticker} received: {proceeds:,.2f} {sec.currency} '
                             f'added to {account.name} cash.')
    return redirect(url_for('rebalance'))


@app.route('/redemptions/<int:redemption_id>/cancel', methods=['POST'])
@login_required
def cancel_redemption(redemption_id):
    r = _get(PendingRedemption, redemption_id)
    if r.status != 'pending':
        flash('Redemption not found', 'error')
        return redirect(url_for('rebalance'))
    r.status = 'cancelled'
    db.session.commit()
    _regenerate_after_change(f'Redemption request for {r.security.ticker} cancelled.')
    return redirect(url_for('rebalance'))


@app.route('/rebalance/execute/<int:transaction_id>', methods=['POST'])
@login_required
def execute_rebalance_transaction(transaction_id):
    txn = _get(RebalanceTransaction, transaction_id)
    if txn.requires_user_selection:
        security_id = request.form.get('security_id')
        if not security_id:
            flash('Please select a security', 'error')
            return redirect(url_for('rebalance'))
        txn.security_id = _get(Security, security_id).id
    try:
        account = _get(Account, txn.account_id)
        holding = Holding.query.filter_by(
            account_id=txn.account_id, security_id=txn.security_id
        ).first()
        P = current_portfolio()
        fx = get_exchange_rates(P)
        base = P.base_currency or 'CAD'
        acct_ccy = account.currency or base
        sec_ccy = txn.currency or (txn.security.currency if txn.security else acct_ccy)
        # Trade amount in the account's cash currency, at spot. Book value is kept in
        # the security's own currency; realised gains are reported in base currency.
        amount_acct = convert_to_base(txn.amount, sec_ccy, acct_ccy, fx)
        if txn.action == 'SELL':
            if not holding or holding.quantity < txn.quantity - 1e-9:
                raise ValueError(
                    f'Insufficient quantity to sell: have '
                    f'{holding.quantity if holding else 0}, need {txn.quantity}'
                )
            # Average-cost ACB: the sold fraction of book value leaves with the shares.
            if holding.book_value is not None and holding.quantity > 0:
                acb_sold = holding.book_value * (txn.quantity / holding.quantity)
                holding.book_value -= acb_sold
                if not account.is_registered:
                    txn.realized_gain = convert_to_base(txn.amount - acb_sold, sec_ccy, base, fx)
            holding.quantity -= txn.quantity
            if holding.quantity <= 1e-9:
                db.session.delete(holding)
            account.cash_balance = (account.cash_balance or 0) + amount_acct
        else:  # BUY
            if (account.cash_balance or 0) < amount_acct - 0.01:
                raise ValueError(
                    f'Insufficient cash: have {account.cash_balance:.2f} {acct_ccy}, '
                    f'need {amount_acct:.2f} {acct_ccy}'
                )
            if holding:
                holding.quantity += txn.quantity
                if holding.book_value is not None:
                    holding.book_value += txn.amount
            else:
                db.session.add(Holding(
                    account_id=txn.account_id,
                    security_id=txn.security_id,
                    quantity=txn.quantity,
                    price=txn.price,
                    book_value=txn.amount,
                ))
            account.cash_balance = (account.cash_balance or 0) - amount_acct
        txn.executed = True
        txn.executed_at = datetime.utcnow()
        db.session.commit()
        flash(f'Executed: {txn.action} {txn.quantity:.2f} of {txn.security.ticker}', 'success')

        # Regen is best-effort — don't let it mask the successful trade
        try:
            prev = _plan_summary() or {}
            _regenerate_plan(prev.get('override'))
        except Exception as regen_exc:
            # The remaining trades were planned together with the one just executed and
            # may no longer make sense on their own – don't leave half a plan on screen.
            log.exception('Post-execution regen failed: %s', regen_exc)
            db.session.rollback()
            _discard_pending_plan()
            flash('Trade executed, but a new plan could not be generated, so the rest of the old plan '
                  'was cleared. Click "Generate Rebalance Plan" to try again.', 'warning')

    except ValueError as exc:
        db.session.rollback()
        flash(str(exc), 'error')
    except Exception as exc:
        db.session.rollback()
        log.exception('Error executing transaction')
        flash(f'Error executing transaction: {exc}', 'error')
    return redirect(url_for('rebalance'))


# ---------------------------------------------------------------------------
# Routes – Settings
# ---------------------------------------------------------------------------
@app.route('/settings')
@login_required
def settings():
    P = current_portfolio()
    others = Portfolio.query.filter(Portfolio.user_id == current_user.id, Portfolio.id != P.id) \
                            .order_by(Portfolio.id).all()
    return render_template('settings.html', user=current_user, portfolio=P, other_portfolios=others)


@app.route('/settings/update', methods=['POST'])
@login_required
def update_settings():
    """Rebalancing settings belong to the active portfolio; the price sheet to the login."""
    P = current_portfolio()
    base_currency = request.form.get('base_currency')
    if base_currency not in ('CAD', 'USD'):
        flash('Invalid currency selection', 'error')
        return redirect(url_for('settings'))
    try:
        threshold = max(0.0, float(request.form.get('balanced_threshold', 0.5)))
        trade_cost = max(0.0, float(request.form.get('trade_cost') or 0))
    except ValueError:
        flash('Threshold and trade cost must be numbers', 'error')
        return redirect(url_for('settings'))
    link = request.form.get('sl_link_portfolio_id') or None
    if link is not None:
        other = Portfolio.query.filter_by(id=int(link), user_id=current_user.id).first()
        link = other.id if other is not None and other.id != P.id else None
    P.base_currency = base_currency
    P.precision_rebalancing = request.form.get('precision_rebalancing') == 'on'
    P.balanced_threshold, P.trade_cost = threshold, trade_cost
    mode = request.form.get('taxable_sell_mode', 'ask')
    P.taxable_sell_mode = mode if mode in ('never', 'ask', 'allow') else 'ask'
    P.sl_link_portfolio_id = link
    price_sheet_id = request.form.get('price_sheet_id', '').strip()
    if price_sheet_id:
        current_user.price_sheet_id = price_sheet_id
    db.session.commit()
    flash('Settings updated successfully', 'success')
    return redirect(url_for('settings'))


# ---------------------------------------------------------------------------
# Routes – Portfolios
# ---------------------------------------------------------------------------
# Pages that make sense in any portfolio; after switching we return to one of these
# (a page about a specific holding/security would 404 in the other portfolio).
_SWITCH_SAFE_PATHS = {'/dashboard', '/accounts', '/holdings', '/targets', '/securities',
                      '/rebalance', '/settings', '/portfolios'}


def _owned_portfolio(portfolio_id):
    p = Portfolio.query.filter_by(id=portfolio_id, user_id=current_user.id).first()
    if p is None:
        abort(404)
    return p


@app.route('/portfolios')
@login_required
def portfolios():
    rows = []
    fx = get_exchange_rates(None)
    for p in Portfolio.query.filter_by(user_id=current_user.id).order_by(Portfolio.id).all():
        _, _, total = calculate_portfolio_allocation(p, fx)
        rows.append({'portfolio': p, 'accounts': len(p.accounts), 'securities': len(p.securities),
                     'total': total})
    return render_template('portfolios.html', rows=rows)


@app.route('/portfolios/add', methods=['POST'])
@login_required
def add_portfolio():
    name = request.form.get('name', '').strip()
    if not name:
        flash('Portfolio name is required', 'error')
        return redirect(url_for('portfolios'))
    src = current_portfolio()
    new = Portfolio(user_id=current_user.id, name=name, base_currency=src.base_currency)
    db.session.add(new)
    db.session.flush()
    if request.form.get('copy_setup') == 'on':
        _copy_setup(src, new)
    db.session.commit()
    session['portfolio_id'] = new.id
    flash(f'Portfolio "{name}" created and selected.'
          + (' Asset classes, securities and targets were copied – it has no accounts or holdings yet.'
             if request.form.get('copy_setup') == 'on' else ''), 'success')
    return redirect(url_for('accounts'))


def _copy_setup(src, dst):
    """One-off convenience copy of asset classes, securities and targets. The copies are
    independent rows: nothing links the two portfolios afterwards (prices excepted)."""
    ac_map = {}
    for ac in src.asset_classes:
        ac_map[ac.id] = AssetClass(portfolio_id=dst.id, name=ac.name)
        db.session.add(ac_map[ac.id])
    db.session.flush()
    for sec in src.securities:
        db.session.add(Security(
            portfolio_id=dst.id, ticker=sec.ticker, name=sec.name, asset_class_id=ac_map[sec.asset_class_id].id,
            currency=sec.currency, is_public=sec.is_public, auto_update_price=sec.auto_update_price,
            is_illiquid=sec.is_illiquid, last_price=sec.last_price))
    for t in src.targets:
        db.session.add(Target(user_id=dst.user_id, portfolio_id=dst.id,
                              asset_class_id=ac_map[t.asset_class_id].id, target_percentage=t.target_percentage))


@app.route('/portfolios/<int:portfolio_id>/switch', methods=['POST'])
@login_required
def switch_portfolio(portfolio_id):
    p = _owned_portfolio(portfolio_id)
    session['portfolio_id'] = p.id
    back = urlparse(request.referrer or '').path
    return redirect(back if back in _SWITCH_SAFE_PATHS else url_for('dashboard'))


@app.route('/portfolios/<int:portfolio_id>/rename', methods=['POST'])
@login_required
def rename_portfolio(portfolio_id):
    p = _owned_portfolio(portfolio_id)
    name = request.form.get('name', '').strip()
    if name:
        p.name = name
        db.session.commit()
        flash(f'Renamed to "{name}".', 'success')
    return redirect(url_for('portfolios'))


@app.route('/portfolios/<int:portfolio_id>/delete', methods=['POST'])
@login_required
def delete_portfolio(portfolio_id):
    p = _owned_portfolio(portfolio_id)
    if Portfolio.query.filter_by(user_id=current_user.id).count() <= 1:
        flash("You can't delete your only portfolio.", 'error')
        return redirect(url_for('portfolios'))
    if request.form.get('confirm_name', '').strip() != p.name:
        flash(f'Type the portfolio name ("{p.name}") to confirm deleting it.', 'error')
        return redirect(url_for('portfolios'))
    name = p.name
    # No ORM cascade for these – remove explicitly, and unhook any superficial-loss links.
    RebalanceTransaction.query.filter_by(portfolio_id=p.id).delete()
    PendingRedemption.query.filter_by(portfolio_id=p.id).delete()
    Portfolio.query.filter_by(sl_link_portfolio_id=p.id).update({'sl_link_portfolio_id': None})
    db.session.delete(p)
    db.session.commit()
    if session.get('portfolio_id') == portfolio_id:
        session.pop('portfolio_id', None)
    g.pop('portfolio', None)
    flash(f'Portfolio "{name}" and everything in it was deleted.', 'success')
    return redirect(url_for('portfolios'))


# ---------------------------------------------------------------------------
# Routes – Exchange Rates
# ---------------------------------------------------------------------------
@app.route('/exchange-rates')
@login_required
def exchange_rates_view():
    rates = ExchangeRate.query.order_by(ExchangeRate.date.desc()).limit(50).all()
    return render_template('exchange_rates.html', rates=rates)


@app.route('/exchange-rates/update', methods=['POST'])
@login_required
def update_exchange_rates():
    from services.fx import fetch_exchange_rate
    currencies = ['CAD', 'USD']
    updated = sum(
        1 for f in currencies for t in currencies
        if f != t and fetch_exchange_rate(f, t)
    )
    flash(f'Updated {updated} exchange rates', 'success')
    return redirect(url_for('exchange_rates_view'))


@app.route('/exchange-rates/add', methods=['POST'])
@login_required
def add_exchange_rate():
    from_currency = request.form.get('from_currency')
    to_currency = request.form.get('to_currency')
    rate = request.form.get('rate')
    if not all([from_currency, to_currency, rate]):
        flash('All fields are required', 'error')
        return redirect(url_for('exchange_rates_view'))
    db.session.add(ExchangeRate(
        from_currency=from_currency,
        to_currency=to_currency,
        rate=float(rate),
        source='manual',
    ))
    db.session.commit()
    flash('Exchange rate added successfully', 'success')
    return redirect(url_for('exchange_rates_view'))


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
@app.route('/api/holdings', methods=['GET'])
@login_required
def api_get_holdings():
    accounts = _p_accounts()
    exchange_rates = get_exchange_rates(current_portfolio())
    return jsonify([
        {
            'account': account.name,
            'account_type': account.account_type,
            'ticker': h.security.ticker,
            'name': h.security.name,   
            'quantity': h.quantity,
            'price': h.price,
            'currency': h.security.currency,  
            'value': h.market_value,
            'value_base_currency': h.market_value_in_base_currency(exchange_rates),
            'asset_class': h.security.asset_class.name, 
            'is_public': h.security.is_public,
        }
        for account in accounts
        for h in account.holdings
    ])


@app.route('/api/portfolio/summary', methods=['GET'])
@login_required
def api_portfolio_summary():
    P = current_portfolio()
    exchange_rates = get_exchange_rates(P)
    allocation, allocation_pct, total_value = calculate_portfolio_allocation(P, exchange_rates)
    return jsonify({
        'portfolio': P.name,
        'total_value': total_value,
        'base_currency': P.base_currency,
        'num_accounts': len(P.accounts),
        'asset_allocation': dict(allocation),
        'asset_allocation_pct': dict(allocation_pct),
    })


# ---------------------------------------------------------------------------
# Error handlers
# ---------------------------------------------------------------------------
@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    db.session.rollback()
    return render_template('500.html'), 500

@app.errorhandler(403)
def forbidden(error):
    return render_template('403.html'), 403


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
# Schema/data upgrades for existing databases live in migrations.py.
def _backfill_prices():
    """One price per security: fill Security.last_price from holdings where missing,
    then repair holdings stuck at price 0."""
    changed = False
    for sec in Security.query.filter(Security.last_price.is_(None)).all():
        h = (Holding.query.filter(Holding.security_id == sec.id, Holding.price > 0)
             .order_by(Holding.updated_at.desc()).first())
        if h:
            sec.last_price = h.price
            changed = True
    for h in Holding.query.filter((Holding.price.is_(None)) | (Holding.price <= 0)).all():
        if h.security and h.security.last_price:
            h.price = h.security.last_price
            changed = True
    if changed:
        db.session.commit()
        log.info('Backfilled security prices.')


def _init_db():
    with app.app_context():
        db.create_all()
        migrations.run_all()
        _backfill_prices()
        log.info('Database schema ensured.')


_init_db()

if __name__ == '__main__':
    app.run(debug=os.getenv('FLASK_ENV') != 'production', host='0.0.0.0', port=5000)

