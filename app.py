import os
import logging
from flask import Flask, render_template, redirect, url_for, request, jsonify, flash
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

from models import User, Account, Holding, Security, SecurityPreference
from models import Target, AssetClass, ExchangeRate, RebalanceTransaction
from auth import auth_bp, init_oauth
from services.fx import get_exchange_rates
from services.portfolio import (
    calculate_portfolio_allocation,
    calculate_asset_class_deltas,
)
from services.prices import fetch_prices_from_user_sheet
from rebalancer import generate_rebalance_transactions

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
    accounts = Account.query.filter_by(user_id=current_user.id).all()
    targets = Target.query.filter_by(user_id=current_user.id).all()
    exchange_rates = get_exchange_rates(current_user)
    allocation, allocation_pct, total_value = calculate_portfolio_allocation(
        current_user, exchange_rates
    )
    return render_template(
        'dashboard.html',
        user=current_user,
        accounts=accounts,
        targets=targets,
        total_value=total_value,
        allocation=allocation,
        allocation_pct=allocation_pct,
        exchange_rates=exchange_rates,
        base_currency=current_user.base_currency,
    )


# ---------------------------------------------------------------------------
# Routes – Accounts
# ---------------------------------------------------------------------------
@app.route('/accounts')
@login_required
def accounts():
    user_accounts = Account.query.filter_by(user_id=current_user.id).all()
    return render_template('accounts.html', accounts=user_accounts)


@app.route('/accounts/add', methods=['POST'])
@login_required
def add_account():
    name = request.form.get('name', '').strip()
    if not name:
        flash('Account name is required', 'error')
        return redirect(url_for('accounts'))
    account = Account(
        user_id=current_user.id,
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
    account = Account.query.get_or_404(account_id)
    if account.user_id != current_user.id:
        flash('Unauthorized access', 'error')
        return redirect(url_for('accounts'))
    db.session.delete(account)
    db.session.commit()
    flash(f'Account "{account.name}" deleted successfully', 'success')
    return redirect(url_for('accounts'))


@app.route('/accounts/<int:account_id>/cash', methods=['POST'])
@login_required
def update_cash_balance(account_id):
    account = Account.query.get_or_404(account_id)
    if account.user_id != current_user.id:
        flash('Unauthorized', 'error')
        return redirect(url_for('holdings'))
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
    accounts = Account.query.filter_by(user_id=current_user.id).all()
    exchange_rates = get_exchange_rates(current_user)
    all_holdings = (
        db.session.query(Holding)
        .join(Account)
        .outerjoin(Security)
        .filter(Account.user_id == current_user.id)
        .order_by(Account.name.asc(), Security.ticker.asc())
        .all()
    )
    return render_template(
        'holdings.html',
        holdings=all_holdings,
        accounts=accounts,
        exchange_rates=exchange_rates,
        base_currency=current_user.base_currency,
        asset_classes=AssetClass.query.order_by(AssetClass.name.asc()).all(),
        securities=Security.query.order_by(Security.ticker.asc()).all(),
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
    account = Account.query.get_or_404(account_id)
    if account.user_id != current_user.id:
        flash('Unauthorized access', 'error')
        return redirect(url_for('holdings'))
    security = Security.query.get_or_404(security_id)
    holding = Holding(
        account_id=account_id,
        security_id=security_id,
        quantity=float(quantity),
        price=float(price) if price else 0.0,
    )
    db.session.add(holding)
    db.session.commit()
    flash(f'Holding {security.ticker} added successfully', 'success')
    return redirect(url_for('holdings'))


@app.route('/holdings/<int:holding_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_holding(holding_id):
    holding = Holding.query.get_or_404(holding_id)
    if holding.account.user_id != current_user.id:
        flash('Unauthorized access', 'error')
        return redirect(url_for('holdings'))
    if request.method == 'POST':
        holding.account_id = request.form.get('account_id')
        holding.quantity = float(request.form.get('quantity', 0))
        holding.price = float(request.form.get('price', 0))
        holding.security_id = request.form.get('security_id')
        holding.notes = request.form.get('notes')
        db.session.commit()
        flash('Holding updated successfully.', 'success')
        return redirect(url_for('holdings'))
    return render_template(
        'edit_holding.html',
        holding=holding,
        accounts=Account.query.filter_by(user_id=current_user.id).all(),
        securities=Security.query.all(),
        asset_classes=AssetClass.query.order_by(AssetClass.name.asc()).all(),
    )


@app.route('/holdings/<int:holding_id>/delete', methods=['POST'])
@login_required
def delete_holding(holding_id):
    holding = Holding.query.get_or_404(holding_id)
    if holding.account.user_id != current_user.id:
        flash('Unauthorized access', 'error')
        return redirect(url_for('holdings'))
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
        if not prices:
            flash('No prices fetched from Google Sheet.', 'warning')
            return redirect(url_for('holdings'))
        all_holdings = (
            Holding.query
            .join(Account)
            .join(Security)
            .filter(
                Account.user_id == current_user.id,
                Security.is_public == True,
                Security.auto_update_price == True,
            )
            .all()
        )
        updated = 0
        for h in all_holdings:
            price = prices.get(h.security.ticker)
            if price is not None:
                h.price = float(price)
                h.updated_at = datetime.utcnow()
                updated += 1
        db.session.commit()
        flash(f'Successfully updated {updated} security prices!', 'success')
    except Exception as exc:
        log.exception('Error updating prices')
        flash(f'Error updating prices: {exc}', 'danger')
    return redirect(url_for('holdings'))


# ---------------------------------------------------------------------------
# Routes – Targets
# ---------------------------------------------------------------------------
@app.route('/targets')
@login_required
def targets():
    return render_template(
        'targets.html',
        targets=Target.query.filter_by(user_id=current_user.id).all(),
        accounts=Account.query.filter_by(user_id=current_user.id).all(),
        asset_classes=AssetClass.query.order_by(AssetClass.name.asc()).all(),
    )


@app.route('/targets/update', methods=['POST'])
@login_required
def update_targets():
    action = request.form.get('action')
    if action == 'add':
        asset_class_id = request.form.get('asset_class_id')
        if asset_class_id == 'new':
            name = request.form.get('new_asset_class_name', '').strip()
            if not name:
                flash('Asset class name is required', 'danger')
                return redirect(url_for('targets'))
            ac = AssetClass(name=name)
            db.session.add(ac)
            db.session.flush()
            asset_class_id = ac.id
        db.session.add(Target(
            user_id=current_user.id,
            asset_class_id=asset_class_id,
            target_percentage=float(request.form.get('percentage', 0)),
        ))
        db.session.commit()
        flash('Target added successfully!', 'success')
    elif action == 'update':
        target = Target.query.get_or_404(request.form.get('target_id'))
        if target.user_id != current_user.id:
            flash('Unauthorized', 'danger')
            return redirect(url_for('targets'))
        target.asset_class_id = request.form.get('asset_class_id')
        target.target_percentage = float(request.form.get('percentage', 0))
        db.session.commit()
        flash('Target updated successfully!', 'success')
    elif action == 'delete':
        target = Target.query.get_or_404(request.form.get('target_id'))
        if target.user_id != current_user.id:
            flash('Unauthorized', 'danger')
            return redirect(url_for('targets'))
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
    all_securities = (
        db.session.query(Security)
        .outerjoin(AssetClass)
        .order_by(AssetClass.name.asc(), Security.ticker.asc())
        .all()
    )
    prefs = SecurityPreference.query.filter_by(user_id=current_user.id).all()
    return render_template(
        'securities.html',
        securities=all_securities,
        preferences={p.security_id: p for p in prefs},
        accounts=Account.query.filter_by(user_id=current_user.id).all(),
        restriction_types=[
            ('unrestricted', 'Unrestricted - Can be held in any account'),
            ('restricted_to_accounts', 'Restricted - Only specific accounts allowed'),
            ('prioritized_accounts', 'Prioritized - Prefer certain accounts over others'),
        ],
        asset_classes=AssetClass.query.order_by(AssetClass.name.asc()).all(),
    )


@app.route('/securities/add', methods=['GET', 'POST'])
@login_required
def add_security():
    if request.method == 'POST':
        ticker = request.form.get('ticker', '').strip()
        asset_class_id = request.form.get('asset_class_id')
        if not ticker:
            flash('Ticker is required', 'error')
        elif not asset_class_id:
            flash('Asset class is required', 'error')
        else:
            db.session.add(Security(
                ticker=ticker,
                name=request.form.get('name'),
                asset_class_id=int(asset_class_id),
                currency=request.form.get('currency', 'CAD'),
                is_public=request.form.get('is_public') == 'on',
                auto_update_price=request.form.get('auto_update_price') == 'on',
            ))
            db.session.commit()
            flash(f'Security {ticker} added.', 'success')
            return redirect(url_for('securities'))
    return render_template(
        'add_security.html',
        asset_classes=AssetClass.query.order_by(AssetClass.name.asc()).all(),
    )


@app.route('/securities/<int:security_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_security(security_id):
    security = Security.query.get_or_404(security_id)
    if request.method == 'POST':
        security.ticker = request.form.get('ticker')
        security.name = request.form.get('name')
        security.asset_class_id = request.form.get('asset_class_id')
        security.currency = request.form.get('currency', 'CAD')
        security.is_public = request.form.get('is_public') == 'on'
        security.auto_update_price = request.form.get('auto_update_price') == 'on'
        db.session.commit()
        flash(f'Security {security.ticker} updated successfully.', 'success')
        return redirect(url_for('securities'))
    return render_template(
        'edit_security.html',
        security=security,
        asset_classes=AssetClass.query.order_by(AssetClass.name.asc()).all(),
    )


@app.route('/securities/<int:security_id>/delete', methods=['POST'])
@login_required
def delete_security(security_id):
    s = Security.query.get_or_404(security_id)
    db.session.delete(s)
    db.session.commit()
    flash(f'Deleted {s.ticker}.', 'success')
    return redirect(url_for('securities'))


@app.route('/securities/<int:security_id>/preference', methods=['POST'])
@login_required
def update_security_preference(security_id):
    security = Security.query.get_or_404(security_id)
    restriction_type = request.form.get('restriction_type')
    if not restriction_type:
        flash('Restriction type is required', 'error')
        return redirect(url_for('securities'))
    account_config = None
    if restriction_type == 'restricted_to_accounts':
        allowed = request.form.getlist('allowed_accounts[]')
        if allowed:
            account_config = {'allowed': [int(a) for a in allowed]}
    elif restriction_type == 'prioritized_accounts':
        account_config = {}
        for level in (1, 2, 3):
            ids = request.form.getlist(f'priority_{level}[]')
            if ids:
                account_config[f'priority_{level}'] = [int(i) for i in ids]
    pref = SecurityPreference.query.filter_by(
        security_id=security_id, user_id=current_user.id
    ).first()
    if pref:
        pref.restriction_type = restriction_type
        pref.account_config = account_config
        pref.notes = request.form.get('notes')
    else:
        db.session.add(SecurityPreference(
            security_id=security_id,
            user_id=current_user.id,
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
    transactions = RebalanceTransaction.query.filter_by(
        user_id=current_user.id, executed=False
    ).order_by(RebalanceTransaction.execution_order).all()
    exchange_rates = get_exchange_rates(current_user)
    allocation_by_id, allocation_pct_by_id, total_portfolio = calculate_portfolio_allocation(
        current_user, exchange_rates
    )
    targets = Target.query.filter_by(user_id=current_user.id).all()
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
        securities=Security.query.all(),
        comparison_data=comparison_data,
        total_portfolio=total_portfolio,
        base_currency=current_user.base_currency,
        accounts=current_user.accounts,
        balanced_threshold=current_user.balanced_threshold,
    )


@app.route('/rebalance/details/<int:asset_class_id>')
@login_required
def rebalance_details(asset_class_id):
    asset_class = AssetClass.query.get_or_404(asset_class_id)
    all_securities = Security.query.filter_by(asset_class_id=asset_class_id).all()
    prefs = {
        p.security_id: p
        for p in SecurityPreference.query.filter_by(user_id=current_user.id).all()
    }
    user_accounts = Account.query.filter_by(user_id=current_user.id).all()
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
        base_currency=current_user.base_currency,
    )


@app.route('/rebalance/generate', methods=['POST'])
@login_required
def generate_rebalance():
    try:
        txns = generate_rebalance_transactions(current_user)
        flash(f'Generated {len(txns)} rebalance transactions', 'success')
    except Exception as exc:
        log.exception('Error generating rebalance plan')
        flash(f'Error generating rebalance plan: {exc}', 'error')
    return redirect(url_for('rebalance'))


@app.route('/rebalance/execute/<int:transaction_id>', methods=['POST'])
@login_required
def execute_rebalance_transaction(transaction_id):
    txn = RebalanceTransaction.query.get_or_404(transaction_id)
    if txn.user_id != current_user.id:
        flash('Unauthorized', 'error')
        return redirect(url_for('rebalance'))
    if txn.requires_user_selection:
        security_id = request.form.get('security_id')
        if not security_id:
            flash('Please select a security', 'error')
            return redirect(url_for('rebalance'))
        txn.security_id = int(security_id)
    try:
        account = Account.query.get_or_404(txn.account_id)
        holding = Holding.query.filter_by(
            account_id=txn.account_id, security_id=txn.security_id
        ).first()
        if txn.action == 'SELL':
            if not holding or holding.quantity < txn.quantity:
                raise ValueError(
                    f'Insufficient quantity to sell: have '
                    f'{holding.quantity if holding else 0}, need {txn.quantity}'
                )
            holding.quantity -= txn.quantity
            if holding.quantity <= 0:
                db.session.delete(holding)
            account.cash_balance = (account.cash_balance or 0) + txn.amount
        else:  # BUY
            if (account.cash_balance or 0) < txn.amount:
                raise ValueError(
                    f'Insufficient cash: have {account.cash_balance:.2f}, '
                    f'need {txn.amount:.2f}'
                )
            if holding:
                holding.quantity += txn.quantity
            else:
                db.session.add(Holding(
                    account_id=txn.account_id,
                    security_id=txn.security_id,
                    quantity=txn.quantity,
                    price=txn.price,
                ))
            account.cash_balance = (account.cash_balance or 0) - txn.amount
        txn.executed = True
        txn.executed_at = datetime.utcnow()
        db.session.commit()
        flash(f'Executed: {txn.action} {txn.quantity:.2f} of {txn.security.ticker}', 'success')

        # Regen is best-effort — don't let it mask the successful trade
        try:
            generate_rebalance_transactions(current_user)
        except Exception as regen_exc:
            log.warning('Post-execution regen failed (non-fatal): %s', regen_exc)
            flash('Trade executed, but plan could not be regenerated automatically. Refresh manually.', 'warning')

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
    return render_template('settings.html', user=current_user)


@app.route('/settings/update', methods=['POST'])
@login_required
def update_settings():
    base_currency = request.form.get('base_currency')
    if base_currency not in ('CAD', 'USD'):
        flash('Invalid currency selection', 'error')
        return redirect(url_for('settings'))
    current_user.base_currency = base_currency
    price_sheet_id = request.form.get('price_sheet_id', '').strip()
    if price_sheet_id:
        current_user.price_sheet_id = price_sheet_id
    current_user.trading_costs_enabled = request.form.get('trading_costs_enabled') == 'on'
    current_user.precision_rebalancing = request.form.get('precision_rebalancing') == 'on'
    current_user.balanced_threshold = float(request.form.get('balanced_threshold', 0.5))
    db.session.commit()
    flash('Settings updated successfully', 'success')
    return redirect(url_for('settings'))


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
    accounts = Account.query.filter_by(user_id=current_user.id).all()
    exchange_rates = get_exchange_rates(current_user)
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
    exchange_rates = get_exchange_rates(current_user)
    allocation, allocation_pct, total_value = calculate_portfolio_allocation(
        current_user, exchange_rates
    )
    return jsonify({
        'total_value': total_value,
        'base_currency': current_user.base_currency,
        'num_accounts': len(current_user.accounts),
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
def _init_db():
    with app.app_context():
        db.create_all()
        log.info('Database schema ensured.')


_init_db()

if __name__ == '__main__':
    app.run(debug=os.getenv('FLASK_ENV') != 'production', host='0.0.0.0', port=5000)

