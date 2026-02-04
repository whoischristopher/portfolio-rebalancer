import os
from flask import Flask, render_template, redirect, url_for, request, jsonify, flash
from flask_login import LoginManager, login_required, current_user
from extensions import db
import yfinance as yf
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import inspect
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

app = Flask(__name__)

# Configuration
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key-please-change-in-production')
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'sqlite:///data/portfolio.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SESSION_COOKIE_SECURE'] = os.getenv('FLASK_ENV') == 'production'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# Initialize database
db.init_app(app)

from models import *
from auth import auth_bp, init_oauth

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Please log in to access this page.'
login_manager.login_message_category = 'info'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# Initialize OAuth
init_oauth(app)

# Register authentication blueprint
app.register_blueprint(auth_bp, url_prefix='/auth')


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def fetch_exchange_rate(from_curr, to_curr):
    '''Fetch current exchange rate with caching'''
    try:
        if from_curr == to_curr:
            return 1.0
        
        # Check for recent cached rate (less than 4 hours old)
        cached_rate = ExchangeRate.query.filter_by(
            from_currency=from_curr,
            to_currency=to_curr
        ).order_by(ExchangeRate.date.desc()).first()
        
        if cached_rate and (datetime.utcnow() - cached_rate.date).total_seconds() < 14400:  # 4 hours
            return cached_rate.rate
        
        # Try exchangerate-api.com (free, no key needed for basic usage)
        import requests
        url = f"https://api.exchangerate-api.com/v4/latest/{from_curr}"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if to_curr in data['rates']:
                rate = float(data['rates'][to_curr])
                
                # Delete old rates
                ExchangeRate.query.filter_by(
                    from_currency=from_curr,
                    to_currency=to_curr
                ).delete()
                
                # Save new rate
                exchange_rate = ExchangeRate(
                    from_currency=from_curr,
                    to_currency=to_curr,
                    rate=rate,
                    source='exchangerate-api',
                    date=datetime.utcnow()
                )
                db.session.add(exchange_rate)
                db.session.commit()
                
                return rate
        
        # Fallback to cached if API fails
        if cached_rate:
            return cached_rate.rate
            
        return 1.35  # Last resort
        
    except Exception as e:
        print(f"Error fetching exchange rate: {e}")
        if cached_rate:
            return cached_rate.rate
        return 1.35

def get_exchange_rates(user):
    '''Get current exchange rates for user's currencies'''
    rates = {}
    
    # Use fetch_exchange_rate to get fresh rates (with caching)
    try:
        usd_to_cad = fetch_exchange_rate('USD', 'CAD')
        rates['USD_TO_CAD'] = usd_to_cad
        rates['CAD_TO_USD'] = 1 / usd_to_cad
    except Exception as e:
        print(f"Error fetching USD/CAD rate: {e}")
        rates['USD_TO_CAD'] = 1.35
        rates['CAD_TO_USD'] = 1 / 1.35
    
    return rates

def calculate_portfolio_allocation(user, exchange_rates):
    """Calculate current portfolio allocation by asset class (keyed by asset_class_id)."""
    allocation = defaultdict(float)
    total_value = 0

    for account in user.accounts:
        for holding in account.holdings:
            value = holding.market_value_in_base_currency(exchange_rates)
            if holding.asset_class_id is None:
                continue  # or handle unclassified holdings separately
            allocation[holding.asset_class_id] += value
            total_value += value

    # Convert to percentages
    allocation_pct = {}
    for asset_class_id, value in allocation.items():
        allocation_pct[asset_class_id] = (value / total_value * 100) if total_value > 0 else 0

    return allocation, allocation_pct, total_value


def fetch_prices_from_user_sheet(user):
    """Read prices from the user's Google Sheet using their OAuth token."""
    
    if not user.google_token or not user.price_sheet_id:
        return {}
    
    try:
        token_dict = json.loads(user.google_token)
        
        creds = Credentials(
            token=token_dict['access_token'],
            refresh_token=token_dict.get('refresh_token'),
            token_uri='https://oauth2.googleapis.com/token',
            client_id=os.getenv('GOOGLE_CLIENT_ID'),
            client_secret=os.getenv('GOOGLE_CLIENT_SECRET'),
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        
        service = build('sheets', 'v4', credentials=creds)
        
        # Read range A1:B (ticker in A, price from GOOGLEFINANCE in B)
        result = service.spreadsheets().values().get(
            spreadsheetId=user.price_sheet_id,
            range='Sheet1!A1:B'
        ).execute()
        
        rows = result.get('values', [])
        
        prices = {}

        for row in rows:
            if len(row) >= 2 and row[0]:
                try:
                    value = float(row[1])
                    prices[row[0]] = value
                except (ValueError, TypeError):
                    pass
        
        return prices
        
    except Exception as e:
        print(f"Error reading user sheet: {e}")
        return {}


def calculate_asset_class_deltas(user, exchange_rates):
    """Calculate dollar and percentage differences from target allocation"""
    allocation, allocation_pct, total_portfolio = calculate_portfolio_allocation(user, exchange_rates)
    targets = Target.query.filter_by(user_id=user.id).all()
    
    deltas = []
    for target in targets:
        current_value = allocation.get(target.asset_class_id, 0)
        current_pct = allocation_pct.get(target.asset_class_id, 0)
        target_value = total_portfolio * target.target_percentage / 100
        dollar_diff = target_value - current_value
        percentage_diff = current_pct - target.target_percentage
        
        deltas.append({
            'asset_class_id': target.asset_class_id,
            'asset_class_name': target.asset_class.name,
            'target': target,
            'current_value': current_value,
            'target_value': target_value,
            'dollar_diff': dollar_diff,
            'percentage_diff': percentage_diff,
            'current_pct': current_pct,
            'target_pct': target.target_percentage
        })
    
    return deltas, total_portfolio


def get_eligible_securities_for_account(asset_class_id, account, user):
    """Get securities that can be held in the specified account"""
    securities = Security.query.filter_by(asset_class_id=asset_class_id).all()
    prefs = {pref.security_id: pref for pref in SecurityPreference.query.filter_by(user_id=user.id).all()}
    
    eligible = []
    for security in securities:
        pref = prefs.get(security.id)
        
        # Check restrictions
        if not pref or pref.restriction_type == 'unrestricted':
            eligible.append(security)
        elif pref.restriction_type == 'restricted_to_accounts':
            allowed_ids = pref.account_config.get('allowed', []) if pref.account_config else []
            if account.id in allowed_ids:
                eligible.append(security)
        elif pref.restriction_type == 'prioritized_accounts':
            # Prioritized means allowed everywhere, just with preferences
            eligible.append(security)
    
    return eligible


def prioritize_accounts_for_sell(accounts, prefer_registered=True):
    """Sort accounts for selling - registered first for tax efficiency"""
    sorted_accounts = sorted(accounts, key=lambda a: (
        not a.is_registered if prefer_registered else a.is_registered,
        -a.priority
    ))
    return sorted_accounts

def generate_rebalance_transactions(user):
    """Generate minimal rebalance transactions at portfolio level
    
    Simple Approach:
    - Calculate portfolio-level imbalances
    - For each underweight asset class, find best account to buy it
    - Generate sells in that same account to fund the buy
    - Minimize transactions by being selective about what to rebalance
    """
        
    # REFRESH PRICES FIRST
    try:
        prices = fetch_prices_from_user_sheet(user)
        
        if prices:
            holdings = (
                Holding.query
                .join(Account)
                .join(Security)
                .filter(
                    Account.user_id == user.id,
                    Security.is_public.is_(True),
                    Security.auto_update_price.is_(True)
                )
                .all() 
            )
            
            for holding in holdings:
                symbol = holding.security.ticker
                price = prices.get(symbol)
                if price is not None:
                    holding.price = float(price)
                    holding.updated_at = datetime.utcnow()
        
            db.session.commit()
        
        # Refresh exchange rates
        fetch_exchange_rate('USD', 'CAD')
        fetch_exchange_rate('CAD', 'USD')

    except Exception as e:
        print(f"Warning: Could not refresh prices: {e}")

    exchange_rates = get_exchange_rates(user)

    # Clear old unexecuted transactions
    RebalanceTransaction.query.filter_by(user_id=user.id, executed=False).delete()

    # Calculate portfolio-level deltas
    deltas, total_portfolio = calculate_asset_class_deltas(user, exchange_rates)

    # Get balanced threshold
    balanced_threshold = user.balanced_threshold or 0.5

    # Filter out balanced asset classes
    deltas = [d for d in deltas if abs(d['percentage_diff']) > balanced_threshold]

    if not deltas:
        db.session.commit()
        return []

    # Additional filter for trading costs if enabled
    if user.trading_costs_enabled:
        deltas = [d for d in deltas if abs(d['percentage_diff']) >= 0.1]

    # Separate into overweight and underweight
    overweight = {d['asset_class_id']: abs(d['dollar_diff']) for d in deltas if d['dollar_diff'] < 0}
    underweight = {d['asset_class_id']: abs(d['dollar_diff']) for d in deltas if d['dollar_diff'] > 0}

    transactions = []
    execution_order = 1
    
    # Track what still needs to be sold/bought
    remaining_to_sell = overweight.copy()
    remaining_to_buy = underweight.copy()

    # Process each underweight asset class (things we need to BUY)
    for asset_class_id, buy_amount in underweight.items():
        # Find the best account to buy this asset class
        # Prioritize: accounts with existing holdings of this class > registered > largest accounts
        
        candidate_accounts = []
        for account in user.accounts:
            has_existing = any(
                h.security and h.security.asset_class_id == asset_class_id 
                for h in account.holdings
            )
            
            # Check if we can sell overweight positions in this account to fund the buy
            sellable_value = 0
            for holding in account.holdings:
                if holding.security and holding.security.asset_class_id in remaining_to_sell:
                    sellable_value += holding.market_value
            
            account_value = sum(h.market_value for h in account.holdings)
            
            candidate_accounts.append({
                'account': account,
                'has_existing': has_existing,
                'sellable_value': sellable_value,
                'account_value': account_value,
                'is_registered': account.is_registered
            })
        
        # Sort: existing > sellable value > registered > largest
        candidate_accounts.sort(key=lambda x: (
            not x['has_existing'],
            -x['sellable_value'],
            not x['is_registered'],
            -x['account_value']
        ))
        
        if not candidate_accounts:
            continue
        
        best_account = candidate_accounts[0]['account']
        
        # Generate SELL transactions in this account to fund the buy
        cash_available = best_account.cash_balance
        cash_needed = buy_amount
        
        for holding in best_account.holdings:
            if cash_needed <= 0.01:
                break
            
            if not holding.security or not holding.security.asset_class_id:
                continue
            
            # Only sell if this asset class is overweight portfolio-wide
            if holding.security.asset_class_id not in remaining_to_sell:
                continue
            
            amount_to_sell = min(cash_needed, holding.market_value, remaining_to_sell[holding.security.asset_class_id])
            
            quantity_to_sell = int(amount_to_sell / holding.price)
            actual_sell = quantity_to_sell * holding.price
            
            if quantity_to_sell < 1:
                continue
            
            txn = RebalanceTransaction(
                user_id=user.id,
                account_id=best_account.id,
                security_id=holding.security_id,
                action='SELL',
                quantity=quantity_to_sell,
                price=holding.price,
                amount=actual_sell,
                currency=holding.security.currency,
                execution_order=execution_order,
                is_final_trade=False
            )
            db.session.add(txn)
            transactions.append(txn)
            execution_order += 1
            
            remaining_to_sell[holding.security.asset_class_id] -= actual_sell
            cash_available += actual_sell
            cash_needed -= actual_sell
        
        # Generate BUY transaction
        amount_to_buy = min(buy_amount, cash_available)
        
        if amount_to_buy < 1:
            continue
        
        # Find eligible securities for this asset class in this account
        securities = Security.query.filter_by(asset_class_id=asset_class_id).all()
        
        eligible = []
        for security in securities:
            preference = SecurityPreference.query.filter_by(
                user_id=user.id,
                security_id=security.id
            ).first()
            
            can_use = True
            if preference:
                if preference.restriction_type == 'restricted_to_accounts':
                    if preference.account_config and preference.account_config.get('allowed'):
                        can_use = best_account.id in preference.account_config['allowed']
                elif preference.restriction_type == 'prioritized_accounts':
                    if preference.account_config and preference.account_config.get('priority_1'):
                        can_use = best_account.id in preference.account_config['priority_1']
            
            if can_use:
                existing = any(h.security_id == security.id for h in best_account.holdings)
                eligible.append({'security': security, 'existing': existing})
        
        if not eligible:
            continue
        
        # Prefer existing holdings
        eligible.sort(key=lambda x: (not x['existing'], x['security'].id))
        
        if len(eligible) > 1:
            txn = RebalanceTransaction(
                user_id=user.id,
                account_id=best_account.id,
                action='BUY',
                quantity=0,
                price=0,
                amount=amount_to_buy,
                currency=best_account.currency,
                execution_order=execution_order,
                requires_user_selection=True,
                available_securities=[s['security'].id for s in eligible],
                is_final_trade=False
            )
            db.session.add(txn)
            transactions.append(txn)
            execution_order += 1
        else:
            security = eligible[0]['security']
            
            # Get price
            existing_holding = next((h for h in best_account.holdings if h.security_id == security.id), None)
            if existing_holding:
                price = existing_holding.price
            else:
                any_holding = Holding.query.filter_by(security_id=security.id).first()
                price = any_holding.price if any_holding else None
            
            if not price or price <= 0:
                continue
            
            quantity = int(amount_to_buy / price)
            actual_buy = quantity * price
            
            if quantity < 1:
                continue
            
            txn = RebalanceTransaction(
                user_id=user.id,
                account_id=best_account.id,
                security_id=security.id,
                action='BUY',
                quantity=quantity,
                price=price,
                amount=actual_buy,
                currency=security.currency,
                execution_order=execution_order,
                is_final_trade=False
            )
            db.session.add(txn)
            transactions.append(txn)
            execution_order += 1
        
        remaining_to_buy[asset_class_id] -= amount_to_buy

    # Mark final BUY per account as fractional
    account_last_buy = {}
    for txn in transactions:
        if txn.action == 'BUY':
            account_last_buy[txn.account_id] = txn

    for account_id, last_txn in account_last_buy.items():
        last_txn.is_final_trade = True

    db.session.commit()
    return transactions


# ============================================================================
# ROUTES
# ============================================================================

@app.route('/')
def index():
    '''Landing page'''
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return render_template('index.html')

@app.route('/dashboard')
@login_required
def dashboard():
    '''Main dashboard showing portfolio overview'''
    accounts = Account.query.filter_by(user_id=current_user.id).all()
    targets = Target.query.filter_by(user_id=current_user.id).all()
    
    # Get exchange rates
    exchange_rates = get_exchange_rates(current_user)
    
    # Calculate total portfolio value
    total_value = 0
    for account in accounts:
        for holding in account.holdings:
            total_value += holding.market_value_in_base_currency(exchange_rates)
    
    # Calculate current allocation
    allocation, allocation_pct, _ = calculate_portfolio_allocation(current_user, exchange_rates)
    
    return render_template('dashboard.html', 
                         user=current_user,
                         accounts=accounts,
                         targets=targets,
                         total_value=total_value,
                         allocation=allocation,
                         allocation_pct=allocation_pct,
                         exchange_rates=exchange_rates,
                         base_currency=current_user.base_currency)


@app.route('/accounts')
@login_required
def accounts():
    '''View and manage accounts'''
    user_accounts = Account.query.filter_by(user_id=current_user.id).all()
    return render_template('accounts.html', accounts=user_accounts)


@app.route('/accounts/add', methods=['POST'])
@login_required
def add_account():
    '''Add new account'''
    name = request.form.get('name')
    account_type = request.form.get('account_type')
    currency = request.form.get('currency', 'CAD')
    is_registered = request.form.get('is_registered') == 'on'
    
    if not name:
        flash('Account name is required', 'error')
        return redirect(url_for('accounts'))
    
    account = Account(
        user_id=current_user.id,
        name=name,
        account_type=account_type,
        currency=currency,
        is_registered=is_registered
    )
    db.session.add(account)
    db.session.commit()
    
    flash(f'Account "{name}" added successfully', 'success')
    return redirect(url_for('accounts'))


@app.route('/accounts/<int:account_id>/delete', methods=['POST'])
@login_required
def delete_account(account_id):
    '''Delete account'''
    account = Account.query.get_or_404(account_id)
    
    if account.user_id != current_user.id:
        flash('Unauthorized access', 'error')
        return redirect(url_for('accounts'))
    
    db.session.delete(account)
    db.session.commit()
    
    flash(f'Account "{account.name}" deleted successfully', 'success')
    return redirect(url_for('accounts'))

@app.route('/holdings')
@login_required
def holdings():
    """View all holdings, with joined security and preference data"""

    # Pull all accounts for the current user
    accounts = Account.query.filter_by(user_id=current_user.id).all()

    # FX rates for portfolio display
    exchange_rates = get_exchange_rates(current_user)

    # Query holdings, joining related models so templates can render complete info
    holdings = (
        db.session.query(Holding)
        .join(Account)
        .outerjoin(Security)
        .outerjoin(SecurityPreference)
        .filter(Account.user_id == current_user.id)
        .order_by(Account.name.asc(), Security.ticker.asc())
        .all()
    )

    # Collect asset classes from user's Target table (for dropdowns, filters, etc.)
    asset_classes = AssetClass.query.order_by(AssetClass.name.asc()).all()
    securities = Security.query.order_by(Security.ticker.asc()).all() 

    # Render holdings page: includes holdings, preferences, dropdown asset classes
    return render_template(
        'holdings.html',
        holdings=holdings,
        accounts=accounts,
        exchange_rates=exchange_rates,
        base_currency=current_user.base_currency,
        asset_classes=asset_classes,
        securities=securities,
    )

@app.route('/holdings/add', methods=['POST'])
@login_required
def add_holding():
    '''Add new holding to an account'''
    account_id = request.form.get('account_id')
    security_id = request.form.get('security_id')
    quantity = request.form.get('quantity')
    price = request.form.get('price')
    
    account = Account.query.get_or_404(account_id)
    if account.user_id != current_user.id:
        flash('Unauthorized access', 'error')
        return redirect(url_for('holdings'))
    
    if not all([security_id, quantity]):
        flash('Ticker, quantity, and asset class are required', 'error')
        return redirect(url_for('holdings'))
    
    # Get security details
    security = Security.query.get_or_404(security_id)

    holding = Holding(
        account_id=account_id,
        security_id=security_id,
        ticker=security.ticker,
        quantity=float(quantity),
        price=float(price) if price else 0,
        currency=security.currency,
        asset_class_id=security.asset_class_id,
    )
    
    db.session.add(holding)
    db.session.commit()
    
    flash(f'Holding {security.ticker} added successfully', 'success')
    return redirect(url_for('holdings'))

@app.route("/edit_holding/<int:holding_id>", methods=["GET", "POST"])
@login_required
def edit_holding(holding_id):
    holding = Holding.query.get_or_404(holding_id)
    accounts = Account.query.filter_by(user_id=current_user.id).all()
    asset_classes = AssetClass.query.order_by(AssetClass.name.asc()).all()

    securities = Security.query.all()

    if request.method == "POST":
        holding.account_id = request.form.get("account_id")
        holding.quantity = request.form.get("quantity")
        holding.price = request.form.get("price")
        holding.asset_class_id = request.form.get('asset_class_id')
        holding.security_id = request.form.get("security_id")
        holding.notes = request.form.get("notes")
        db.session.commit()
        flash("Holding updated successfully.")
        return redirect(url_for("holdings"))

    return render_template(
        "edit_holding.html",
        holding=holding,
        accounts=accounts,
        securities=securities,
        asset_classes=asset_classes,
    )

@app.route('/holdings/<int:holding_id>/delete', methods=['POST'])
@login_required
def delete_holding(holding_id):
    '''Delete a holding'''
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
        
        if prices:
            holdings = (
                Holding.query
                .join(Account)
                .join(Security)
                .filter(
                    Account.user_id == current_user.id,
                    Security.is_public == True,
                    Security.auto_update_price == True
                )
                .all()
            )
            
            updated_count = 0
            for holding in holdings:
                symbol = holding.security.ticker
                price = prices.get(symbol)
                if price is not None:
                    holding.price = float(price)
                    holding.updated_at = datetime.utcnow()
                    updated_count += 1
            
            db.session.commit()
            flash(f"Successfully updated {updated_count} security prices!", "success")
        else:
            flash("No prices fetched from Google Sheet.", "warning")
    
    except Exception as e:
        flash(f"Error updating prices: {str(e)}", "danger")
    
    return redirect(url_for('holdings'))

@app.route('/targets')
@login_required
def targets():
    '''View and edit target allocations'''
    user_targets = Target.query.filter_by(user_id=current_user.id).all()
    accounts = Account.query.filter_by(user_id=current_user.id).all()
    asset_classes = AssetClass.query.order_by(AssetClass.name.asc()).all()

    return render_template('targets.html', targets=user_targets, accounts=accounts, asset_classes=asset_classes)

@app.route('/targets/update', methods=['POST'])
@login_required
def update_targets():
    action = request.form.get('action')
    
    if action == 'add':
        # Check if creating new asset class
        asset_class_id = request.form.get('asset_class_id')
        
        if asset_class_id == 'new':
            new_asset_class_name = request.form.get('new_asset_class_name', '').strip()
            if not new_asset_class_name:
                flash('Asset class name is required', 'danger')
                return redirect(url_for('targets'))
            
            asset_class = AssetClass(name=new_asset_class_name)
            db.session.add(asset_class)
            db.session.flush()
            asset_class_id = asset_class.id
        
        # Add new target (simplified - no account restrictions)
        target = Target(
            user_id=current_user.id,
            asset_class_id=asset_class_id,
            target_percentage=float(request.form.get('percentage'))
        )
        db.session.add(target)
        db.session.commit()
        flash('Target added successfully!', 'success')
        
    elif action == 'update':
        target_id = request.form.get('target_id')
        target = Target.query.get_or_404(target_id)
        
        if target.user_id != current_user.id:
            flash('Unauthorized', 'danger')
            return redirect(url_for('targets'))
        
        target.asset_class_id = request.form.get('asset_class_id')
        target.target_percentage = float(request.form.get('percentage'))
        
        db.session.commit()
        flash('Target updated successfully!', 'success')
        
    elif action == 'delete':
        target_id = request.form.get('target_id')
        target = Target.query.get_or_404(target_id)
        
        if target.user_id != current_user.id:
            flash('Unauthorized', 'danger')
            return redirect(url_for('targets'))
        
        db.session.delete(target)
        db.session.commit()
        flash('Target deleted successfully!', 'success')
    
    return redirect(url_for('targets'))

@app.route("/securities")
@login_required
def securities():
    """View and manage securities and restrictions"""
    securities = (
        db.session.query(Security)
        .outerjoin(AssetClass)
        .order_by(AssetClass.name.asc(), Security.ticker.asc())
        .all()
    )

    # Get preferences for this user
    prefs = (
        SecurityPreference.query
        .filter_by(user_id=current_user.id)
        .all()
    )
    
    # Create preference lookup dictionary
    pref_map = {pref.security_id: pref for pref in prefs}
    
    accounts = Account.query.filter_by(user_id=current_user.id).all()
    restriction_types = [
        ("unrestricted", "Unrestricted - Can be held in any account"),
        ("restricted_to_accounts", "Restricted - Only specific accounts allowed"),
        ("prioritized_accounts", "Prioritized - Prefer certain accounts over others"),
    ]

    asset_classes = AssetClass.query.order_by(AssetClass.name.asc()).all()

    return render_template(
        "securities.html",
        securities=securities,
        preferences=pref_map,
        accounts=accounts,
        restriction_types=restriction_types,
        asset_classes=asset_classes,
    )

@app.route("/edit_security/<int:security_id>", methods=["GET", "POST"])
@login_required
def edit_security(security_id):
    security = Security.query.get_or_404(security_id)
    
    if request.method == "POST":
        security.ticker = request.form.get("ticker")
        security.name = request.form.get("name")
        security.asset_class_id = request.form.get("asset_class_id")
        security.currency = request.form.get("currency", "CAD")
        security.is_public = request.form.get("is_public") == "on"
        security.auto_update_price = request.form.get("auto_update_price") == "on"
        
        db.session.commit()
        flash(f"Security {security.ticker} updated successfully.")
        return redirect(url_for("securities"))
    
    asset_classes = AssetClass.query.order_by(AssetClass.name.asc()).all()
    return render_template("edit_security.html", security=security, asset_classes=asset_classes)

@app.route('/securities/<int:security_id>/preference', methods=['POST'])
@login_required
def update_security_preference(security_id):
    '''Update preference/restriction for a specific security'''
    security = Security.query.get_or_404(security_id)
    
    restriction_type = request.form.get('restriction_type')
    notes = request.form.get('notes')
    
    if not restriction_type:
        flash('Restriction type is required', 'error')
        return redirect(url_for('securities'))

    # Build account configuration based on restriction type
    account_config = None
    
    if restriction_type == 'restricted_to_accounts':
        # Get list of allowed accounts
        allowed_accounts = request.form.getlist('allowed_accounts[]')
        if allowed_accounts:
            account_config = {"allowed": [int(aid) for aid in allowed_accounts]}
    
    elif restriction_type == 'prioritized_accounts':
        # Get prioritized account lists
        priority_1 = request.form.getlist('priority_1[]')
        priority_2 = request.form.getlist('priority_2[]')
        priority_3 = request.form.getlist('priority_3[]')
        
        account_config = {}
        if priority_1:
            account_config['priority_1'] = [int(aid) for aid in priority_1]
        if priority_2:
            account_config['priority_2'] = [int(aid) for aid in priority_2]
        if priority_3:
            account_config['priority_3'] = [int(aid) for aid in priority_3]
    
    # Find or create preference
    pref = SecurityPreference.query.filter_by(
        security_id=security_id, 
        user_id=current_user.id
    ).first()
    
    if pref:
        pref.restriction_type = restriction_type
        pref.account_config = account_config
        pref.notes = notes
    else:
        pref = SecurityPreference(
            security_id=security_id,
            user_id=current_user.id,
            restriction_type=restriction_type,
            account_config=account_config,
            notes=notes
        )
        db.session.add(pref)

    db.session.commit()
    flash(f'Preference for {security.ticker} updated successfully', 'success')
    return redirect(url_for('securities'))


@app.route("/add_security", methods=["GET", "POST"])
@login_required
def add_security():
    if request.method == "POST":
        ticker = request.form.get("ticker")
        name = request.form.get("name")
        asset_class_id = request.form.get("asset_class_id")
        currency = request.form.get("currency", "CAD")
        is_public = request.form.get("is_public") == "on"
        auto_update_price = request.form.get("auto_update_price") == "on"

        if not ticker:
            flash('Ticker is required', 'error')
            asset_classes = AssetClass.query.order_by(AssetClass.name.asc()).all()
            return render_template("add_security.html", asset_classes=asset_classes)
        
        if not asset_class_id:
            flash('Asset class is required', 'error')
            asset_classes = AssetClass.query.order_by(AssetClass.name.asc()).all()
            return render_template("add_security.html", asset_classes=asset_classes)

        security = Security(
            ticker=ticker,
            name=name,
            asset_class_id=int(asset_class_id),
            currency=currency,
            is_public=is_public,
            auto_update_price=auto_update_price
        )
        db.session.add(security)
        db.session.commit()
        flash(f"Security {ticker} added.")
        return redirect(url_for("securities"))

    asset_classes = AssetClass.query.order_by(AssetClass.name.asc()).all()
    return render_template("add_security.html", asset_classes=asset_classes)


@app.route("/delete_security/<int:security_id>", methods=["POST"])
@login_required
def delete_security(security_id):
    s = Security.query.get_or_404(security_id)
    db.session.delete(s)
    db.session.commit()
    flash(f"Deleted {s.ticker}.")
    return redirect(url_for("securities"))


@app.route('/rebalance')
@login_required
def rebalance():
    '''View rebalance transactions'''
    transactions = RebalanceTransaction.query.filter_by(
        user_id=current_user.id,
        executed=False
    ).order_by(RebalanceTransaction.execution_order).all()
    
    exchange_rates = get_exchange_rates(current_user)
    
    # Get all asset classes
    asset_classes = AssetClass.query.all()
    
    # Calculate current allocation by asset class ID
    allocation_by_id = {}
    allocation_pct_by_id = {}
    total_portfolio = 0
    
    for account in current_user.accounts:
        for holding in account.holdings:
            if holding.security and holding.security.asset_class_id:
                value_in_base = holding.market_value_in_base_currency(exchange_rates)
                asset_class_id = holding.security.asset_class_id
                allocation_by_id[asset_class_id] = allocation_by_id.get(asset_class_id, 0) + value_in_base
                total_portfolio += value_in_base
    
    # Calculate percentages
    if total_portfolio > 0:
        for asset_class_id, value in allocation_by_id.items():
            allocation_pct_by_id[asset_class_id] = (value / total_portfolio) * 100
    
    # Get targets
    targets = Target.query.filter_by(user_id=current_user.id).all()
    
    # Build comparison data
    comparison_data = []
    
    for target in targets:
        current_value = allocation_by_id.get(target.asset_class_id, 0)
        current_pct = allocation_pct_by_id.get(target.asset_class_id, 0)
        diff = current_pct - target.target_percentage
        
        comparison_data.append({
            'asset_class_name': target.asset_class.name,
            'current_value': current_value,
            'current_pct': current_pct,
            'target_pct': target.target_percentage,
            'difference': diff
        })
    
    # Sort by current percentage descending
    comparison_data.sort(key=lambda x: x['current_pct'], reverse=True)
    
    # Add securities for dropdown
    securities = Security.query.all()
    
    return render_template('rebalance.html',
                         transactions=transactions,
                         securities=securities,
                         comparison_data=comparison_data,
                         total_portfolio=total_portfolio,
                         base_currency=current_user.base_currency,
                         accounts=current_user.accounts,
                         balanced_threshold=current_user.balanced_threshold)


@app.route('/rebalance/details/<int:asset_class_id>')
@login_required
def rebalance_details(asset_class_id):
    '''Show detailed security-level recommendations for an asset class'''
    asset_class = AssetClass.query.get_or_404(asset_class_id)
    exchange_rates = get_exchange_rates(current_user)
    
    # Get all securities in this asset class
    securities = Security.query.filter_by(asset_class_id=asset_class_id).all()
    
    # Get user's preferences for these securities
    prefs = {
        pref.security_id: pref 
        for pref in SecurityPreference.query.filter_by(user_id=current_user.id).all()
    }
    
    # For each security, determine which accounts it can be held in
    security_restrictions = []
    for security in securities:
        pref = prefs.get(security.id)
        accounts = Account.query.filter_by(user_id=current_user.id).all()
        
        if not pref or pref.restriction_type == 'unrestricted':
            # Can be in any account
            allowed_accounts = accounts
            priority_accounts = []
        
        elif pref.restriction_type == 'restricted_to_accounts':
            # Only specific accounts allowed
            allowed_ids = pref.account_config.get('allowed', [])
            allowed_accounts = [a for a in accounts if a.id in allowed_ids]
            priority_accounts = []
        
        elif pref.restriction_type == 'prioritized_accounts':
            # All accounts allowed, but prioritized
            allowed_accounts = accounts
            priority_config = pref.account_config or {}
            priority_accounts = [
                {
                    'level': 1,
                    'accounts': [a for a in accounts if a.id in priority_config.get('priority_1', [])]
                },
                {
                    'level': 2,
                    'accounts': [a for a in accounts if a.id in priority_config.get('priority_2', [])]
                },
                {
                    'level': 3,
                    'accounts': [a for a in accounts if a.id in priority_config.get('priority_3', [])]
                }
            ]
        else:
            allowed_accounts = accounts
            priority_accounts = []
        
        security_restrictions.append({
            'security': security,
            'restriction_type': pref.restriction_type if pref else 'unrestricted',
            'allowed_accounts': allowed_accounts,
            'priority_accounts': priority_accounts,
            'notes': pref.notes if pref else None
        })
    
    return render_template('rebalance_details.html',
                         asset_class=asset_class,
                         security_restrictions=security_restrictions,
                         base_currency=current_user.base_currency)


@app.route('/rebalance/generate', methods=['POST'])
@login_required
def generate_rebalance():
    '''Generate fresh rebalance transactions'''
    try:
        transactions = generate_rebalance_transactions(current_user)
        flash(f'Generated {len(transactions)} rebalance transactions', 'success')
    except Exception as e:
        flash(f'Error generating rebalance plan: {e}', 'error')
    
    return redirect(url_for('rebalance'))


@app.route('/rebalance/execute/<int:transaction_id>', methods=['POST'])
@login_required
def execute_rebalance_transaction(transaction_id):
    '''Execute a single rebalance transaction and refresh the plan'''
    txn = RebalanceTransaction.query.get_or_404(transaction_id)
    
    if txn.user_id != current_user.id:
        flash('Unauthorized', 'error')
        return redirect(url_for('rebalance'))
    
    if txn.requires_user_selection:
        # User needs to select security first
        security_id = request.form.get('security_id')
        if not security_id:
            flash('Please select a security', 'error')
            return redirect(url_for('rebalance'))
        
        txn.security_id = int(security_id)
        security = Security.query.get(security_id)
        # Update price and quantity based on selection
        # (you'd need current price here)
    
    try:
        # Update holdings
        holding = Holding.query.filter_by(
            account_id=txn.account_id,
            security_id=txn.security_id
        ).first()
        
        if txn.action == 'SELL':
            if holding:
                holding.quantity -= txn.quantity
                if holding.quantity <= 0:
                    db.session.delete(holding)
        else:  # BUY
            if holding:
                holding.quantity += txn.quantity
            else:
                holding = Holding(
                    account_id=txn.account_id,
                    security_id=txn.security_id,
                    quantity=txn.quantity,
                    price=txn.price
                )
                db.session.add(holding)
        
        # Update cash balance
        account = Account.query.get(txn.account_id)
        if txn.action == 'SELL':
            account.cash_balance += txn.amount
        else:  # BUY
            account.cash_balance -= txn.amount
        
        # Mark as executed
        txn.executed = True
        txn.executed_at = datetime.utcnow()
        db.session.commit()
        
        # Regenerate plan
        generate_rebalance_transactions(current_user)
        
        flash(f'Executed: {txn.action} {txn.quantity:.2f} of {txn.security.ticker}', 'success')
        
    except Exception as e:
        db.session.rollback()
        flash(f'Error executing transaction: {e}', 'error')
    
    return redirect(url_for('rebalance'))


@app.route('/account/<int:account_id>/cash', methods=['POST'])
@login_required
def update_cash_balance(account_id):
    '''Update cash balance for an account'''
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


@app.route('/settings')
@login_required
def settings():
    '''User settings page'''
    return render_template('settings.html', user=current_user)


@app.route('/settings/update', methods=['POST'])
@login_required
def update_settings():
    '''Update user settings'''
    base_currency = request.form.get('base_currency')
    price_sheet_id = request.form.get('price_sheet_id')
    trading_costs_enabled = request.form.get('trading_costs_enabled') == 'on'
    
    if base_currency in ['CAD', 'USD']:
        current_user.base_currency = base_currency
    else:
        flash('Invalid currency selection', 'error')
        return redirect(url_for('settings'))
    
    if price_sheet_id:
        current_user.price_sheet_id = price_sheet_id.strip()
    
    current_user.trading_costs_enabled = trading_costs_enabled
    current_user.balanced_threshold = float(request.form.get('balanced_threshold', 0.5))
    
    db.session.commit()
    flash('Settings updated successfully', 'success')
    return redirect(url_for('settings'))


@app.route('/exchange-rates')
@login_required
def exchange_rates_view():
    '''View and manage exchange rates'''
    rates = ExchangeRate.query.order_by(ExchangeRate.date.desc()).limit(50).all()
    return render_template('exchange_rates.html', rates=rates)


@app.route('/exchange-rates/update', methods=['POST'])
@login_required
def update_exchange_rates():
    '''Fetch latest exchange rates'''
    currencies = ['CAD', 'USD']
    updated = 0
    
    for from_curr in currencies:
        for to_curr in currencies:
            if from_curr != to_curr:
                rate = fetch_exchange_rate(from_curr, to_curr)
                if rate:
                    updated += 1
    
    flash(f'Updated {updated} exchange rates', 'success')
    return redirect(url_for('exchange_rates_view'))


@app.route('/exchange-rates/add', methods=['POST'])
@login_required
def add_exchange_rate():
    '''Manually add exchange rate'''
    from_currency = request.form.get('from_currency')
    to_currency = request.form.get('to_currency')
    rate = request.form.get('rate')
    
    if not all([from_currency, to_currency, rate]):
        flash('All fields are required', 'error')
        return redirect(url_for('exchange_rates_view'))
    
    exchange_rate = ExchangeRate(
        from_currency=from_currency,
        to_currency=to_currency,
        rate=float(rate),
        source='manual'
    )
    
    db.session.add(exchange_rate)
    db.session.commit()
    
    flash('Exchange rate added successfully', 'success')
    return redirect(url_for('exchange_rates_view'))


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/api/holdings', methods=['GET'])
@login_required
def api_get_holdings():
    '''API endpoint to get all holdings'''
    accounts = Account.query.filter_by(user_id=current_user.id).all()
    exchange_rates = get_exchange_rates(current_user)
    data = []
    
    for account in accounts:
        for holding in account.holdings:
            data.append({
                'account': account.name,
                'account_type': account.account_type,
                'ticker': holding.security.ticker,
                'name': holding.name,
                'quantity': holding.quantity,
                'price': holding.price,
                'currency': holding.currency,
                'value': holding.market_value,
                'value_base_currency': holding.market_value_in_base_currency(exchange_rates),
                'asset_class': holding.asset_class.name,
                'is_public': holding.is_public
            })
    
    return jsonify(data)


@app.route('/api/portfolio/summary', methods=['GET'])
@login_required
def api_portfolio_summary():
    '''API endpoint for portfolio summary'''
    exchange_rates = get_exchange_rates(current_user)
    allocation, allocation_pct, total_value = calculate_portfolio_allocation(current_user, exchange_rates)
    
    return jsonify({
        'total_value': total_value,
        'base_currency': current_user.base_currency,
        'num_accounts': len(current_user.accounts),
        'asset_allocation': dict(allocation),
        'asset_allocation_pct': dict(allocation_pct)
    })


# ============================================================================
# ERROR HANDLERS
# ============================================================================

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


# ============================================================================
# INITIALIZATION
# ============================================================================

# Initialize database tables on startup
try:
    with app.app_context():
        inspector = inspect(db.engine)
        if not inspector.get_table_names():
            db.create_all()
            print("✓ Database tables created successfully")
except Exception as e:
    print(f"⚠ Warning: Could not create database tables: {e}")


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 
