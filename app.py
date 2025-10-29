import os
from flask import Flask, render_template, redirect, url_for, request, jsonify, flash
from flask_login import LoginManager, login_required, current_user
from extensions import db
import yfinance as yf
from datetime import datetime, timedelta
from collections import defaultdict
from sqlalchemy import inspect

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

def get_exchange_rates(user):
    '''Get current exchange rates for user's currencies'''
    rates = {}
    base = user.base_currency
    
    # Get latest rates from database (within last 24 hours)
    recent_rates = ExchangeRate.query.filter(
        ExchangeRate.date >= datetime.utcnow() - timedelta(days=1)
    ).all()
    
    for rate in recent_rates:
        key = f"{rate.from_currency}_TO_{rate.to_currency}"
        rates[key] = rate.rate
    
    # Add default rates if missing
    if 'USD_TO_CAD' not in rates:
        rates['USD_TO_CAD'] = 1.35  # Default fallback
    if 'CAD_TO_USD' not in rates:
        rates['CAD_TO_USD'] = 1 / rates.get('USD_TO_CAD', 1.35)
    
    return rates


def update_prices_from_yfinance(holdings):
    '''Update holding prices using yFinance'''
    updated_count = 0
    
    for holding in holdings:
        if not holding.auto_update_price or not holding.is_public:
            continue
        
        try:
            # Get ticker data
            ticker = yf.Ticker(holding.ticker)
            info = ticker.info
            
            # Get current price
            price = info.get('currentPrice') or info.get('regularMarketPrice')
            
            if price:
                holding.price = float(price)
                holding.last_price_update = datetime.utcnow()
                holding.name = info.get('longName', holding.name)
                updated_count += 1
        
        except Exception as e:
            print(f"Error updating {holding.ticker}: {e}")
            continue
    
    if updated_count > 0:
        db.session.commit()
    
    return updated_count


def fetch_exchange_rate(from_curr, to_curr):
    '''Fetch current exchange rate using yFinance'''
    try:
        if from_curr == to_curr:
            return 1.0
        
        pair = f"{from_curr}{to_curr}=X"
        ticker = yf.Ticker(pair)
        data = ticker.history(period='1d')
        
        if not data.empty:
            rate = float(data['Close'].iloc[-1])
            
            # Save to database
            exchange_rate = ExchangeRate(
                from_currency=from_curr,
                to_currency=to_curr,
                rate=rate,
                source='yfinance'
            )
            db.session.add(exchange_rate)
            db.session.commit()
            
            return rate
    except Exception as e:
        print(f"Error fetching exchange rate {from_curr}/{to_curr}: {e}")
    
    # Fallback to approximate rate
    if from_curr == 'USD' and to_curr == 'CAD':
        return 1.35
    elif from_curr == 'CAD' and to_curr == 'USD':
        return 0.74
    
    return 1.0


def calculate_portfolio_allocation(user, exchange_rates):
    '''Calculate current portfolio allocation by asset class'''
    allocation = defaultdict(float)
    total_value = 0
    
    for account in user.accounts:
        for holding in account.holdings:
            value = holding.market_value_in_base_currency(exchange_rates)
            allocation[holding.asset_class.name] += value
            total_value += value
    
    # Convert to percentages
    allocation_pct = {}
    for asset_class, value in allocation.items():
        allocation_pct[asset_class] = (value / total_value * 100) if total_value > 0 else 0
    
    return allocation, allocation_pct, total_value


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
    ticker = request.form.get('ticker')
    quantity = request.form.get('quantity')
    price = request.form.get('price')
    currency = request.form.get('currency', 'CAD')
    asset_class_id = request.form.get('asset_class_id')
    
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
        currency=security.currency if hasattr(security, 'currency') else 'CAD',
        asset_class_id=security.asset_class_id,
        is_public=security.is_public,
        auto_update_price=security.auto_update_price 
    )
    
    # If public and auto-update enabled, fetch price from yFinance
    if holding.is_public and holding.auto_update and not price:
        try:
            ticker_data = yf.Ticker(security.ticker)
            info = ticker_data.info
            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            if current_price:
                holding.price = float(current_price)
                holding.name = info.get('longName', '')
                holding.last_price_update = datetime.utcnow()
        except Exception as e:
            print(f"Could not fetch price for {security.ticker}: {e}")
    
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
    
    ticker = holding.ticker
    db.session.delete(holding)
    db.session.commit()
    
    flash(f'Holding {ticker} deleted successfully', 'success')
    return redirect(url_for('holdings'))


@app.route('/holdings/update-prices', methods=['POST'])
@login_required
def update_prices():
    '''Update all public holdings prices from yFinance'''
    holdings = Holding.query.join(Account).filter(
        Account.user_id == current_user.id,
        Holding.is_public == True,
        Holding.auto_update_price == True
    ).all()
    
    updated_count = update_prices_from_yfinance(holdings)
    
    flash(f'Updated {updated_count} holdings from market data', 'success')
    return redirect(url_for('holdings'))

@app.route('/asset-classes/manage', methods=['GET', 'POST'])
@login_required
def manage_asset_classes():
    '''Manage asset classes - view, add, delete'''
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'add':
            name = request.form.get('name')
            if name:
                # Check if already exists
                existing = AssetClass.query.filter_by(name=name).first()
                if existing:
                    flash(f'Asset class "{name}" already exists', 'warning')
                else:
                    ac = AssetClass(name=name)
                    db.session.add(ac)
                    db.session.commit()
                    flash(f'Asset class "{name}" added successfully', 'success')
        
        elif action == 'delete':
            ac_id = request.form.get('asset_class_id')
            if ac_id:
                ac = AssetClass.query.get(ac_id)
                if ac:
                    # Check if it's used by any targets, holdings, or securities
                    if ac.targets or ac.holdings or ac.securities:
                        flash(f'Cannot delete "{ac.name}" - it is currently in use', 'error')
                    else:
                        db.session.delete(ac)
                        db.session.commit()
                        flash(f'Asset class "{ac.name}" deleted successfully', 'success')
        
        return redirect(url_for('manage_asset_classes'))
    
    # GET request - show all asset classes
    asset_classes = AssetClass.query.order_by(AssetClass.name.asc()).all()
    return render_template('manage_asset_classes.html', asset_classes=asset_classes)

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
    '''Update target allocations'''
    # Delete existing targets
    Target.query.filter_by(user_id=current_user.id).delete()
    
    target_count = 0
    for key in request.form.keys():
        if key.startswith('asset_class_id_'):
            index = key.split('_')[-1]
            asset_class_id = request.form.get(f'asset_class_id_{index}')
            new_asset_class_name = request.form.get(f'new_asset_class_{index}')
            percentage = request.form.get(f'percentage_{index}')
            
            # Handle new asset class creation
            if asset_class_id == 'new' and new_asset_class_name:
                # Check if asset class already exists
                existing = AssetClass.query.filter_by(name=new_asset_class_name).first()
                if existing:
                    asset_class_id = existing.id
                else:
                    new_ac = AssetClass(name=new_asset_class_name)
                    db.session.add(new_ac)
                    db.session.flush()  # Get the ID
                    asset_class_id = new_ac.id
            
            # Get restrictions
            allowed_registered = request.form.get(f'allowed_registered_{index}') == 'on'
            allowed_nonregistered = request.form.get(f'allowed_nonregistered_{index}') == 'on'
            preferred_account = request.form.get(f'preferred_account_{index}')
            
            if asset_class_id and asset_class_id != 'new' and percentage:
                target = Target(
                    user_id=current_user.id,
                    asset_class_id=int(asset_class_id),
                    target_percentage=float(percentage),
                    allowed_in_registered=allowed_registered,
                    allowed_in_nonregistered=allowed_nonregistered,
                    preferred_account_type=preferred_account if preferred_account else None
                )
                db.session.add(target)
                target_count += 1
    
    db.session.commit()
    flash(f'{target_count} target allocation(s) updated successfully', 'success')
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
    account_id = request.form.get('account_id')
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


@app.route('/save_preferences', methods=['POST'])
@login_required
def save_preferences():
    """Persist security preference selections"""
    for security in Security.query.all():
        restriction = request.form.get(f'restriction_type_{security.id}')
        account_id = request.form.get(f'account_id_{security.id}')

        if restriction:
            # find existing record
            pref = (
                SecurityPreference.query
                .filter_by(security_id=security.id, account_id=account_id)
                .join(Account)
                .filter(Account.user_id == current_user.id)
                .first()
            )

            if pref:
                pref.restriction_type = restriction
            else:
                db.session.add(SecurityPreference(
                    security_id=security.id,
                    account_id=account_id or None,
                    restriction_type=restriction
                ))
    db.session.commit()
    flash('Preferences updated successfully.')
    return redirect(url_for('preferences'))

@app.route('/rebalance')
@login_required
def rebalance():
    '''Calculate rebalance recommendations with account restrictions'''
    accounts = Account.query.filter_by(user_id=current_user.id).all()
    targets = Target.query.filter_by(user_id=current_user.id).all()
    preferences = AssetClassPreference.query.filter_by(user_id=current_user.id).all()
    
    # Get exchange rates
    exchange_rates = get_exchange_rates(current_user)
    
    # Calculate current allocation
    allocation, allocation_pct, total_portfolio = calculate_portfolio_allocation(current_user, exchange_rates)
    
    # Build preference map
    preference_map = {p.asset_class: p for p in preferences}
    
    # Calculate differences and generate recommendations
    rebalance_data = []
    transactions = []
    
    for target in targets:
        current_value = allocation.get(target.asset_class_id, 0)
        current_pct = allocation_pct.get(target.asset_class_id, 0)
        target_value = total_portfolio * target.target_percentage / 100
        difference = target_value - current_value
        
        # Get preference for this asset class
        preference = preference_map.get(target.asset_class_id)
        
        # Determine eligible accounts
        eligible_accounts = []
        for account in accounts:
            # Check registration status restrictions
            if not target.allowed_in_registered and account.is_registered:
                continue
            if not target.allowed_in_nonregistered and not account.is_registered:
                continue
            
            # Check preferences
            if preference:
                if preference.only_in_registered and not account.is_registered:
                    continue
                if preference.only_in_nonregistered and account.is_registered:
                    continue
                if preference.avoid_account_types:
                    avoid_list = [x.strip() for x in preference.avoid_account_types.split(',')]
                    if account.account_type in avoid_list:
                        continue
            
            eligible_accounts.append(account)
        
        # Determine action and preferred account
        action = None
        preferred_account = None
        
        if abs(difference) > 1:  # Threshold: $1
            if difference > 0:
                action = 'BUY'
            else:
                action = 'SELL'
            
            # Find preferred account
            if preference and preference.preferred_account_id:
                preferred_account = next((a for a in eligible_accounts if a.id == preference.preferred_account_id), None)
            
            if not preferred_account and target.preferred_account_type:
                preferred_account = next((a for a in eligible_accounts if a.account_type == target.preferred_account_type), None)
            
            if not preferred_account and eligible_accounts:
                # Use highest priority account
                preferred_account = max(eligible_accounts, key=lambda a: a.priority)
        
        rebalance_data.append({
            'asset_class': target.asset_class.name,
            'current_value': current_value,
            'current_pct': current_pct,
            'target_pct': target.target_percentage,
            'target_value': target_value,
            'difference': difference,
            'action': action,
            'preferred_account': preferred_account,
            'eligible_accounts': eligible_accounts,
            'restrictions': {
                'allowed_registered': target.allowed_in_registered,
                'allowed_nonregistered': target.allowed_in_nonregistered
            }
        })
    
    return render_template('rebalance.html', 
                         rebalance_data=rebalance_data,
                         total_portfolio=total_portfolio,
                         base_currency=current_user.base_currency,
                         accounts=accounts)


@app.route('/rebalance/generate-transactions', methods=['POST'])
@login_required
def generate_rebalance_transactions():
    '''Generate and save rebalance transaction recommendations'''
    # Clear old transactions
    RebalanceTransaction.query.filter_by(user_id=current_user.id, executed=False).delete()
    
    # Get rebalance data from request
    targets = Target.query.filter_by(user_id=current_user.id).all()
    exchange_rates = get_exchange_rates(current_user)
    allocation, allocation_pct, total_portfolio = calculate_portfolio_allocation(current_user, exchange_rates)
    
    transaction_count = 0
    
    for target in targets:
        current_value = allocation.get(target.asset_class_id, 0)
        target_value = total_portfolio * target.target_percentage / 100
        difference = target_value - current_value
        
        if abs(difference) > 1:  # Only if difference > $1
            action = 'BUY' if difference > 0 else 'SELL'
            
            # Find best account (simplified - you can add more logic)
            accounts = Account.query.filter_by(user_id=current_user.id).all()
            best_account = accounts[0] if accounts else None
            
            if best_account:
                transaction = RebalanceTransaction(
                    user_id=current_user.id,
                    account_id=best_account.id,
                    asset_class_id=target.asset_class_id,
                    action=action,
                    amount=abs(difference)
                )
                db.session.add(transaction)
                transaction_count += 1
    
    db.session.commit()
    
    flash(f'Generated {transaction_count} rebalancing transactions', 'success')
    return redirect(url_for('rebalance'))


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
    
    if base_currency in ['CAD', 'USD']:
        current_user.base_currency = base_currency
        db.session.commit()
        flash('Settings updated successfully', 'success')
    else:
        flash('Invalid currency selection', 'error')
    
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
                'ticker': holding.ticker,
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


@app.route('/api/ticker/<ticker>/info', methods=['GET'])
@login_required
def api_ticker_info(ticker):
    '''Get ticker information from yFinance'''
    try:
        ticker_obj = yf.Ticker(ticker)
        info = ticker_obj.info
        
        return jsonify({
            'ticker': ticker,
            'name': info.get('longName', ''),
            'price': info.get('currentPrice') or info.get('regularMarketPrice'),
            'currency': info.get('currency', 'USD'),
            'sector': info.get('sector', ''),
            'industry': info.get('industry', '')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 400


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
#    app.run(host='0.0.0.0', port=5000, debug=os.getenv('FLASK_ENV') != 'production')
    app.run(debug=True, host='0.0.0.0', port=5000) 
