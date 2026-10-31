from flask_login import UserMixin
from datetime import datetime
from extensions import db

class User(UserMixin, db.Model):
    '''User model for authentication'''
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    google_id = db.Column(db.String(255), unique=True, nullable=False, index=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    name = db.Column(db.String(255))
    picture = db.Column(db.String(512))
    base_currency = db.Column(db.String(3), default='CAD')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    trading_costs_enabled = db.Column(db.Boolean, default=False)   # legacy, unused by the optimiser
    balanced_threshold = db.Column(db.Float, default=0.5)
    precision_rebalancing = db.Column(db.Boolean, default=True)   # = fractional final buy per account
    trade_cost = db.Column(db.Float, default=0.0)                 # per trade, in base currency
    taxable_sell_mode = db.Column(db.String(10), default='ask')   # 'never' | 'ask' | 'allow'
    last_plan_summary = db.Column(db.Text)                        # JSON from rebalancer.generate_plan

    # OAuth token storage
    google_token = db.Column(db.Text)  # JSON string of the token dict
    
    # Optional: store the user's price sheet ID
    price_sheet_id = db.Column(db.String(255))  # e.g., "1abc...xyz"
 
    # Relationships
    portfolios = db.relationship('Portfolio', back_populates='user', lazy=True, cascade='all, delete-orphan',
                                 foreign_keys='Portfolio.user_id', order_by='Portfolio.id')
    targets = db.relationship('Target', back_populates='user', lazy=True, cascade='all, delete-orphan')
    accounts = db.relationship('Account', back_populates='user', lazy=True, cascade='all, delete-orphan')
    security_preferences = db.relationship('SecurityPreference', back_populates='user', lazy=True, cascade='all, delete-orphan') 
    
    def __repr__(self):
        return f'<User {self.email}>'


class Portfolio(db.Model):
    '''A self-contained portfolio: its own accounts, securities, asset classes, targets,
    holdings/ACB, plan and rebalancing settings. One login can manage several (e.g. your
    own and your spouse's); nothing is shared between them except the price feed.

    The rebalancing settings below used to live on User; the User columns remain only
    so old databases can be migrated.'''
    __tablename__ = 'portfolios'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    name = db.Column(db.String(100), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    base_currency = db.Column(db.String(3), default='CAD')
    balanced_threshold = db.Column(db.Float, default=0.5)
    precision_rebalancing = db.Column(db.Boolean, default=True)   # = fractional final buy per account
    trade_cost = db.Column(db.Float, default=0.0)                 # per trade, in base currency
    taxable_sell_mode = db.Column(db.String(10), default='ask')   # 'never' | 'ask' | 'allow'
    last_plan_summary = db.Column(db.Text)                        # JSON from rebalancer.generate_plan
    # Optional: also respect the superficial-loss rule against trades recorded in this
    # other portfolio (e.g. your spouse's – an affiliated person). NULL = off (default).
    sl_link_portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolios.id'), nullable=True)

    user = db.relationship('User', back_populates='portfolios', foreign_keys=[user_id])
    sl_link_portfolio = db.relationship('Portfolio', remote_side=[id], foreign_keys=[sl_link_portfolio_id])
    accounts = db.relationship('Account', back_populates='portfolio', lazy=True, cascade='all, delete-orphan')
    targets = db.relationship('Target', back_populates='portfolio', lazy=True, cascade='all, delete-orphan')
    securities = db.relationship('Security', back_populates='portfolio', lazy=True, cascade='all, delete-orphan')
    asset_classes = db.relationship('AssetClass', back_populates='portfolio', lazy=True, cascade='all, delete-orphan')
    security_preferences = db.relationship('SecurityPreference', back_populates='portfolio', lazy=True,
                                           cascade='all, delete-orphan')

    def __repr__(self):
        return f'<Portfolio {self.id} {self.name}>'


class Account(db.Model):
    '''Investment account model'''
    __tablename__ = 'accounts'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolios.id'), nullable=False, index=True)
    name = db.Column(db.String(255), nullable=False)
    account_type = db.Column(db.String(50))  # 'RRSP', 'TFSA', 'Non-registered', etc.
    currency = db.Column(db.String(3), default='CAD')  # Account currency
    is_registered = db.Column(db.Boolean, default=False)  # Tax status
    priority = db.Column(db.Integer, default=0)  # For rebalancing order
    notes = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    cash_balance = db.Column(db.Float, default=0.0)  # In account's currency
    
    # Relationships
    holdings = db.relationship('Holding', back_populates='account', lazy=True, cascade='all, delete-orphan')
    user = db.relationship('User', back_populates='accounts')
    portfolio = db.relationship('Portfolio', back_populates='accounts')
    
    def total_value_in_base_currency(self, exchange_rates):
        '''Calculate total account value in base currency'''
        total = 0
        for holding in self.holdings:
            total += holding.market_value_in_base_currency(exchange_rates)
        return total

    
    def __repr__(self):
        return f'<Account {self.name}>'


class Holding(db.Model):
    '''Individual security holding'''
    __tablename__ = 'holdings'
    
    id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False, index=True)

    # NEW line: link each holding directly to its Security entry
    security_id = db.Column(db.Integer, db.ForeignKey("securities.id"), nullable=False, index=True)

    quantity = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=False)
    # Book value (adjusted cost base): TOTAL for the position, in the SECURITY's
    # currency (same as price). Gains are estimated by converting at spot, so for
    # USD holdings the FX component of the gain is approximate.
    # Only matters for non-registered accounts; NULL = unknown (treated as all gain).
    book_value = db.Column(db.Float, nullable=True)
    notes = db.Column(db.Text)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    account = db.relationship("Account", back_populates="holdings")
    security = db.relationship("Security", back_populates="holdings")
    
    @property
    def market_value(self):
        '''Market value in security's currency'''
        return self.quantity * self.price
    
    def market_value_in_base_currency(self, exchange_rates):
        value = self.market_value
        sec_currency = self.security.currency  # single source of truth
        base_currency = self.account.portfolio.base_currency
        if sec_currency != base_currency:
            rate = exchange_rates.get(f"{sec_currency}_TO_{base_currency}", 1.0)
            value *= rate
        return value

    @property
    def ticker(self):
        return self.security.ticker
    
    @property
    def currency(self):
        return self.security.currency
    
    @property
    def asset_class_id(self):
        return self.security.asset_class_id
    
    def __repr__(self):
        return f'<Holding {self.security.ticker}: {self.quantity} @ ${self.price} {self.security.currency}>'


class Target(db.Model):
    '''Target allocation for asset classes'''
    __tablename__ = 'targets'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolios.id'), nullable=False, index=True)
    asset_class_id = db.Column(db.Integer, db.ForeignKey('asset_classes.id'), nullable=False)
    asset_class = db.relationship('AssetClass', back_populates='targets')
    portfolio = db.relationship('Portfolio', back_populates='targets')
    target_percentage = db.Column(db.Float, nullable=False)
    
    user = db.relationship('User', back_populates='targets')
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f'<Target {self.asset_class}: {self.target_percentage}%>'


class ExchangeRate(db.Model):
    '''Store exchange rates for currency conversion'''
    __tablename__ = 'exchange_rates'
    
    id = db.Column(db.Integer, primary_key=True)
    from_currency = db.Column(db.String(3), nullable=False, index=True)
    to_currency = db.Column(db.String(3), nullable=False, index=True)
    rate = db.Column(db.Float, nullable=False)
    date = db.Column(db.DateTime, default=datetime.utcnow, index=True)
    source = db.Column(db.String(50))  # 'manual', 'api', 'yfinance'
    
    def __repr__(self):
        return f'<ExchangeRate {self.from_currency}/{self.to_currency}: {self.rate}>'

class RebalanceTransaction(db.Model):
    '''Recommended transactions for rebalancing'''
    __tablename__ = 'rebalance_transactions'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolios.id'), nullable=False, index=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False)
    security_id = db.Column(db.Integer, db.ForeignKey('securities.id'), nullable=True)
    
    action = db.Column(db.String(10))  # 'BUY' or 'SELL'
    quantity = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=False)
    amount = db.Column(db.Float, nullable=False)  # Total dollar amount
    currency = db.Column(db.String(3))
    
    is_final_trade = db.Column(db.Boolean, default=False)  # Use fractional shares to zero cash
    requires_user_selection = db.Column(db.Boolean, default=False)  # Multiple securities available
    available_securities = db.Column(db.JSON, nullable=True)  # List of security IDs if user needs to choose
    
    realized_gain = db.Column(db.Float, nullable=True)  # SELLs in taxable accounts, base currency

    execution_order = db.Column(db.Integer)  # Sequence number
    executed = db.Column(db.Boolean, default=False)
    executed_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    account = db.relationship('Account')
    user = db.relationship('User')
    security = db.relationship('Security')
    
    def __repr__(self):
        return f'<Transaction {self.action} {self.quantity} of {self.security.ticker if self.security else "?"} @ ${self.price}>'


class PendingRedemption(db.Model):
    '''A redemption request filed for an illiquid holding, paid out months later.
    While pending, the units are left out of rebalancing (treated as already gone).'''
    __tablename__ = 'pending_redemptions'

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolios.id'), nullable=False, index=True)
    account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=False)
    security_id = db.Column(db.Integer, db.ForeignKey('securities.id'), nullable=False)
    quantity = db.Column(db.Float, nullable=False)
    requested_on = db.Column(db.Date, default=lambda: datetime.utcnow().date())
    expected_on = db.Column(db.Date, nullable=True)
    status = db.Column(db.String(10), default='pending')    # pending | received | cancelled
    received_on = db.Column(db.Date, nullable=True)
    proceeds = db.Column(db.Float, nullable=True)           # security currency

    account = db.relationship('Account')
    security = db.relationship('Security')


class AssetClass(db.Model):
    __tablename__ = 'asset_classes'
    __table_args__ = (db.UniqueConstraint('portfolio_id', 'name', name='uq_asset_classes_portfolio_name'),)
    id = db.Column(db.Integer, primary_key=True)
    portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolios.id'), nullable=False, index=True)
    name = db.Column(db.String(100), nullable=False)          # unique within a portfolio
    portfolio = db.relationship('Portfolio', back_populates='asset_classes')

    # One-to-many link: an asset class has many securities
    securities = db.relationship('Security', back_populates='asset_class', cascade='all, delete-orphan')

    targets = db.relationship('Target', back_populates='asset_class', cascade='all, delete-orphan')


# ---------- Security / SecurityPreference Models ---------- #
class Security(db.Model):
    __tablename__ = 'securities'
    __table_args__ = (db.UniqueConstraint('portfolio_id', 'ticker', name='uq_securities_portfolio_ticker'),)
    
    id = db.Column(db.Integer, primary_key=True)
    portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolios.id'), nullable=False, index=True)
    ticker = db.Column(db.String(20), nullable=False)       # e.g. VTI, MUB – unique within a portfolio
    name = db.Column(db.String(120))
    asset_class_id = db.Column(db.Integer, db.ForeignKey('asset_classes.id'), nullable=False)

    currency = db.Column(db.String(3), default='CAD')
    is_public = db.Column(db.Boolean, default=True)
    auto_update_price = db.Column(db.Boolean, default=True)
    is_illiquid = db.Column(db.Boolean, default=False)  # can't be sold on demand (private funds etc.)
    last_price = db.Column(db.Float, nullable=True)   # THE price (security currency); holdings mirror it

    # Relationship links
    portfolio = db.relationship('Portfolio', back_populates='securities')
    asset_class = db.relationship('AssetClass', back_populates='securities')
    preferences = db.relationship('SecurityPreference', back_populates='security', cascade='all, delete-orphan')

    # Optional if you want holdings to reference this security directly
    holdings = db.relationship('Holding', back_populates='security', cascade='all, delete-orphan')


class SecurityPreference(db.Model):
    __tablename__ = 'security_preferences'

    id = db.Column(db.Integer, primary_key=True)
    security_id = db.Column(db.Integer, db.ForeignKey('securities.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    portfolio_id = db.Column(db.Integer, db.ForeignKey('portfolios.id'), nullable=False, index=True)

    restriction_type = db.Column(
        db.String(30),
        nullable=False,
        default='unrestricted'
    )

    # Store account preferences as JSON for flexibility
    # Format examples:
    # Restricted: {"allowed": [1, 3, 5]}
    # Prioritized: {"priority_1": [1, 2], "priority_2": [3, 4], "priority_3": [5]}
    account_config = db.Column(db.JSON, nullable=True)

    notes = db.Column(db.String(255), nullable=True)

    # Relationship links
    security = db.relationship('Security', back_populates='preferences')
    user = db.relationship('User', back_populates='security_preferences')
    portfolio = db.relationship('Portfolio', back_populates='security_preferences')
