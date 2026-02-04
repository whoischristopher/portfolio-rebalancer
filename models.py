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
    trading_costs_enabled = db.Column(db.Boolean, default=False)
    balanced_threshold = db.Column(db.Float, default=0.5)

    # OAuth token storage
    google_token = db.Column(db.Text)  # JSON string of the token dict
    
    # Optional: store the user's price sheet ID
    price_sheet_id = db.Column(db.String(255))  # e.g., "1abc...xyz"
 
    # Relationships
    targets = db.relationship('Target', back_populates='user', lazy=True, cascade='all, delete-orphan')
    accounts = db.relationship('Account', back_populates='user', lazy=True, cascade='all, delete-orphan')
    security_preferences = db.relationship('SecurityPreference', back_populates='user', lazy=True, cascade='all, delete-orphan') 
    
    def __repr__(self):
        return f'<User {self.email}>'


class Account(db.Model):
    '''Investment account model'''
    __tablename__ = 'accounts'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
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
        '''Convert market value to base currency'''
        value = self.market_value
        if self.security.currency != self.account.user.base_currency:
            rate = exchange_rates.get(f"{self.currency}_TO_{self.account.user.base_currency}", 1.0)
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
    asset_class_id = db.Column(db.Integer, db.ForeignKey('asset_classes.id'), nullable=False)
    asset_class = db.relationship('AssetClass', back_populates='targets')
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
    
    execution_order = db.Column(db.Integer)  # Sequence number
    executed = db.Column(db.Boolean, default=False)
    executed_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    account = db.relationship('Account')
    user = db.relationship('User')
    security = db.relationship('Security')
    
    def __repr__(self):
        return f'<Transaction {self.action} {self.quantity} of {self.security.ticker if self.security else "?"} @ ${self.price}>'


class AssetClass(db.Model):
    __tablename__ = 'asset_classes'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)

    # One-to-many link: an asset class has many securities
    securities = db.relationship('Security', back_populates='asset_class', cascade='all, delete-orphan')

    targets = db.relationship('Target', back_populates='asset_class', cascade='all, delete-orphan')


# ---------- Security / SecurityPreference Models ---------- #
class Security(db.Model):
    __tablename__ = 'securities'
    
    id = db.Column(db.Integer, primary_key=True)
    ticker = db.Column(db.String(20), unique=True, nullable=False)       # e.g. VTI, MUB
    name = db.Column(db.String(120))
    asset_class_id = db.Column(db.Integer, db.ForeignKey('asset_classes.id'), nullable=False)

    currency = db.Column(db.String(3), default='CAD')
    is_public = db.Column(db.Boolean, default=True)
    auto_update_price = db.Column(db.Boolean, default=True)

    # Relationship links
    asset_class = db.relationship('AssetClass', back_populates='securities')
    preferences = db.relationship('SecurityPreference', back_populates='security', cascade='all, delete-orphan')

    # Optional if you want holdings to reference this security directly
    holdings = db.relationship('Holding', back_populates='security', cascade='all, delete-orphan')


class SecurityPreference(db.Model):
    __tablename__ = 'security_preferences'

    id = db.Column(db.Integer, primary_key=True)
    security_id = db.Column(db.Integer, db.ForeignKey('securities.id'), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)

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
