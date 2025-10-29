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
    base_currency = db.Column(db.String(3), default='CAD')  # User's base currency
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    targets = db.relationship('Target', back_populates='user', lazy=True, cascade='all, delete-orphan')
    accounts = db.relationship('Account', back_populates='user', lazy=True, cascade='all, delete-orphan')
    preferences = db.relationship('AssetClassPreference', back_populates='user', lazy=True, cascade='all, delete-orphan')
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
    
    # Relationships
    holdings = db.relationship('Holding', back_populates='account', lazy=True, cascade='all, delete-orphan')
    user = db.relationship('User', back_populates='accounts')
    
    def total_value_in_base_currency(self, exchange_rate=1.0):
        '''Calculate total account value in base currency'''
        total = sum(holding.market_value for holding in self.holdings)
        if self.currency != self.user.base_currency:
            total *= exchange_rate
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

    ticker = db.Column(db.String(20), nullable=False)
    name = db.Column(db.String(255))  # Full security name
    quantity = db.Column(db.Float, nullable=False)
    price = db.Column(db.Float, nullable=False)
    currency = db.Column(db.String(3), default='CAD')  # Security currency
    asset_class_id = db.Column(db.Integer, db.ForeignKey('asset_classes.id'), index=True)
    asset_class = db.relationship('AssetClass', back_populates='holdings')
    is_public = db.Column(db.Boolean, default=True)  # Public vs Private security
    auto_update_price = db.Column(db.Boolean, default=True)  # Use yFinance to update
    last_price_update = db.Column(db.DateTime)
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
        if self.currency != self.account.user.base_currency:
            rate = exchange_rates.get(f"{self.currency}_TO_{self.account.user.base_currency}", 1.0)
            value *= rate
        return value
    
    def __repr__(self):
        return f'<Holding {self.ticker}: {self.quantity} @ ${self.price} {self.currency}>'


class Target(db.Model):
    '''Target allocation for asset classes'''
    __tablename__ = 'targets'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    asset_class_id = db.Column(db.Integer, db.ForeignKey('asset_classes.id'), nullable=False)
    asset_class = db.relationship('AssetClass', back_populates='targets')
    target_percentage = db.Column(db.Float, nullable=False)
    
    # Restrictions
    allowed_in_registered = db.Column(db.Boolean, default=True)
    allowed_in_nonregistered = db.Column(db.Boolean, default=True)
    preferred_account_type = db.Column(db.String(50))  # Preferred account type for this asset class

    user = db.relationship('User', back_populates='targets')
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f'<Target {self.asset_class}: {self.target_percentage}%>'


class AssetClassPreference(db.Model):
    '''Preferences for where to hold specific asset classes'''
    __tablename__ = 'asset_class_preferences'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False, index=True)
    asset_class_id = db.Column(db.Integer, db.ForeignKey('asset_classes.id'), nullable=False)
    asset_class = db.relationship('AssetClass')

    preferred_account_id = db.Column(db.Integer, db.ForeignKey('accounts.id'), nullable=True)
   
    user = db.relationship('User', back_populates='preferences')
 
    # Restrictions
    only_in_registered = db.Column(db.Boolean, default=False)
    only_in_nonregistered = db.Column(db.Boolean, default=False)
    avoid_account_types = db.Column(db.String(255))  # Comma-separated list
    
    notes = db.Column(db.Text)
    
    preferred_account = db.relationship('Account', foreign_keys=[preferred_account_id])
    
    def __repr__(self):
        return f'<Preference {self.asset_class}>'


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
    asset_class_id = db.Column(db.Integer, db.ForeignKey('asset_classes.id'), nullable=False)
    asset_class = db.relationship('AssetClass')

    action = db.Column(db.String(10))  # 'BUY' or 'SELL'
    amount = db.Column(db.Float, nullable=False)  # In base currency
    ticker_suggestion = db.Column(db.String(20))  # Suggested ticker to trade
    executed = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    account = db.relationship('Account')
    user = db.relationship('User')
    
    def __repr__(self):
        return f'<Transaction {self.action} ${self.amount} of {self.asset_class}>'

# ---------- Asset class table remains unchanged ---------- #
class AssetClass(db.Model):
    __tablename__ = 'asset_classes'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)

    # One-to-many link: an asset class has many securities
    securities = db.relationship('Security', back_populates='asset_class', cascade='all, delete-orphan')

    holdings = db.relationship('Holding', back_populates='asset_class', cascade='all, delete-orphan')
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
