CREATE TABLE accounts (
	id INTEGER NOT NULL, 
	user_id INTEGER NOT NULL, 
	name VARCHAR(255) NOT NULL, 
	account_type VARCHAR(50), 
	currency VARCHAR(3), 
	is_registered BOOLEAN, 
	priority INTEGER, 
	notes TEXT, 
	created_at DATETIME, 
	updated_at DATETIME, 
	cash_balance FLOAT, 
	PRIMARY KEY (id), 
	FOREIGN KEY(user_id) REFERENCES users (id)
);
CREATE TABLE asset_classes (
	id INTEGER NOT NULL, 
	name VARCHAR(100) NOT NULL, 
	PRIMARY KEY (id), 
	UNIQUE (name)
);
CREATE TABLE exchange_rates (
	id INTEGER NOT NULL, 
	from_currency VARCHAR(3) NOT NULL, 
	to_currency VARCHAR(3) NOT NULL, 
	rate FLOAT NOT NULL, 
	date DATETIME, 
	source VARCHAR(50), 
	PRIMARY KEY (id)
);
CREATE TABLE holdings (
	id INTEGER NOT NULL, 
	account_id INTEGER NOT NULL, 
	security_id INTEGER NOT NULL, 
	quantity FLOAT NOT NULL, 
	price FLOAT NOT NULL, 
	book_value FLOAT, 
	notes TEXT, 
	updated_at DATETIME, 
	PRIMARY KEY (id), 
	FOREIGN KEY(account_id) REFERENCES accounts (id), 
	FOREIGN KEY(security_id) REFERENCES securities (id)
);
CREATE TABLE pending_redemptions (
	id INTEGER NOT NULL, 
	user_id INTEGER NOT NULL, 
	account_id INTEGER NOT NULL, 
	security_id INTEGER NOT NULL, 
	quantity FLOAT NOT NULL, 
	requested_on DATE, 
	expected_on DATE, 
	status VARCHAR(10), 
	received_on DATE, 
	proceeds FLOAT, 
	PRIMARY KEY (id), 
	FOREIGN KEY(user_id) REFERENCES users (id), 
	FOREIGN KEY(account_id) REFERENCES accounts (id), 
	FOREIGN KEY(security_id) REFERENCES securities (id)
);
CREATE TABLE rebalance_transactions (
	id INTEGER NOT NULL, 
	user_id INTEGER NOT NULL, 
	account_id INTEGER NOT NULL, 
	security_id INTEGER, 
	action VARCHAR(10), 
	quantity FLOAT NOT NULL, 
	price FLOAT NOT NULL, 
	amount FLOAT NOT NULL, 
	currency VARCHAR(3), 
	is_final_trade BOOLEAN, 
	requires_user_selection BOOLEAN, 
	available_securities JSON, 
	realized_gain FLOAT, 
	execution_order INTEGER, 
	executed BOOLEAN, 
	executed_at DATETIME, 
	created_at DATETIME, 
	PRIMARY KEY (id), 
	FOREIGN KEY(user_id) REFERENCES users (id), 
	FOREIGN KEY(account_id) REFERENCES accounts (id), 
	FOREIGN KEY(security_id) REFERENCES securities (id)
);
CREATE TABLE securities (
	id INTEGER NOT NULL, 
	ticker VARCHAR(20) NOT NULL, 
	name VARCHAR(120), 
	asset_class_id INTEGER NOT NULL, 
	currency VARCHAR(3), 
	is_public BOOLEAN, 
	auto_update_price BOOLEAN, 
	is_illiquid BOOLEAN, 
	last_price FLOAT, 
	PRIMARY KEY (id), 
	UNIQUE (ticker), 
	FOREIGN KEY(asset_class_id) REFERENCES asset_classes (id)
);
CREATE TABLE security_preferences (
	id INTEGER NOT NULL, 
	security_id INTEGER NOT NULL, 
	user_id INTEGER NOT NULL, 
	restriction_type VARCHAR(30) NOT NULL, 
	account_config JSON, 
	notes VARCHAR(255), 
	PRIMARY KEY (id), 
	FOREIGN KEY(security_id) REFERENCES securities (id), 
	FOREIGN KEY(user_id) REFERENCES users (id)
);
CREATE TABLE targets (
	id INTEGER NOT NULL, 
	user_id INTEGER NOT NULL, 
	asset_class_id INTEGER NOT NULL, 
	target_percentage FLOAT NOT NULL, 
	created_at DATETIME, 
	updated_at DATETIME, 
	PRIMARY KEY (id), 
	FOREIGN KEY(user_id) REFERENCES users (id), 
	FOREIGN KEY(asset_class_id) REFERENCES asset_classes (id)
);
CREATE TABLE users (
	id INTEGER NOT NULL, 
	google_id VARCHAR(255) NOT NULL, 
	email VARCHAR(255) NOT NULL, 
	name VARCHAR(255), 
	picture VARCHAR(512), 
	base_currency VARCHAR(3), 
	created_at DATETIME, 
	last_login DATETIME, 
	trading_costs_enabled BOOLEAN, 
	balanced_threshold FLOAT, 
	precision_rebalancing BOOLEAN, 
	trade_cost FLOAT, 
	taxable_sell_mode VARCHAR(10), 
	last_plan_summary TEXT, 
	google_token TEXT, 
	price_sheet_id VARCHAR(255), 
	PRIMARY KEY (id)
);
CREATE INDEX ix_accounts_user_id ON accounts (user_id);
CREATE INDEX ix_exchange_rates_date ON exchange_rates (date);
CREATE INDEX ix_exchange_rates_from_currency ON exchange_rates (from_currency);
CREATE INDEX ix_exchange_rates_to_currency ON exchange_rates (to_currency);
CREATE INDEX ix_holdings_account_id ON holdings (account_id);
CREATE INDEX ix_holdings_security_id ON holdings (security_id);
CREATE INDEX ix_pending_redemptions_user_id ON pending_redemptions (user_id);
CREATE INDEX ix_rebalance_transactions_user_id ON rebalance_transactions (user_id);
CREATE INDEX ix_security_preferences_security_id ON security_preferences (security_id);
CREATE INDEX ix_security_preferences_user_id ON security_preferences (user_id);
CREATE INDEX ix_targets_user_id ON targets (user_id);
CREATE UNIQUE INDEX ix_users_email ON users (email);
CREATE UNIQUE INDEX ix_users_google_id ON users (google_id);
