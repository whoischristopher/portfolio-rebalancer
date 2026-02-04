# migrate_rebalance.py
from app import app, db
from sqlalchemy import text

with app.app_context():
    try:
        # Add cash_balance to accounts
        db.session.execute(text('ALTER TABLE accounts ADD COLUMN cash_balance REAL DEFAULT 0.0'))
        
        # Add trading_costs_enabled to users
        db.session.execute(text('ALTER TABLE users ADD COLUMN trading_costs_enabled BOOLEAN DEFAULT 0'))
        
        # Recreate rebalance_transactions with new schema
        db.session.execute(text('DROP TABLE IF EXISTS rebalance_transactions'))
        db.session.execute(text('''
            CREATE TABLE rebalance_transactions (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                security_id INTEGER,
                action VARCHAR(10),
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                amount REAL NOT NULL,
                currency VARCHAR(3),
                is_final_trade BOOLEAN DEFAULT 0,
                requires_user_selection BOOLEAN DEFAULT 0,
                available_securities TEXT,
                execution_order INTEGER,
                executed BOOLEAN DEFAULT 0,
                executed_at DATETIME,
                created_at DATETIME,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (account_id) REFERENCES accounts(id),
                FOREIGN KEY (security_id) REFERENCES securities(id)
            )
        '''))
        
        db.session.commit()
        print('✓ Migration complete')
    except Exception as e:
        db.session.rollback()
        print(f'✗ Migration failed: {e}')
        raise

