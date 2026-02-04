# migrate_holdings.py
from app import app, db
from sqlalchemy import text

with app.app_context():
    try:
        # SQLite doesn't support DROP COLUMN, so recreate the table
        db.session.execute(text('''
            CREATE TABLE holdings_new (
                id INTEGER PRIMARY KEY,
                account_id INTEGER NOT NULL,
                security_id INTEGER NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                notes TEXT,
                updated_at DATETIME,
                FOREIGN KEY (account_id) REFERENCES accounts(id),
                FOREIGN KEY (security_id) REFERENCES securities(id)
            )
        '''))
        
        # Copy data
        db.session.execute(text('''
            INSERT INTO holdings_new (id, account_id, security_id, quantity, price, notes, updated_at)
            SELECT id, account_id, security_id, quantity, price, notes, updated_at
            FROM holdings
        '''))
        
        # Swap tables
        db.session.execute(text('DROP TABLE holdings'))
        db.session.execute(text('ALTER TABLE holdings_new RENAME TO holdings'))
        
        # Recreate indexes
        db.session.execute(text('CREATE INDEX ix_holdings_account_id ON holdings (account_id)'))
        db.session.execute(text('CREATE INDEX ix_holdings_security_id ON holdings (security_id)'))
        
        db.session.commit()
        
        print('✓ Migration complete - redundant columns removed')
    except Exception as e:
        db.session.rollback()
        print(f'✗ Migration failed: {e}')
        raise

