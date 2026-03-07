from app import app, db
from sqlalchemy import text

with app.app_context():
    try:
        # Try to add the column
        with db.engine.connect() as conn:
            conn.execute(text("ALTER TABLE user ADD COLUMN precision_rebalancing BOOLEAN DEFAULT 1"))
            conn.commit()
        print("✓ Added precision_rebalancing column")
    except Exception as e:
        if "duplicate column" in str(e).lower() or "already exists" in str(e).lower():
            print("✓ Column already exists, skipping")
        else:
            print(f"Error: {e}")
            raise
