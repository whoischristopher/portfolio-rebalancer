from flask import Flask, request, jsonify
from flask_cors import CORS
import yfinance as yf
from datetime import datetime
import logging
import time
import requests
import sqlite3
import json
import os

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database file location
DB_PATH = os.environ.get('DB_PATH', '/app/data/portfolio.db')

# Create data directory if it doesn't exist
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Create a custom session with headers
import random

# Rotate user agents to avoid detection
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
]

session = requests.Session()
session.headers.update({
    'User-Agent': random.choice(USER_AGENTS),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
})


# Cache for prices
price_cache = {}
CACHE_DURATION = 600

# Rate limiting
last_api_call = 0
MIN_DELAY_BETWEEN_CALLS = 3.0

def init_db():
    """Initialize the database with required tables"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Accounts table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            label TEXT,
            cash REAL DEFAULT 0,
            cash_currency TEXT DEFAULT 'CAD',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Securities table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS securities (
            id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL UNIQUE,
            name TEXT,
            price REAL,
            price_currency TEXT DEFAULT 'USD',
            is_private BOOLEAN DEFAULT 0,
            manual_price REAL,
            manual_price_date TIMESTAMP,
            asset_class TEXT,
            preference_mode TEXT DEFAULT 'none',
            allowed_accounts TEXT,
            account_priority TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Holdings table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            shares REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_id) REFERENCES accounts(id),
            FOREIGN KEY (symbol) REFERENCES securities(symbol),
            UNIQUE(account_id, symbol)
        )
    ''')
    
    # Asset classes table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS asset_classes (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            target_allocation REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Settings table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_PATH}")

# Initialize database on startup
init_db()

def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def detect_currency_from_symbol(symbol):
    """Detect currency based on stock symbol suffix"""
    symbol_upper = symbol.upper()
    if symbol_upper.endswith('.TO') or symbol_upper.endswith('.V'):
        return 'CAD'
    if symbol_upper.endswith('.L'):
        return 'GBP'
    if symbol_upper.endswith('.PA') or symbol_upper.endswith('.AS') or symbol_upper.endswith('.DE'):
        return 'EUR'
    return 'USD'

def get_cached_price(symbol):
    cache_key = symbol.upper()
    if cache_key in price_cache:
        cached_data, timestamp = price_cache[cache_key]
        if time.time() - timestamp < CACHE_DURATION:
            return cached_data
    return None

def set_cached_price(symbol, data):
    price_cache[symbol.upper()] = (data, time.time())

def rate_limited_api_call():
    global last_api_call
    current_time = time.time()
    time_since_last = current_time - last_api_call
    if time_since_last < MIN_DELAY_BETWEEN_CALLS:
        sleep_time = MIN_DELAY_BETWEEN_CALLS - time_since_last
        time.sleep(sleep_time)
    last_api_call = time.time()

# ==================== HEALTH & INFO ====================

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'cache_size': len(price_cache),
        'database': DB_PATH
    })

# ==================== ACCOUNTS ====================

@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    """Get all accounts"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM accounts ORDER BY created_at')
    accounts = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(accounts)

@app.route('/api/accounts', methods=['POST'])
def create_account():
    """Create a new account"""
    data = request.get_json()
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO accounts (id, name, label, cash, cash_currency)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            data['id'],
            data['name'],
            data.get('label', data['name']),
            data.get('cash', 0),
            data.get('cash_currency', 'CAD')
        ))
        conn.commit()
        logger.info(f"Created account: {data['name']}")
        return jsonify({'status': 'success', 'id': data['id']}), 201
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating account: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/accounts/<account_id>', methods=['PUT'])
def update_account(account_id):
    """Update an account"""
    data = request.get_json()
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            UPDATE accounts 
            SET name = ?, label = ?, cash = ?, cash_currency = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            data['name'],
            data.get('label', data['name']),
            data.get('cash', 0),
            data.get('cash_currency', 'CAD'),
            account_id
        ))
        conn.commit()
        logger.info(f"Updated account: {account_id}")
        return jsonify({'status': 'success'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating account: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/accounts/<account_id>', methods=['DELETE'])
def delete_account(account_id):
    """Delete an account"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('DELETE FROM holdings WHERE account_id = ?', (account_id,))
        cursor.execute('DELETE FROM accounts WHERE id = ?', (account_id,))
        conn.commit()
        logger.info(f"Deleted account: {account_id}")
        return jsonify({'status': 'success'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting account: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()
# ==================== SECURITIES ====================

@app.route('/api/securities', methods=['GET'])
def get_securities():
    """Get all securities"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM securities ORDER BY symbol')
    securities = []
    for row in cursor.fetchall():
        sec = dict(row)
        if sec['allowed_accounts']:
            sec['allowed_accounts'] = json.loads(sec['allowed_accounts'])
        else:
            sec['allowed_accounts'] = []
        if sec['account_priority']:
            sec['account_priority'] = json.loads(sec['account_priority'])
        else:
            sec['account_priority'] = {}
        securities.append(sec)
    conn.close()
    return jsonify(securities)

@app.route('/api/securities', methods=['POST'])
def create_security():
    """Create a new security"""
    data = request.get_json()
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO securities 
            (id, symbol, name, price, price_currency, is_private, manual_price, 
             asset_class, preference_mode, allowed_accounts, account_priority)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('id', data['symbol']),
            data['symbol'],
            data.get('name', data['symbol']),
            data.get('price'),
            data.get('price_currency', detect_currency_from_symbol(data['symbol'])),
            data.get('is_private', False),
            data.get('manual_price'),
            data.get('asset_class'),
            data.get('preference_mode', 'none'),
            json.dumps(data.get('allowed_accounts', [])),
            json.dumps(data.get('account_priority', {}))
        ))
        conn.commit()
        logger.info(f"Created security: {data['symbol']}")
        return jsonify({'status': 'success', 'id': data.get('id', data['symbol'])}), 201
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating security: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/securities/<security_id>', methods=['PUT'])
def update_security(security_id):
    """Update a security"""
    data = request.get_json()
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            UPDATE securities 
            SET symbol = ?, name = ?, price = ?, price_currency = ?, is_private = ?,
                manual_price = ?, asset_class = ?, preference_mode = ?,
                allowed_accounts = ?, account_priority = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (
            data['symbol'],
            data.get('name', data['symbol']),
            data.get('price'),
            data.get('price_currency', detect_currency_from_symbol(data['symbol'])),
            data.get('is_private', False),
            data.get('manual_price'),
            data.get('asset_class'),
            data.get('preference_mode', 'none'),
            json.dumps(data.get('allowed_accounts', [])),
            json.dumps(data.get('account_priority', {})),
            security_id
        ))
        conn.commit()
        logger.info(f"Updated security: {security_id}")
        return jsonify({'status': 'success'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating security: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/securities/<security_id>', methods=['DELETE'])
def delete_security(security_id):
    """Delete a security"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT symbol FROM securities WHERE id = ?', (security_id,))
        row = cursor.fetchone()
        if row:
            symbol = row['symbol']
            cursor.execute('DELETE FROM holdings WHERE symbol = ?', (symbol,))
        cursor.execute('DELETE FROM securities WHERE id = ?', (security_id,))
        conn.commit()
        logger.info(f"Deleted security: {security_id}")
        return jsonify({'status': 'success'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting security: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# ==================== HOLDINGS ====================

@app.route('/api/holdings', methods=['GET'])
def get_holdings():
    """Get all holdings"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM holdings ORDER BY account_id, symbol')
    holdings = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(holdings)

@app.route('/api/holdings', methods=['POST'])
def create_holding():
    """Create or update a holding"""
    data = request.get_json()
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO holdings (account_id, symbol, shares)
            VALUES (?, ?, ?)
            ON CONFLICT(account_id, symbol) 
            DO UPDATE SET shares = ?, updated_at = CURRENT_TIMESTAMP
        ''', (
            data['account_id'],
            data['symbol'],
            data['shares'],
            data['shares']
        ))
        conn.commit()
        logger.info(f"Created/Updated holding: {data['symbol']} in {data['account_id']}")
        return jsonify({'status': 'success'}), 201
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating holding: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/holdings/<int:holding_id>', methods=['DELETE'])
def delete_holding(holding_id):
    """Delete a holding"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('DELETE FROM holdings WHERE id = ?', (holding_id,))
        conn.commit()
        logger.info(f"Deleted holding: {holding_id}")
        return jsonify({'status': 'success'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting holding: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# ==================== ASSET CLASSES ====================

@app.route('/api/asset_classes/<asset_class_id>', methods=['PUT'])
def update_asset_class(asset_class_id):
    """Update an asset class"""
    data = request.get_json()
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            UPDATE asset_classes 
            SET name = ?, target_allocation = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (data['name'], data['target_allocation'], asset_class_id))
        conn.commit()
        logger.info(f"Updated asset class: {asset_class_id}")
        return jsonify({'status': 'success'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating asset class: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/asset_classes/<asset_class_id>', methods=['DELETE'])
def delete_asset_class(asset_class_id):
    """Delete an asset class"""
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('DELETE FROM asset_classes WHERE id = ?', (asset_class_id,))
        conn.commit()
        logger.info(f"Deleted asset class: {asset_class_id}")
        return jsonify({'status': 'success'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting asset class: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/asset_classes', methods=['GET'])
def get_asset_classes():
    """Get all asset classes"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM asset_classes ORDER BY name')
    asset_classes = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(asset_classes)

@app.route('/api/asset_classes', methods=['POST'])
def create_asset_class():
    """Create an asset class"""
    data = request.get_json()
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO asset_classes (id, name, target_allocation)
            VALUES (?, ?, ?)
        ''', (data['id'], data['name'], data['target_allocation']))
        conn.commit()
        logger.info(f"Created asset class: {data['name']}")
        return jsonify({'status': 'success', 'id': data['id']}), 201
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating asset class: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# ==================== SETTINGS ====================

@app.route('/api/settings', methods=['GET'])
def get_settings():
    """Get all settings"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT key, value FROM settings')
    settings = {row['key']: row['value'] for row in cursor.fetchall()}
    conn.close()
    return jsonify(settings)

@app.route('/api/settings/<key>', methods=['PUT'])
def update_setting(key):
    """Update a setting"""
    data = request.get_json()
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO settings (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = CURRENT_TIMESTAMP
        ''', (key, data['value'], data['value']))
        conn.commit()
        logger.info(f"Updated setting: {key}")
        return jsonify({'status': 'success'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating setting: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()
# ==================== PRICE LOOKUPS ====================

@app.route('/api/price/<symbol>', methods=['GET'])
def get_price(symbol):
    """Get current price for a symbol"""
    cached = get_cached_price(symbol)
    if cached:
        return jsonify(cached)
    
    try:
        rate_limited_api_call()
        ticker = yf.Ticker(symbol, session=session)
        
        price = None
        currency = detect_currency_from_symbol(symbol)
        name = symbol
        
        try:
            hist = ticker.history(period='5d')
            if not hist.empty:
                price = hist['Close'].iloc[-1]
        except Exception as e:
            logger.warning(f"History failed for {symbol}: {str(e)[:100]}")
        
        if price is None:
            try:
                info = ticker.info
                price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
                if info.get('currency'):
                    currency = info.get('currency')
                name = info.get('shortName', symbol)
            except Exception as e:
                logger.warning(f"Info failed for {symbol}: {str(e)[:100]}")
        
        if price is None:
            return jsonify({'error': f'Price not available for {symbol}'}), 404
        
        result = {
            'symbol': symbol.upper(),
            'price': round(float(price), 2),
            'currency': currency,
            'last_updated': datetime.now().isoformat(),
            'name': name
        }
        
        set_cached_price(symbol, result)
        return jsonify(result)
    
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/exchange_rate/<from_currency>/<to_currency>', methods=['GET'])
def get_exchange_rate(from_currency, to_currency):
    """Get exchange rate"""
    cache_key = f"{from_currency}{to_currency}"
    cached = get_cached_price(cache_key)
    if cached and 'rate' in cached:
        return jsonify(cached)
    
    try:
        rate_limited_api_call()
        symbol = f"{from_currency}{to_currency}=X"
        ticker = yf.Ticker(symbol, session=session)
        hist = ticker.history(period='5d')
        
        if hist.empty:
            return jsonify({'error': 'Exchange rate not available'}), 404
        
        rate = hist['Close'].iloc[-1]
        result = {
            'from': from_currency,
            'to': to_currency,
            'rate': round(float(rate), 4),
            'last_updated': datetime.now().isoformat()
        }
        
        set_cached_price(cache_key, result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Exchange rate error: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ==================== DATA EXPORT/IMPORT ====================

@app.route('/api/export', methods=['GET'])
def export_data():
    """Export all data as JSON"""
    conn = get_db()
    cursor = conn.cursor()
    
    data = {
        'accounts': [dict(row) for row in cursor.execute('SELECT * FROM accounts').fetchall()],
        'securities': [],
        'holdings': [dict(row) for row in cursor.execute('SELECT * FROM holdings').fetchall()],
        'asset_classes': [dict(row) for row in cursor.execute('SELECT * FROM asset_classes').fetchall()],
        'settings': {row['key']: row['value'] for row in cursor.execute('SELECT * FROM settings').fetchall()},
        'exported_at': datetime.now().isoformat()
    }
    
    for row in cursor.execute('SELECT * FROM securities').fetchall():
        sec = dict(row)
        if sec['allowed_accounts']:
            sec['allowed_accounts'] = json.loads(sec['allowed_accounts'])
        if sec['account_priority']:
            sec['account_priority'] = json.loads(sec['account_priority'])
        data['securities'].append(sec)
    
    conn.close()
    return jsonify(data)

@app.route('/api/import', methods=['POST'])
def import_data():
    """Import data from JSON"""
    data = request.get_json()
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        for acc in data.get('accounts', []):
            cursor.execute('''
                INSERT OR REPLACE INTO accounts (id, name, label, cash, cash_currency)
                VALUES (?, ?, ?, ?, ?)
            ''', (acc['id'], acc['name'], acc.get('label'), acc.get('cash', 0), acc.get('cash_currency', 'CAD')))
        
        for sec in data.get('securities', []):
            cursor.execute('''
                INSERT OR REPLACE INTO securities 
                (id, symbol, name, price, price_currency, is_private, manual_price,
                 asset_class, preference_mode, allowed_accounts, account_priority)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                sec.get('id'), sec['symbol'], sec.get('name'), sec.get('price'),
                sec.get('price_currency', 'USD'), sec.get('is_private', False),
                sec.get('manual_price'), sec.get('asset_class'), sec.get('preference_mode', 'none'),
                json.dumps(sec.get('allowed_accounts', [])), json.dumps(sec.get('account_priority', {}))
            ))
        
        for holding in data.get('holdings', []):
            cursor.execute('''
                INSERT OR REPLACE INTO holdings (account_id, symbol, shares)
                VALUES (?, ?, ?)
            ''', (holding['account_id'], holding['symbol'], holding['shares']))
        
        for ac in data.get('asset_classes', []):
            cursor.execute('''
                INSERT OR REPLACE INTO asset_classes (id, name, target_allocation)
                VALUES (?, ?, ?)
            ''', (ac['id'], ac['name'], ac['target_allocation']))
        
        for key, value in data.get('settings', {}).items():
            cursor.execute('''
                INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)
            ''', (key, value))
        
        conn.commit()
        logger.info("Data import successful")
        return jsonify({'status': 'success', 'message': 'Data imported successfully'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Import error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

