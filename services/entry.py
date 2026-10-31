"""
services/entry.py – Data-entry helpers shared by the Holdings page and the quick-start wizard.

The idea: a holding is entered by *ticker*. If the ticker is already a security in the
portfolio it is used as-is; otherwise the security (and, if needed, its asset class) is
created on the spot from the few extra fields the form shows for a new ticker.
"""
from extensions import db
from models import AssetClass, Holding, Security
from services.prices import normalize_ticker, same_security_elsewhere, set_security_price

# Canadian registered (tax-sheltered) account types. Choosing one of these ticks the
# "registered" box; the box itself stays editable for anything unusual.
REGISTERED_ACCOUNT_TYPES = ('RRSP', 'Spousal RRSP', 'TFSA', 'FHSA', 'RESP', 'RDSP', 'LIRA', 'LIF', 'RRIF')
NON_REGISTERED_ACCOUNT_TYPES = ('Non-Registered', 'Corporate')
ACCOUNT_TYPES = REGISTERED_ACCOUNT_TYPES + NON_REGISTERED_ACCOUNT_TYPES + ('Other',)
CURRENCIES = ('CAD', 'USD')


class EntryError(ValueError):
    """A user-readable problem with submitted data."""


def is_registered_type(account_type):
    return (account_type or '').strip() in REGISTERED_ACCOUNT_TYPES


def registered_flag(account_type, checkbox_value):
    """Registered status from a form: the checkbox wins, except that it can't be left
    unticked for a type that is registered by definition (or ticked for Non-Registered)."""
    t = (account_type or '').strip()
    if t in REGISTERED_ACCOUNT_TYPES:
        return True
    if t in NON_REGISTERED_ACCOUNT_TYPES:
        return False
    return checkbox_value in ('on', 'true', '1', True)


def parse_amount(value, field, *, allow_blank=True, minimum=None):
    """'1,234.50' / '$1234.5' → 1234.5.  Blank → None (if allowed)."""
    raw = (value or '').strip().replace(',', '').replace('$', '')
    if not raw:
        if allow_blank:
            return None
        raise EntryError(f'{field} is required.')
    try:
        num = float(raw)
    except ValueError:
        raise EntryError(f'{field} must be a number (got "{value}").')
    if minimum is not None and num < minimum:
        raise EntryError(f'{field} must be at least {minimum:g}.')
    return num


def find_or_create_asset_class(portfolio, name):
    name = (name or '').strip()
    if not name:
        raise EntryError('Asset class is required for a new security.')
    ac = (AssetClass.query.filter(AssetClass.portfolio_id == portfolio.id,
                                  db.func.lower(AssetClass.name) == name.lower()).first())
    if ac is None:
        ac = AssetClass(portfolio_id=portfolio.id, name=name)
        db.session.add(ac)
        db.session.flush()
    return ac


def find_security(portfolio, ticker):
    """Exact (case-insensitive) ticker first; else a unique match ignoring exchange
    prefixes/suffixes, so 'XIC.TO' finds an existing 'XIC'."""
    t = (ticker or '').strip()
    if not t:
        return None
    exact = Security.query.filter(Security.portfolio_id == portfolio.id,
                                  db.func.upper(Security.ticker) == t.upper()).first()
    if exact:
        return exact
    key = normalize_ticker(t)
    loose = [s for s in Security.query.filter_by(portfolio_id=portfolio.id).all()
             if normalize_ticker(s.ticker) == key]
    return loose[0] if len(loose) == 1 else None


def suggest_currency(ticker, fallback='CAD'):
    t = (ticker or '').strip().upper()
    if t.startswith(('TSE:', 'CVE:', 'NEO:')) or t.endswith(('.TO', '.V', '.NE', '.CN')):
        return 'CAD'
    if t.endswith('.U'):
        return 'USD'
    if t.startswith(('NYSE:', 'NASDAQ:', 'NYSEARCA:', 'BATS:')):
        return 'USD'
    return fallback


def lookup_elsewhere(user_id, ticker):
    """Details of the same ticker in the user's other portfolios – used to pre-fill the
    new-security fields (handy when setting up a spouse's portfolio)."""
    from models import Portfolio
    key = normalize_ticker(ticker)
    if not key:
        return None
    for s in (Security.query.join(Portfolio, Security.portfolio_id == Portfolio.id)
              .filter(Portfolio.user_id == user_id).order_by(Security.id.desc()).all()):
        if normalize_ticker(s.ticker) == key:
            return s
    return None


def resolve_security(portfolio, ticker, *, name=None, asset_class_name=None, currency=None,
                     price=None, is_illiquid=False, default_currency='CAD'):
    """Return (security, created). Creates the security – and its asset class – when the
    ticker isn't in the portfolio yet. A price, if given, becomes the security's price."""
    ticker = (ticker or '').strip()
    if not ticker:
        raise EntryError('Ticker is required.')
    if len(ticker) > 20:
        raise EntryError(f'Ticker "{ticker[:20]}…" is too long (20 characters max).')
    sec = find_security(portfolio, ticker)
    created = False
    if sec is None:
        ac = find_or_create_asset_class(portfolio, asset_class_name)
        ccy = (currency or '').upper() or suggest_currency(ticker, default_currency)
        if ccy not in CURRENCIES:
            raise EntryError(f'Currency must be one of {", ".join(CURRENCIES)}.')
        sec = Security(portfolio_id=portfolio.id, ticker=ticker, name=(name or '').strip() or None,
                       asset_class_id=ac.id, currency=ccy,
                       auto_update_price=not is_illiquid, is_illiquid=bool(is_illiquid))
        db.session.add(sec)
        db.session.flush()
        created = True
        if price is None:   # same listing already priced in another of your portfolios → reuse it
            sec.last_price = next((o.last_price for o in same_security_elsewhere(sec) if o.last_price), None)
    if price is not None and price > 0:
        set_security_price(sec, price)
    return sec, created


def upsert_holding(account, security, quantity, book_value=None):
    """Add *quantity* of *security* to *account*. An existing position is topped up
    (quantities and book values add) instead of creating a duplicate row.
    Returns (holding, merged)."""
    if quantity is None or quantity <= 0:
        raise EntryError(f'Quantity for {security.ticker} must be more than 0.')
    if account.is_registered:
        book_value = None                   # ACB isn't tracked in registered accounts
    h = Holding.query.filter_by(account_id=account.id, security_id=security.id).first()
    if h:
        # Total ACB is only known if it's known for both lots; otherwise it becomes unknown
        # (the rebalancer then treats a sale as all gain – the conservative choice).
        h.book_value = h.book_value + book_value if (h.book_value is not None and book_value is not None) else None
        h.quantity += quantity
        h.price = security.last_price or h.price
        return h, True
    h = Holding(account_id=account.id, security_id=security.id, quantity=quantity,
                price=security.last_price or 0.0, book_value=book_value)
    db.session.add(h)
    return h, False


def parse_holding_rows(form, prefix='h'):
    """Rows posted as parallel lists: {prefix}_ticker[], {prefix}_quantity[], … Blank rows
    (no ticker and no quantity) are skipped. Returns a list of dicts, 1-based row numbers."""
    keys = ('ticker', 'quantity', 'book_value', 'price', 'name', 'asset_class', 'currency')
    cols = {k: form.getlist(f'{prefix}_{k}[]') for k in keys}
    n = max((len(v) for v in cols.values()), default=0)
    rows = []
    for i in range(n):
        row = {k: (cols[k][i] if i < len(cols[k]) else '').strip() for k in keys}
        if not row['ticker'] and not row['quantity']:
            continue
        row['row'] = i + 1
        rows.append(row)
    return rows


def save_holding_rows(portfolio, account, rows):
    """Validate every row first, then write. Raises EntryError (nothing written) on the
    first bad row. Returns a summary dict."""
    parsed = []
    for r in rows:
        where = f'Row {r["row"]}' + (f' ({r["ticker"]})' if r['ticker'] else '')
        if not r['ticker']:
            raise EntryError(f'{where}: ticker is required.')
        try:
            qty = parse_amount(r['quantity'], 'Quantity', allow_blank=False, minimum=0)
            bv = parse_amount(r['book_value'], 'Book value', minimum=0)
            price = parse_amount(r['price'], 'Price', minimum=0)
        except EntryError as exc:
            raise EntryError(f'{where}: {exc}')
        if qty <= 0:
            raise EntryError(f'{where}: quantity must be more than 0.')
        if find_security(portfolio, r['ticker']) is None and not r['asset_class']:
            raise EntryError(f'{where}: {r["ticker"]} is a new security – choose or type an asset class.')
        parsed.append((r, qty, bv, price))

    summary = {'added': 0, 'merged': 0, 'new_securities': [], 'unpriced': []}
    for r, qty, bv, price in parsed:
        sec, created = resolve_security(portfolio, r['ticker'], name=r['name'], asset_class_name=r['asset_class'],
                                        currency=r['currency'], price=price,
                                        default_currency=account.currency or portfolio.base_currency or 'CAD')
        if created:
            summary['new_securities'].append(sec.ticker)
        _, merged = upsert_holding(account, sec, qty, bv)
        summary['merged' if merged else 'added'] += 1
        if not sec.last_price:
            summary['unpriced'].append(sec.ticker)
    return summary
