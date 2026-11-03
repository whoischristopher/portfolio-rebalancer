"""
services/prices.py – Price handling.

A security has ONE price: Security.last_price (in the security's currency).
Holding.price is kept as a synced copy for the rest of the app; always change
prices through set_security_price() so the two never diverge.

Prices are the one thing portfolios share. Each portfolio has its own security list,
but the price sheet belongs to the login and fills every portfolio in one pass, and a
price typed by hand is copied to the same ticker (same currency) in the login's other
portfolios – so the same ETF can never carry two different prices.
"""
import json
import logging
import os
import re

log = logging.getLogger(__name__)

# Exchange suffixes (Yahoo style) we ignore when matching sheet tickers to securities.
# ".U" is deliberately NOT here: on the TSX it marks a separate USD-traded listing.
_SUFFIXES = (".TO", ".V", ".NE", ".CN", ".TSX")


class PriceSheetError(Exception):
    """Raised with a user-readable reason when the price sheet can't be read."""


class SheetPrices(dict):
    """ticker → price, as read from the sheet, plus .tickers: every ticker in column A,
    including rows with no usable price (#N/A, Loading…) – so the app can tell a ticker
    that's missing from the sheet apart from one whose GOOGLEFINANCE lookup fails –
    and .names: ticker → security name from the optional column C
    (=GOOGLEFINANCE(A2,"name")), for filling in names nobody has typed."""

    def __init__(self, prices=None, tickers=None, names=None):
        super().__init__(prices or {})
        self.tickers = list(tickers) if tickers is not None else list(self)
        self.names = dict(names or {})


# Column-A values treated as a header row rather than a ticker.
_HEADER_WORDS = {"TICKER", "TICKERS", "SYMBOL", "SYMBOLS", "SECURITY"}


def normalize_ticker(ticker: str) -> str:
    """'TSE:XEC' / 'XEC.TO' / ' xec ' → 'XEC'.  'NYSEARCA:ITOT' → 'ITOT'."""
    t = (ticker or "").strip().upper()
    if ":" in t:
        t = t.split(":", 1)[1]
    for sfx in _SUFFIXES:
        if t.endswith(sfx):
            t = t[: -len(sfx)]
            break
    return t


def _to_float(value):
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = re.sub(r"[^0-9.\-]", "", str(value))   # drops $, CA$, US$, commas, spaces
    return float(cleaned) if cleaned not in ("", "-", ".") else None


def fetch_prices_from_user_sheet(user) -> SheetPrices:
    """Read ticker→price from the user's Google Sheet (column A ticker, column B price,
    optional column C name).

    Reads the first sheet tab, UNFORMATTED values (so "$45.12" or "1,234.50"
    formatting can't break parsing).  Raises PriceSheetError on failure.
    """
    if not user.price_sheet_id:
        raise PriceSheetError("No price sheet ID set – add it in Settings.")
    if not user.google_token:
        raise PriceSheetError("No Google sign-in token stored – log out and log in again.")

    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    try:
        token = json.loads(user.google_token)
        creds = Credentials(
            token=token["access_token"],
            refresh_token=token.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.getenv("GOOGLE_CLIENT_ID"),
            client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        meta = service.spreadsheets().get(spreadsheetId=user.price_sheet_id,
                                          fields="sheets.properties.title").execute()
        first_tab = meta["sheets"][0]["properties"]["title"]
        result = service.spreadsheets().values().get(
            spreadsheetId=user.price_sheet_id,
            range=f"'{first_tab}'!A1:C",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except Exception as exc:
        log.exception("Price sheet read failed for user %s", user.id)
        raise PriceSheetError(_explain_google_error(exc)) from exc

    prices = parse_sheet_rows(result.get("values", []))
    if not prices:
        raise PriceSheetError("The sheet was read but no usable prices were found in columns A–B "
                              "(check for #N/A or text in column B).")
    return prices


def _clean_name(value):
    """A usable name from column C, or None for blanks and formula errors (#N/A, Loading…)."""
    name = str(value).strip() if value is not None else ""
    if not name or name.startswith("#") or name.lower().startswith("loading"):
        return None
    return name[:200]


def parse_sheet_rows(rows) -> SheetPrices:
    """Turn the sheet's A:C values into SheetPrices. Blank and header rows are skipped."""
    prices, tickers, names = {}, [], {}
    for row in rows:
        ticker = str(row[0]).strip() if row else ""
        if not ticker or ticker.upper() in _HEADER_WORDS:
            continue
        if ticker not in tickers:
            tickers.append(ticker)
        try:
            price = _to_float(row[1]) if len(row) > 1 else None
        except ValueError:
            price = None            # "#N/A", "Loading..." etc.
        if price and price > 0:
            prices[ticker] = price
        name = _clean_name(row[2]) if len(row) > 2 else None
        if name:
            names[ticker] = name
    return SheetPrices(prices, tickers, names)


def sheet_name_for(ticker, names: dict):
    """Name the sheet gives for *ticker* (exchange prefix/suffix ignored), if exactly one
    row matches – used to pre-fill a new security that's already in the sheet."""
    if not ticker or not names:
        return None
    exact = {t.strip().upper(): n for t, n in names.items()}
    if ticker.strip().upper() in exact:
        return exact[ticker.strip().upper()]
    key = normalize_ticker(ticker)
    hits = {n for t, n in names.items() if normalize_ticker(t) == key}
    return hits.pop() if len(hits) == 1 else None


def _explain_google_error(exc) -> str:
    """Turn a Google API failure into an actionable message, keeping Google's own reason."""
    status = getattr(getattr(exc, "resp", None), "status", None) or getattr(exc, "status_code", None)
    reason = (getattr(exc, "reason", None) or str(exc)).strip()
    low = reason.lower() + " " + str(exc).lower()
    if "invalid_grant" in low or status == 401:
        hint = "Google sign-in expired or was revoked – log out and log in again."
    elif "insufficient authentication scopes" in low or "insufficientpermissions" in low:
        hint = ("Your sign-in didn't grant access to Google Sheets. Log out, log in again, and on Google's "
                "consent screen tick the box allowing the app to see your spreadsheets.")
    elif "has not been used in project" in low or "is disabled" in low or "service_disabled" in low \
            or "accessnotconfigured" in low:
        hint = ("The Google Sheets API isn't enabled in your Google Cloud project – enable it under "
                "APIs & Services → Library → Google Sheets API, wait a minute, then retry.")
    elif status == 404 or "not found" in low:
        hint = "Sheet not found – check the Sheet ID in Settings."
    elif status == 403:
        hint = ("Google refused access to the sheet – make sure you're logged in with a Google account "
                "that can open it.")
    else:
        hint = "Couldn't read the price sheet."
    return f"{hint} (Google said: {reason[:200]})"


def match_prices_to_securities(prices: dict, securities) -> tuple[dict, list]:
    """Return ({security_id: price}, [unmatched sheet tickers]).  Exact ticker match wins,
    otherwise match on the normalised ticker (exchange prefix/suffix ignored)."""
    exact = {s.ticker.strip().upper(): s for s in securities if s.ticker}
    norm = {}
    for s in securities:
        if s.ticker:
            norm.setdefault(normalize_ticker(s.ticker), []).append(s)
    matched, unmatched = {}, []
    for raw, price in prices.items():
        sec = exact.get(raw.strip().upper())
        candidates = [sec] if sec else norm.get(normalize_ticker(raw), [])
        if len(candidates) == 1:
            matched[candidates[0].id] = price
        else:
            unmatched.append(raw)
    return matched, unmatched


def match_prices_across_portfolios(prices: dict, securities) -> tuple[dict, list]:
    """match_prices_to_securities() for securities spread over several portfolios.

    Each portfolio is matched on its own (the same ticker legitimately appears once per
    portfolio); a sheet ticker counts as unmatched only if no portfolio could use it."""
    groups = {}
    for s in securities:
        groups.setdefault(s.portfolio_id, []).append(s)
    matched, used = {}, set()
    for group in groups.values():
        m, unmatched = match_prices_to_securities(prices, group)
        matched.update(m)
        used |= set(prices) - set(unmatched)
    return matched, [t for t in prices if t not in used]


def same_security_elsewhere(security):
    """The same listing (normalised ticker + currency) in the owner's other portfolios."""
    from models import Portfolio, Security
    owner = security.portfolio.user_id if security.portfolio else None
    if owner is None or not security.ticker:
        return []
    key = normalize_ticker(security.ticker)
    others = (Security.query.join(Portfolio, Security.portfolio_id == Portfolio.id)
              .filter(Portfolio.user_id == owner, Security.id != security.id,
                      Security.currency == security.currency).all())
    return [s for s in others if normalize_ticker(s.ticker) == key]


def set_security_price(security, price: float, propagate: bool = True):
    """Single place to change a price: updates the security and every holding of it –
    and, with propagate, the same ticker in the owner's other portfolios."""
    from datetime import datetime
    from models import Holding
    targets = [security] + (same_security_elsewhere(security) if propagate else [])
    for sec in targets:
        sec.last_price = float(price)
        Holding.query.filter_by(security_id=sec.id).update(
            {"price": float(price), "updated_at": datetime.utcnow()}, synchronize_session="fetch")


# --------------------------------------------------------------------------- #
# Keeping the sheet in step with the securities list.
# The app can only READ the sheet, so it can't add rows itself; instead it compares the
# sheet's tickers (remembered from the last read) with the securities and hands the user
# ready-to-paste rows for anything missing.

# Yahoo-style suffix → GOOGLEFINANCE exchange prefix.
_GF_PREFIX_FOR_SUFFIX = {".TO": "TSE", ".TSX": "TSE", ".V": "CVE", ".NE": "NEO", ".CN": "CNSX"}

# Formula for column B that prices whatever ticker sits in column A of the same row,
# so the rows can be pasted anywhere in the sheet. Column C (optional) gives the name.
SHEET_PRICE_FORMULA = '=GOOGLEFINANCE(INDIRECT("A"&ROW()),"price")'
SHEET_NAME_FORMULA = '=IFERROR(GOOGLEFINANCE(INDIRECT("A"&ROW()),"name"),"")'


def google_finance_symbol(ticker: str, currency: str = "CAD") -> str:
    """Best guess at the GOOGLEFINANCE symbol for a security.

    'TSE:XEC' stays as is; 'XEC.TO' → 'TSE:XEC'; a CAD ticker with no exchange → 'TSE:…'
    (including TSX USD listings such as 'DLR.U'); USD and other tickers are left bare,
    which GOOGLEFINANCE resolves to the main US listing. Only a guess – the user should
    check any row that comes back #N/A."""
    t = (ticker or "").strip().upper()
    if not t or ":" in t:
        return t
    for sfx, prefix in _GF_PREFIX_FOR_SUFFIX.items():
        if t.endswith(sfx):
            return f"{prefix}:{t[: -len(sfx)]}"
    if (currency or "CAD").upper() == "CAD" or t.endswith(".U"):
        return f"TSE:{t}"
    return t


def sheet_coverage(sheet_tickers, priced_tickers, securities) -> dict:
    """Compare the price sheet with the auto-priced securities of every portfolio.

    sheet_tickers:  every ticker in column A at the last read
    priced_tickers: those of them that had a usable price
    securities:     Security objects with auto_update_price on (any portfolio)

    Returns {'missing': [Security…]  – no row in the sheet (one per ticker+currency),
             'no_price': [(Security, sheet ticker)…] – a row, but no price from it,
             'orphans': [sheet ticker…] – rows no security uses}."""
    rows = {t: t for t in sheet_tickers}
    matched, orphans = match_prices_across_portfolios(rows, securities)
    priced = set(priced_tickers)
    missing, no_price, seen = [], [], set()
    for s in sorted(securities, key=lambda s: (s.ticker or "").upper()):
        key = (normalize_ticker(s.ticker), (s.currency or "CAD").upper())
        if key in seen:              # same listing in another portfolio – one row covers both
            continue
        seen.add(key)
        row = matched.get(s.id)
        if row is None:
            missing.append(s)
        elif row not in priced:
            no_price.append((s, row))
    return {"missing": missing, "no_price": no_price, "orphans": orphans}


def sheet_rows_for(securities) -> str:
    """Tab-separated rows (ticker, price formula, name formula) – pasted into column A of
    the sheet they fill A, B and C."""
    return "\n".join(f"{google_finance_symbol(s.ticker, s.currency)}\t{SHEET_PRICE_FORMULA}\t{SHEET_NAME_FORMULA}"
                     for s in securities)
