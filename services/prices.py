"""
services/prices.py – Price handling.

A security has ONE price: Security.last_price (in the security's currency).
Holding.price is kept as a synced copy for the rest of the app; always change
prices through set_security_price() so the two never diverge.
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


def fetch_prices_from_user_sheet(user) -> dict:
    """Read ticker→price from the user's Google Sheet (column A ticker, column B price).

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
            range=f"'{first_tab}'!A1:B",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute()
    except Exception as exc:
        log.exception("Price sheet read failed for user %s", user.id)
        raise PriceSheetError(_explain_google_error(exc)) from exc

    prices = {}
    for row in result.get("values", []):
        if len(row) < 2 or not str(row[0]).strip():
            continue
        try:
            price = _to_float(row[1])
        except ValueError:
            price = None            # "#N/A", "Loading..." etc.
        if price and price > 0:
            prices[str(row[0]).strip()] = price
    if not prices:
        raise PriceSheetError("The sheet was read but no usable prices were found in columns A–B "
                              "(check for #N/A or text in column B).")
    return prices


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


def set_security_price(security, price: float):
    """Single place to change a price: updates the security and every holding of it."""
    from datetime import datetime
    from models import Holding
    security.last_price = float(price)
    Holding.query.filter_by(security_id=security.id).update(
        {"price": float(price), "updated_at": datetime.utcnow()}, synchronize_session="fetch")
