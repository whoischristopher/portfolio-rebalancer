"""
services/fx.py – Currency / exchange-rate helpers.

All monetary values passed around the application are in the user's
base_currency (default: CAD).  This module is the single source of truth
for FX logic.
"""
import logging
import requests
from datetime import datetime
from extensions import db
from models import ExchangeRate

log = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 14_400   # 4 hours
_FALLBACK_USD_CAD  = 1.35


def fetch_exchange_rate(from_curr: str, to_curr: str) -> float:
    """Return exchange rate from_curr → to_curr, using DB cache then live API."""
    if from_curr == to_curr:
        return 1.0

    cached = (
        ExchangeRate.query
        .filter_by(from_currency=from_curr, to_currency=to_curr)
        .order_by(ExchangeRate.date.desc())
        .first()
    )
    if cached and (datetime.utcnow() - cached.date).total_seconds() < _CACHE_TTL_SECONDS:
        return cached.rate

    try:
        resp = requests.get(
            f"https://api.exchangerate-api.com/v4/latest/{from_curr}", timeout=10
        )
        resp.raise_for_status()
        rate = float(resp.json()["rates"][to_curr])
        # Replace stale cache entry
        ExchangeRate.query.filter_by(
            from_currency=from_curr, to_currency=to_curr
        ).delete()
        db.session.add(ExchangeRate(
            from_currency=from_curr,
            to_currency=to_curr,
            rate=rate,
            source="exchangerate-api",
            date=datetime.utcnow(),
        ))
        db.session.commit()
        return rate
    except Exception as exc:
        log.warning("FX fetch failed (%s→%s): %s", from_curr, to_curr, exc)
        if cached:
            return cached.rate
        return _FALLBACK_USD_CAD


def get_exchange_rates(user) -> dict:
    """
    Return a dict of all cross-rates needed for the user's portfolio.
    Keys follow the pattern  "{FROM}_TO_{TO}"  e.g. "USD_TO_CAD".
    """
    rates: dict = {}
    currencies = {"CAD", "USD"}  # expand as needed

    for frm in currencies:
        for to in currencies:
            if frm == to:
                continue
            key = f"{frm}_TO_{to}"
            try:
                rates[key] = fetch_exchange_rate(frm, to)
            except Exception as exc:
                log.error("Could not obtain rate %s: %s", key, exc)
    return rates


def convert_to_base(amount: float, from_currency: str, to_currency: str,
                    exchange_rates: dict) -> float:
    """Convert *amount* from_currency → to_currency using exchange_rates dict."""
    if from_currency == to_currency:
        return amount
    key = f"{from_currency}_TO_{to_currency}"
    if key in exchange_rates:
        return amount * exchange_rates[key]
    inverse = f"{to_currency}_TO_{from_currency}"
    if inverse in exchange_rates:
        return amount / exchange_rates[inverse]
    log.warning("No rate found for %s; assuming 1:1", key)
    return amount

