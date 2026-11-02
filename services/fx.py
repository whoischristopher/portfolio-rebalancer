"""
services/fx.py – Currency / exchange-rate helpers.

All monetary values passed around the application are in the user's
base_currency (default: CAD).  This module is the single source of truth
for FX logic.
"""
import logging
import requests
from datetime import date, datetime, timedelta
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
        # No live rate and nothing cached: rough fallback, in the right direction.
        if (from_curr, to_curr) == ("CAD", "USD"):
            return 1 / _FALLBACK_USD_CAD
        return _FALLBACK_USD_CAD


def get_exchange_rates(_portfolio=None) -> dict:
    """
    Return a dict of all cross-rates needed for a portfolio (rates are global, so the
    argument is currently unused).
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


# --------------------------------------------------------------------------- #
# Bank of Canada daily rates (for tax: the CRA accepts the BoC rate on the day of
# the transaction). Valet API, no key needed. Rates only change once a day and past
# days never change, so a small in-process cache is enough.
# --------------------------------------------------------------------------- #
BOC_VALET_URL = "https://www.bankofcanada.ca/valet/observations/FX{ccy}CAD/json"
_BOC_LOOKBACK_DAYS = 10       # weekends/holidays have no observation: use the last one before
_boc_cache: dict = {}


class BocRateError(Exception):
    pass


def _boc_to_cad(ccy: str, on_date: date) -> tuple[float, date]:
    """(rate ccy→CAD, observation date) for the last BoC observation on or before on_date."""
    if ccy == "CAD":
        return 1.0, on_date
    key = (ccy, on_date)
    if key in _boc_cache:
        return _boc_cache[key]
    try:
        resp = requests.get(BOC_VALET_URL.format(ccy=ccy), timeout=10, params={
            "start_date": (on_date - timedelta(days=_BOC_LOOKBACK_DAYS)).isoformat(),
            "end_date": on_date.isoformat(),
        })
        resp.raise_for_status()
        series = f"FX{ccy}CAD"
        obs = [(o["d"], o[series]["v"]) for o in resp.json().get("observations", []) if series in o]
    except Exception as exc:
        raise BocRateError(f"Bank of Canada rate lookup failed: {exc}") from exc
    obs = [(d, v) for d, v in obs if d <= on_date.isoformat()]
    if not obs:
        raise BocRateError(f"No Bank of Canada {ccy}/CAD rate published on or before {on_date.isoformat()}")
    d, v = max(obs)
    result = (float(v), date.fromisoformat(d))
    if on_date < date.today():           # today's rate may not be published yet: don't cache a miss-day
        _boc_cache[key] = result
    return result


def boc_rate(from_curr: str, to_curr: str, on_date: date) -> tuple[float, date]:
    """Bank of Canada rate from_curr→to_curr on on_date (crossed through CAD when neither
    side is CAD). Returns (rate, observation date). Raises BocRateError."""
    if from_curr == to_curr:
        return 1.0, on_date
    f, fd = _boc_to_cad(from_curr, on_date)
    t, td = _boc_to_cad(to_curr, on_date)
    # CAD legs report on_date itself; the real observation date is the other leg's.
    obs_dates = [d for c, d in ((from_curr, fd), (to_curr, td)) if c != "CAD"]
    return f / t, min(obs_dates)


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

