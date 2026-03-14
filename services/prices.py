"""
services/prices.py – External price fetching helpers.
"""
import json
import logging
import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

log = logging.getLogger(__name__)


def fetch_prices_from_user_sheet(user) -> dict:
    """
    Read ticker→price pairs from the user's Google Sheet.

    Expects column A = ticker symbol, column B = price (e.g. a GOOGLEFINANCE formula).
    Returns {} on any error so callers can decide how to handle missing prices.
    """
    if not user.google_token or not user.price_sheet_id:
        return {}
    try:
        token_dict = json.loads(user.google_token)
        creds = Credentials(
            token=token_dict["access_token"],
            refresh_token=token_dict.get("refresh_token"),
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.getenv("GOOGLE_CLIENT_ID"),
            client_secret=os.getenv("GOOGLE_CLIENT_SECRET"),
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
        )
        service = build("sheets", "v4", credentials=creds)
        result = (
            service.spreadsheets().values()
            .get(spreadsheetId=user.price_sheet_id, range="Sheet1!A1:B")
            .execute()
        )
        prices = {}
        for row in result.get("values", []):
            if len(row) >= 2 and row[0]:
                try:
                    prices[row[0]] = float(row[1])
                except (ValueError, TypeError):
                    pass
        return prices
    except Exception as exc:
        log.error("Error reading price sheet for user %s: %s", user.id, exc)
        return {}

