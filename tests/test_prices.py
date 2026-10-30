import os
import sys
from types import SimpleNamespace as NS

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.prices import _to_float, match_prices_to_securities, normalize_ticker  # noqa: E402


def test_normalize_ticker():
    assert normalize_ticker("TSE:XEC") == "XEC"
    assert normalize_ticker(" xec.to ") == "XEC"
    assert normalize_ticker("NYSEARCA:ITOT") == "ITOT"
    assert normalize_ticker("DLR.U") == "DLR.U"        # USD listing on TSX is a different security
    assert normalize_ticker("VFV.NE") == "VFV"


def test_to_float_handles_formatting():
    assert _to_float(45.12) == 45.12
    assert _to_float("$1,234.50") == 1234.5
    assert _to_float("CA$ 32.10") == 32.1
    assert _to_float("#N/A") is None


def test_match_prefers_exact_then_normalised():
    secs = [NS(id=1, ticker="XEC"), NS(id=2, ticker="ITOT"), NS(id=3, ticker="DLR.U"), NS(id=4, ticker="DLR")]
    matched, unmatched = match_prices_to_securities(
        {"TSE:XEC": 30.0, "NYSEARCA:ITOT": 130.0, "DLR.U": 10.0, "TSE:DLR": 13.8, "ZZZ": 1.0}, secs)
    assert matched == {1: 30.0, 2: 130.0, 3: 10.0, 4: 13.8}
    assert unmatched == ["ZZZ"]


def test_google_error_messages_are_specific():
    from services.prices import _explain_google_error

    class HttpError(Exception):
        def __init__(self, status, reason):
            super().__init__(reason); self.resp = NS(status=status); self.reason = reason

    assert "tick the box" in _explain_google_error(HttpError(403, "Request had insufficient authentication scopes."))
    assert "Sheets API isn't enabled" in _explain_google_error(
        HttpError(403, "Google Sheets API has not been used in project 123 before or it is disabled."))
    assert "account that can open it" in _explain_google_error(HttpError(403, "The caller does not have permission"))
    assert "Sheet ID" in _explain_google_error(HttpError(404, "Requested entity was not found."))
