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


# --- keeping the sheet in step with the securities list ---------------------------------

def test_parse_sheet_rows_keeps_tickers_without_a_price():
    from services.prices import parse_sheet_rows
    p = parse_sheet_rows([["Ticker", "Price"], ["TSE:XEC", 30.5], ["TSE:BAD", "#N/A"], [], [""], ["NYSEARCA:ITOT"],
                          ["TSE:XEC", 30.5]])
    assert dict(p) == {"TSE:XEC": 30.5}
    assert p.tickers == ["TSE:XEC", "TSE:BAD", "NYSEARCA:ITOT"]     # header, blanks and duplicates dropped


def test_google_finance_symbol_guesses():
    from services.prices import google_finance_symbol as gf
    assert gf("XEQT", "CAD") == "TSE:XEQT"
    assert gf("xeqt.to", "CAD") == "TSE:XEQT"
    assert gf("DLR.U", "USD") == "TSE:DLR.U"          # TSX USD listing
    assert gf("VFV.NE", "CAD") == "NEO:VFV"
    assert gf("ABC.V", "CAD") == "CVE:ABC"
    assert gf("ITOT", "USD") == "ITOT"                # Google resolves the US listing
    assert gf("NYSEARCA:ITOT", "USD") == "NYSEARCA:ITOT"


def test_sheet_coverage_finds_missing_broken_and_orphan_rows():
    from services.prices import sheet_coverage
    secs = [NS(id=1, ticker="XEC", currency="CAD", portfolio_id=1),
            NS(id=2, ticker="ITOT", currency="USD", portfolio_id=1),
            NS(id=3, ticker="ZAG", currency="CAD", portfolio_id=1),
            NS(id=4, ticker="VFV", currency="CAD", portfolio_id=1),
            NS(id=5, ticker="ZAG", currency="CAD", portfolio_id=2)]   # same listing, other portfolio
    cov = sheet_coverage(["TSE:XEC", "NYSEARCA:ITOT", "TSE:VFV", "TSE:OLD"],
                         ["TSE:XEC", "NYSEARCA:ITOT", "TSE:OLD"], secs)
    assert [s.ticker for s in cov["missing"]] == ["ZAG"]                     # listed once for both portfolios
    assert [(s.ticker, row) for s, row in cov["no_price"]] == [("VFV", "TSE:VFV")]
    assert cov["orphans"] == ["TSE:OLD"]


def test_sheet_coverage_all_good():
    from services.prices import sheet_coverage
    secs = [NS(id=1, ticker="XEC", currency="CAD", portfolio_id=1)]
    assert sheet_coverage(["TSE:XEC"], ["TSE:XEC"], secs) == {"missing": [], "no_price": [], "orphans": []}


def test_sheet_rows_are_tab_separated_with_a_row_relative_formula():
    from services.prices import sheet_rows_for
    rows = sheet_rows_for([NS(ticker="ZAG", currency="CAD"), NS(ticker="ITOT", currency="USD")]).split("\n")
    assert rows == ['TSE:ZAG\t=GOOGLEFINANCE(INDIRECT("A"&ROW()),"price")',
                    'ITOT\t=GOOGLEFINANCE(INDIRECT("A"&ROW()),"price")']
