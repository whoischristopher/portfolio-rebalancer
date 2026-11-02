"""
services/money.py – One way to show money everywhere.

Amounts in the portfolio's base currency get a plain "$"; other currencies get a
prefix that says which dollar it is ("US$" in a CAD portfolio, "C$" in a USD one),
so mixed CAD/USD accounts stay unambiguous.
"""

_PREFIX = {'USD': 'US$', 'CAD': 'C$'}


def currency_symbol(ccy=None, base='CAD'):
    base = (base or 'CAD').upper()
    ccy = (ccy or base).upper()
    if ccy == base:
        return '$'
    return _PREFIX.get(ccy, f'{ccy} ')


def fmt_money(value, ccy=None, base='CAD', decimals=2):
    """1234.5 → '$1,234.50';  -20 → '−$20.00';  (5, 'USD', 'CAD') → 'US$5.00'."""
    if value is None:
        return '—'
    sign = '−' if value < 0 and round(abs(value), decimals) > 0 else ''
    return f'{sign}{currency_symbol(ccy, base)}{abs(value):,.{decimals}f}'
