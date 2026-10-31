"""
services/portfolio.py – Portfolio-level analytics helpers.

All functions take a Portfolio; returned values are in its base_currency unless noted.
"""
from collections import defaultdict
from services.fx import convert_to_base


def calculate_portfolio_allocation(portfolio, exchange_rates: dict):
    """
    Compute current allocation by asset_class_id.

    Returns:
        allocation      – {asset_class_id: value_in_base}
        allocation_pct  – {asset_class_id: percentage}
        total_value     – float, total portfolio value in base currency
    """
    allocation: dict = defaultdict(float)
    total_value = 0.0

    for account in portfolio.accounts:
        # Cash contribution
        cash = account.cash_balance or 0.0
        cash_base = convert_to_base(
            cash, account.currency, portfolio.base_currency, exchange_rates
        )
        total_value += cash_base

        # Holdings contribution
        for holding in account.holdings:
            if holding.asset_class_id is None:
                continue
            value = holding.market_value_in_base_currency(exchange_rates)
            allocation[holding.asset_class_id] += value
            total_value += value

    allocation_pct = {
        ac_id: (value / total_value * 100 if total_value > 0 else 0.0)
        for ac_id, value in allocation.items()
    }
    return dict(allocation), allocation_pct, total_value
