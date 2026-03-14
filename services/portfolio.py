"""
services/portfolio.py – Portfolio-level analytics helpers.

All returned values are in the user's base_currency unless noted.
"""
from collections import defaultdict
from models import Account, Target, AssetClass, Security, SecurityPreference, Holding
from services.fx import convert_to_base


def calculate_portfolio_allocation(user, exchange_rates: dict):
    """
    Compute current allocation by asset_class_id.

    Returns:
        allocation      – {asset_class_id: value_in_base}
        allocation_pct  – {asset_class_id: percentage}
        total_value     – float, total portfolio value in base currency
    """
    allocation: dict = defaultdict(float)
    total_value = 0.0

    for account in user.accounts:
        # Cash contribution
        cash = account.cash_balance or 0.0
        cash_base = convert_to_base(
            cash, account.currency, user.base_currency, exchange_rates
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


def calculate_asset_class_deltas(user, exchange_rates: dict):
    """
    For each target, compute how far current allocation is from target.

    Returns:
        deltas          – list of dicts with keys:
                          asset_class_id, asset_class_name, target,
                          current_value, target_value, dollar_diff,
                          percentage_diff, current_pct, target_pct
        total_portfolio – float
    """
    allocation, allocation_pct, total_portfolio = calculate_portfolio_allocation(
        user, exchange_rates
    )
    deltas = []
    for target in Target.query.filter_by(user_id=user.id).all():
        current_value = allocation.get(target.asset_class_id, 0.0)
        current_pct   = allocation_pct.get(target.asset_class_id, 0.0)
        target_value  = total_portfolio * target.target_percentage / 100
        deltas.append({
            "asset_class_id":   target.asset_class_id,
            "asset_class_name": target.asset_class.name,
            "target":           target,
            "current_value":    current_value,
            "target_value":     target_value,
            "dollar_diff":      target_value - current_value,
            "percentage_diff":  current_pct - target.target_percentage,
            "current_pct":      current_pct,
            "target_pct":       target.target_percentage,
        })
    return deltas, total_portfolio


def calculate_security_deltas(user, exchange_rates: dict):
    """
    Compute per-security value deltas against proportional targets.

    Returns:
        security_totals          – {security_id: {security, asset_class_id,
                                    total_value, by_account}}
        desired_security_delta   – {security_id: delta_in_base}
    """
    security_totals: dict = {}
    current_by_class: dict = defaultdict(float)

    for account in user.accounts:
        for holding in account.holdings:
            sec = holding.security
            if not sec or not sec.asset_class_id:
                continue
            value = holding.market_value_in_base_currency(exchange_rates)
            if sec.id not in security_totals:
                security_totals[sec.id] = {
                    "security":       sec,
                    "asset_class_id": sec.asset_class_id,
                    "total_value":    0.0,
                    "by_account":     defaultdict(float),
                }
            security_totals[sec.id]["total_value"]           += value
            security_totals[sec.id]["by_account"][account.id] += value
            current_by_class[sec.asset_class_id]             += value

    deltas, _ = calculate_asset_class_deltas(user, exchange_rates)
    target_by_class = {d["asset_class_id"]: d["target_value"] for d in deltas}

    desired_security_delta = {}
    for sec_id, info in security_totals.items():
        cls           = info["asset_class_id"]
        current_total = info["total_value"]
        class_current = current_by_class[cls]
        class_target  = target_by_class.get(cls, class_current)
        if class_current <= 0:
            desired_security_delta[sec_id] = 0.0
        else:
            target_for_sec = (current_total / class_current) * class_target
            desired_security_delta[sec_id] = target_for_sec - current_total

    return security_totals, desired_security_delta

