"""
rebalancer.py - Portfolio Rebalancing Engine
"""

import copy
import logging
from collections import defaultdict

from models import (
    Account, Holding, Security, SecurityPreference,
    RebalanceTransaction, Target,
)
from extensions import db
from services.fx import convert_to_base
from services.portfolio import calculate_portfolio_allocation, calculate_asset_class_deltas

log = logging.getLogger(__name__)

class TransactionPlan:
    def __init__(self, transactions: list, metadata: dict = None):
        self.transactions = transactions
        self.metadata = metadata or {}

    def __len__(self):
        return len(self.transactions)

    def score(self, user) -> tuple:
        new_positions = 0
        registered_sells = 0
        unregistered_sells = 0
        accounts_cache: dict = {}

        # Build a portfolio-wide set of existing security IDs
        all_held_security_ids = set()
        for acc in Account.query.filter_by(user_id=user.id).all():
            for h in acc.holdings:
                all_held_security_ids.add(h.security_id)

        for txn in self.transactions:
            acc = accounts_cache.setdefault(txn.account_id, Account.query.get(txn.account_id))
            if txn.action == "BUY" and txn.security_id:
                if txn.security_id not in all_held_security_ids:
                    new_positions += 1
            elif txn.action == "SELL" and acc:
                if acc.is_registered:
                    registered_sells += 1
                else:
                    unregistered_sells += 1

        return (new_positions * 100, len(self.transactions), unregistered_sells * 50, -registered_sells)

class RebalancingStrategy:

    def __init__(self, name: str):
        self.name = name

    def _to_base(self, amount, from_currency, user, exchange_rates):
        return convert_to_base(amount, from_currency, user.base_currency, exchange_rates)

    def _from_base(self, amount, to_currency, user, exchange_rates):
        return convert_to_base(amount, user.base_currency, to_currency, exchange_rates)

    def _compute_constraint_score(self, user, ac_id):
        """
        Returns a constraint score for an asset class.
        Lower = more constrained = should be scheduled first.
        Counts unique eligible securities across all accounts × number of eligible accounts.
        """
        all_securities = set()
        eligible_account_count = 0
        for account in user.accounts:
            eligible = self._eligible_securities(ac_id, account, user)
            if eligible:
                eligible_account_count += 1
                for e in eligible:
                    all_securities.add(e["security"].id)
        return len(all_securities) * 10 + eligible_account_count


    def _eligible_securities(self, asset_class_id, account, user):
        securities = Security.query.filter_by(asset_class_id=asset_class_id).all()
        prefs = {p.security_id: p for p in SecurityPreference.query.filter_by(user_id=user.id).all()}
        result = []
        for sec in securities:
            pref = prefs.get(sec.id)
            allowed = True
            priority = 99  # unlisted = lowest priority

            if pref and pref.restriction_type == "restricted_to_accounts":
                allowed_ids = (pref.account_config or {}).get("allowed", [])
                allowed = account.id in allowed_ids

            elif pref and pref.restriction_type == "prioritized_accounts":
                cfg = pref.account_config or {}
                for level in (1, 2, 3):
                    if account.id in cfg.get(f"priority_{level}", []):
                        priority = level
                        break
                # accounts not listed remain allowed but at priority 99

            if allowed:
                existing = any(h.security_id == sec.id for h in account.holdings)
                result.append({"security": sec, "existing": existing, "priority": priority})
        return result


    def _create_sell_transaction(self, user, account, holding, sell_value, execution_order):
        quantity = int(sell_value / holding.price)
        if quantity < 1:
            return None
        return RebalanceTransaction(
            user_id=user.id,
            account_id=account.id,
            security_id=holding.security_id,
            action="SELL",
            quantity=quantity,
            price=holding.price,
            amount=quantity * holding.price,
            currency=holding.security.currency,
            execution_order=execution_order,
            is_final_trade=False,
        )

    def _create_buy_transaction(self, user, account, asset_class_id, amount_base,
                                execution_order, prefer_existing=True, exchange_rates=None):
        eligible = self._eligible_securities(asset_class_id, account, user)
        if not eligible:
            return None
        if prefer_existing:
            eligible.sort(key=lambda x: (x.get("priority", 99), not x["existing"], x["security"].id))
        if len(eligible) > 1:
            return RebalanceTransaction(
                user_id=user.id,
                account_id=account.id,
                action="BUY",
                quantity=0,
                price=0,
                amount=amount_base,
                currency=user.base_currency,
                execution_order=execution_order,
                requires_user_selection=True,
                available_securities=[e["security"].id for e in eligible],
                is_final_trade=False,
            )
        sec = eligible[0]["security"]
        holding = (
            next((h for h in account.holdings if h.security_id == sec.id), None)
            or Holding.query.filter_by(security_id=sec.id).first()
        )
        if not holding or not holding.price or holding.price <= 0:
            return None
        price_in_base = self._to_base(holding.price, sec.currency, user, exchange_rates or {})
        quantity = int(amount_base / price_in_base)
        if quantity < 1:
            return None
        return RebalanceTransaction(
            user_id=user.id,
            account_id=account.id,
            security_id=sec.id,
            action="BUY",
            quantity=quantity,
            price=holding.price,
            amount=quantity * holding.price,
            currency=sec.currency,
            execution_order=execution_order,
            is_final_trade=False,
        )

    def _apply_sell_limiting(self, transactions, original_cash, user, exchange_rates):
        by_account = defaultdict(lambda: {"sells": [], "buys": []})
        for txn in transactions:
            by_account[txn.account_id]["sells" if txn.action == "SELL" else "buys"].append(txn)
        result = []
        for acc_id, txns in by_account.items():
            sells, buys = txns["sells"], txns["buys"]
            total_sell = sum(self._to_base(s.amount, s.currency, user, exchange_rates) for s in sells)
            total_buy  = sum(self._to_base(b.amount, b.currency, user, exchange_rates) for b in buys)
            cash       = original_cash.get(acc_id, 0.0)
            cash_needed = max(0.0, total_buy - cash)
            if total_sell > 0 and cash_needed < total_sell:
                ratio = min(1.0, (cash_needed * 1.02) / total_sell)
                for s in sells:
                    s.quantity = int(s.quantity * ratio)
                    s.amount  *= ratio
            result.extend(sells)
            result.extend(buys)
        return result

    def _consolidate_transactions(self, transactions: list) -> list:
        """
        1. Merge duplicate BUYs for the same (account, security).
        2. Net out SELL+BUY pairs for the same (account, security)
           to avoid selling and immediately re-buying the same ticker.
        """
        sell_map: dict = {}
        buy_map:  dict = {}
        result = []

        for txn in transactions:
            if txn.security_id is None:
                result.append(txn) 
                continue
            key = (txn.account_id, txn.security_id)
            if txn.action == "SELL":
                if key in sell_map:
                    sell_map[key].quantity += txn.quantity
                    sell_map[key].amount   += txn.amount
                else:
                    sell_map[key] = txn
            else:
                if key in buy_map:
                    buy_map[key].quantity += txn.quantity
                    buy_map[key].amount   += txn.amount
                else:
                    buy_map[key] = txn

        all_keys = set(list(sell_map.keys()) + list(buy_map.keys()))

        for key in all_keys:
            sell = sell_map.get(key)
            buy  = buy_map.get(key)
            if sell and buy:
                net_qty = buy.quantity - sell.quantity
                if net_qty > 0:
                    buy.quantity = net_qty
                    buy.amount   = net_qty * buy.price
                    result.append(buy)
                    log.debug("NET BUY sec_id=%s account_id=%s qty=%d", key[1], key[0], net_qty)
                elif net_qty < 0:
                    sell.quantity = abs(net_qty)
                    sell.amount   = abs(net_qty) * sell.price
                    result.append(sell)
                    log.debug("NET SELL sec_id=%s account_id=%s qty=%d", key[1], key[0], abs(net_qty))
                else:
                    log.debug("CANCELLED sec_id=%s account_id=%s equal qty", key[1], key[0])
            elif sell:
                result.append(sell)
            else:
                result.append(buy_map[key])

        result.sort(key=lambda t: (0 if t.action == "SELL" else 1))
        for i, txn in enumerate(result, 1):
            txn.execution_order = i
        return result

    def _recalculate_deltas(self, deltas, transactions, user, exchange_rates):
        delta_map = {d["asset_class_id"]: dict(d) for d in deltas}
        portfolio_total = sum(d["target_value"] for d in deltas) or 0
        for txn in transactions:
            if not txn.security_id:
                continue
            sec = Security.query.get(txn.security_id)
            if not sec or sec.asset_class_id not in delta_map:
                continue
            amount_base = self._to_base(txn.amount, txn.currency, user, exchange_rates)
            d = delta_map[sec.asset_class_id]
            if txn.action == "BUY":
                d["current_value"] += amount_base
                d["dollar_diff"]   -= amount_base
            elif txn.action == "SELL":
                d["current_value"] -= amount_base
                d["dollar_diff"]   += amount_base
            if portfolio_total > 0:
                d["percentage_diff"] = (d["current_value"] / portfolio_total * 100) - d["target"].target_percentage

        return list(delta_map.values())

    def _precision_tune(self, user, deltas, account_cash, transactions, execution_order, exchange_rates):
        if not getattr(user, "precision_rebalancing", False):
            return transactions, execution_order
        underweight = sorted(
            [(d["asset_class_id"], d["asset_class_name"], d["dollar_diff"], abs(d["percentage_diff"]))
             for d in deltas if d["dollar_diff"] > 0],
            key=lambda x: -x[3],
        )
        remaining = {ac_id: amt for ac_id, _, amt, _ in underweight}
        for account in user.accounts:
            cash = account_cash.get(account.id, 0.0)
            if cash < 500:
                continue
            for ac_id, _, _, pct_diff in underweight:
                if remaining.get(ac_id, 0) < 1 or cash < 100:
                    continue
                if self.name in ("Minimize-Positions", "Cash-Efficient"):
                    if not any(h.security and h.security.asset_class_id == ac_id for h in account.holdings):
                        continue

                eligible = self._eligible_securities(ac_id, account, user)
                if not eligible:
                    continue
                my_priority = min(e.get("priority", 99) for e in eligible)
                if my_priority > 1:
                    best_possible_priority = min(
                        (
                            min(
                                (e.get("priority", 99) for e in self._eligible_securities(ac_id, a, user)),
                                default=99
                            )
                            for a in user.accounts
                            if a.id != account.id and self._eligible_securities(ac_id, a, user)
                        ),
                        default=99,
                    )
                    if my_priority > best_possible_priority:
                        log.info("PRIORITY_SKIP precision_tune account=%s ac_id=%s", account.name, ac_id)
                        continue
                has_explicit_priority_pref = any(
                    p.restriction_type == "prioritized_accounts"
                    for p in SecurityPreference.query.filter_by(user_id=user.id).all()
                    if Security.query.get(p.security_id) is not None
                    and Security.query.get(p.security_id).asset_class_id == ac_id
                )
                if has_explicit_priority_pref:
                    if my_priority >= 99:
                        log.info("PRIORITY_SKIP precision_tune (explicit pref) account=%s ac_id=%s", account.name, ac_id)
                        continue

                amount_to_buy = min(cash, remaining[ac_id])

                overweight_ids = {d["asset_class_id"] for d in deltas if d["percentage_diff"] > 0}
                if ac_id in overweight_ids:
                    continue

                txn = self._create_buy_transaction(
                    user, account, ac_id, amount_to_buy, execution_order,
                    prefer_existing=True, exchange_rates=exchange_rates,
                )
                if txn:
                    actual = self._to_base(txn.amount, txn.currency, user, exchange_rates)
                    if actual < 500:
                        continue
                    transactions.append(txn)
                    execution_order += 1
                    remaining[ac_id]        -= actual
                    cash                    -= actual
                    account_cash[account.id] = cash
        return transactions, execution_order

    def _execute_sells(self, user, accounts_sorted, overweight, remaining_to_sell,
                       account_cash, transactions, execution_order, exchange_rates):
        for account in accounts_sorted:
            for ac_id, _, _, _ in overweight:
                if remaining_to_sell.get(ac_id, 0) < 1:
                    continue
                for holding in account.holdings:
                    if remaining_to_sell[ac_id] < 1:
                        break
                    if not holding.security or holding.security.asset_class_id != ac_id:
                        continue
                    sell_value = min(remaining_to_sell[ac_id], holding.market_value)
                    txn = self._create_sell_transaction(user, account, holding, sell_value, execution_order)
                    if txn:
                        if txn.amount < 500:   # skip tiny sells
                            continue
                        transactions.append(txn)
                        execution_order += 1
                        actual_base = self._to_base(txn.amount, txn.currency, user, exchange_rates)
                        account_cash[account.id] += actual_base
                        remaining_to_sell[ac_id] -= actual_base
        return transactions, execution_order, account_cash

    def _execute_buys(self, user, accounts_sorted, underweight, remaining_to_buy,
                      account_cash, transactions, execution_order, require_existing, exchange_rates):
        for account in accounts_sorted:
            for ac_id, _, _, _ in underweight:
                if remaining_to_buy.get(ac_id, 0) < 1:
                    continue
                cash = account_cash.get(account.id, 0.0)
                if cash < 500:
                    continue

                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id
                    for h in account.holdings
                )
                if require_existing and not has_existing:
                    continue

                eligible = self._eligible_securities(ac_id, account, user)
                if not eligible:
                    continue
                my_priority = min(e.get("priority", 99) for e in eligible)
                log.info("PRIORITY_CHECK _execute_buys account=%s ac_id=%s my_priority=%s require_existing=%s", account.name, ac_id, my_priority, require_existing)

                if my_priority > 1:
                    best_possible_priority = min(
                        (
                            min(
                                (e.get("priority", 99) for e in self._eligible_securities(ac_id, a, user)),
                                default=99
                            )
                            for a in user.accounts
                            if a.id != account.id
                            and self._eligible_securities(ac_id, a, user)
                        ),
                        default=99,
                    )
                    log.info("PRIORITY_CHECK _execute_buys account=%s ac_id=%s best_possible=%s", account.name, ac_id, best_possible_priority)
                    if my_priority > best_possible_priority:
                        log.info("PRIORITY_SKIP account=%s ac_id=%s", account.name, ac_id)
                        continue

                has_explicit_priority_pref = any(
                    p.restriction_type == "prioritized_accounts"
                    for p in SecurityPreference.query.filter_by(user_id=user.id).all()
                    if Security.query.get(p.security_id) is not None
                    and Security.query.get(p.security_id).asset_class_id == ac_id
                )
                if has_explicit_priority_pref:
                    if my_priority >= 99:
                        continue
                else:
                    portfolio_has_class = db.session.query(
                        db.session.query(Holding).join(Security).join(Account)
                        .filter(Account.user_id == user.id)
                        .filter(Security.asset_class_id == ac_id)
                        .exists()
                    ).scalar()
                    if portfolio_has_class and not has_existing:
                        account_has_class = db.session.query(
                            db.session.query(Holding)
                            .filter(Holding.account_id == account.id)
                            .join(Security)
                            .filter(Security.asset_class_id == ac_id)
                            .exists()
                        ).scalar()
                        if not account_has_class:
                            continue
    
                amount_to_buy = min(remaining_to_buy[ac_id], cash)
    
                # Top up existing pending BUY for same asset class in same account
                existing_buy = next(
                    (t for t in transactions
                    if t.action == "BUY"
                    and t.account_id == account.id
                    and t.security_id is not None
                    and Security.query.get(t.security_id) is not None
                    and Security.query.get(t.security_id).asset_class_id == ac_id),
                    None
                )
                if existing_buy and existing_buy.price and existing_buy.price > 0:
                    extra_qty = int(amount_to_buy / existing_buy.price)
                    if extra_qty > 0:
                        actual = extra_qty * existing_buy.price
                        existing_buy.quantity += extra_qty
                        existing_buy.amount   += actual
                        remaining_to_buy[ac_id]  -= actual
                        account_cash[account.id] -= actual
                        log.debug("Topped up BUY ac_id=%s account_id=%s +qty=%d", ac_id, account.id, extra_qty)
                    continue

                txn = self._create_buy_transaction(
                    user, account, ac_id, amount_to_buy, execution_order,
                    exchange_rates=exchange_rates,
                )
                if txn:
                    actual = self._to_base(txn.amount, txn.currency, user, exchange_rates)
                    if actual < 500:   # skip tiny transactions
                        continue
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id]   -= actual
                    account_cash[account.id]  -= actual
        return transactions, execution_order, account_cash

    def _execute_targeted_sells(self, user, underweight, remaining_to_buy, account_cash,
                                 transactions, execution_order, exchange_rates):
        """
        Phase 1.5 ~@~T For constrained asset classes still needing funds, sell overweight
        holdings *within the same priority account* rather than relying on global sells.
        This prevents constrained buys (e.g. WSE200 in RRSP) from being starved of cash.
        """
        for ac_id, ac_name, _, _ in underweight:
            if remaining_to_buy.get(ac_id, 0) < 500:
                continue

            # Find accounts that are the best (lowest) priority for this asset class
            priority_accounts = []
            for account in user.accounts:
                eligible = self._eligible_securities(ac_id, account, user)
                if not eligible:
                    continue
                best_priority = min(e.get("priority", 99) for e in eligible)
                priority_accounts.append((best_priority, account))

            if not priority_accounts:
                continue

            # Only act on the highest-priority account(s)
            min_priority = min(p for p, _ in priority_accounts)
            if min_priority >= 99:
                # No explicit priority set ~@~T skip targeted sell, let global phase handle it
                continue

            for prio, account in sorted(priority_accounts, key=lambda x: x[0]):
                if prio > min_priority:
                    break
                cash_needed = remaining_to_buy[ac_id] - account_cash.get(account.id, 0.0)
                if cash_needed <= 0:
                    continue

                # Sell overweight holdings in this account (non-constrained asset classes first)
                for holding in sorted(account.holdings, key=lambda h: (
                    h.security.asset_class_id == ac_id if h.security else True,
                )):
                    if cash_needed <= 0:
                        break
                    if not holding.security or holding.market_value < 500:
                        continue
                    if holding.security.asset_class_id == ac_id:
                        continue  # Don't sell the thing we're trying to buy
                    sell_value = min(cash_needed, holding.market_value)
                    txn = self._create_sell_transaction(
                        user, account, holding, sell_value, execution_order
                    )
                    if txn:
                        if txn.amount < 500:
                            continue
                        actual_base = self._to_base(txn.amount, txn.currency, user, exchange_rates)
                        transactions.append(txn)
                        execution_order += 1
                        account_cash[account.id] = account_cash.get(account.id, 0.0) + actual_base  # use base currency
                        cash_needed -= actual_base
                        # remaining_to_buy is decremented by _execute_buys when the actual buy occurs
                        log.info(
                            "TARGETED_SELL ac_id=%s account=%s ticker=%s amount=%.2f",
                            ac_id, account.name,
                            holding.security.ticker if holding.security else "?",
                            txn.amount,
                        )

        return transactions, execution_order, account_cash
    def _execute_cross_account_swaps(self, user, underweight, account_cash,
                                      transactions, execution_order, exchange_rates):
        """
        Phase 0 — Cross-account swaps.
        For constrained asset classes where the priority account lacks cash, sell a
        relocatable holding in that account and re-buy it in another eligible account,
        freeing cash in the priority account for the constrained buy.
        """
        for ac_id, ac_name, needed_amt, _ in underweight:
            priority_accounts = []
            for account in user.accounts:
                eligible = self._eligible_securities(ac_id, account, user)
                if not eligible:
                    continue
                best_priority = min(e.get("priority", 99) for e in eligible)
                priority_accounts.append((best_priority, account))

            if not priority_accounts:
                continue

            min_priority = min(p for p, _ in priority_accounts)
            if min_priority >= 99:
                continue  # No explicit prioritization — skip

            priority_account_ids = {a.id for _, a in priority_accounts}

            for prio, target_account in sorted(priority_accounts, key=lambda x: x[0]):
                if prio > min_priority:
                    break
                cash_available = account_cash.get(target_account.id, 0.0)
                if cash_available >= needed_amt * 0.8:
                    continue  # Close enough — don't churn

                cash_gap = needed_amt - cash_available

                for holding in target_account.holdings:
                    if cash_gap <= 0:
                        break
                    if not holding.security or holding.market_value < 500:
                        continue
                    if holding.security.asset_class_id == ac_id:
                        continue  # Don't relocate the constrained class itself

                    # Find a destination — prefer non-priority accounts to avoid circular chains
                    destination = None
                    for other_account in user.accounts:
                        if other_account.id == target_account.id:
                            continue
                        if other_account.id in priority_account_ids:
                            continue  # Don't swap between priority accounts
                        if self._eligible_securities(holding.security.asset_class_id, other_account, user):
                            destination = other_account
                            break
                    # Fall back to any eligible account if no non-priority destination found
                    if not destination:
                        for other_account in user.accounts:
                            if other_account.id == target_account.id:
                                continue
                            if self._eligible_securities(holding.security.asset_class_id, other_account, user):
                                destination = other_account
                                break
                    if not destination:
                        continue

                    swap_value = min(cash_gap, holding.market_value)
                    sell_txn = self._create_sell_transaction(
                        user, target_account, holding, swap_value, execution_order
                    )
                    if not sell_txn:
                        continue

                    price_base = self._to_base(
                        holding.price, holding.security.currency, user, exchange_rates
                    )
                    buy_qty = int(swap_value / price_base) if price_base > 0 else 0
                    if buy_qty < 1:
                        continue

                    buy_txn = RebalanceTransaction(
                        user_id=user.id,
                        account_id=destination.id,
                        security_id=holding.security_id,
                        action="BUY",
                        quantity=buy_qty,
                        price=holding.price,
                        amount=buy_qty * holding.price,
                        currency=holding.security.currency,
                        execution_order=execution_order + 1,
                        is_final_trade=False,
                    )

                    actual_sell = self._to_base(sell_txn.amount, sell_txn.currency, user, exchange_rates)
                    transactions.append(sell_txn)
                    transactions.append(buy_txn)
                    execution_order += 2
                    account_cash[target_account.id] = account_cash.get(target_account.id, 0.0) + sell_txn.amount
                    account_cash[destination.id] = account_cash.get(destination.id, 0.0) - buy_txn.amount
                    cash_gap -= actual_sell

                    log.info(
                        "CROSS_ACCOUNT_SWAP ac_id=%s sell_account=%s buy_account=%s ticker=%s amount=%.2f",
                        ac_id, target_account.name, destination.name,
                        holding.security.ticker, sell_txn.amount,
                    )
                    break  # Only one swap per account per asset class

        return transactions, execution_order, account_cash

    def _assemble_plan(self, user, deltas, overweight, underweight, account_cash,
                       exchange_rates, accounts_sorted, require_existing,
                       cash_phase_accounts=None):
        transactions    = []
        execution_order = 1
        remaining_to_buy  = {ac_id: abs(amt) for ac_id, _, amt, _ in underweight}
        remaining_to_sell = {ac_id: abs(amt) for ac_id, _, amt, _ in overweight}
        account_cash      = copy.deepcopy(account_cash)
        original_cash     = copy.deepcopy(account_cash)

        # Phase 0: cross-account swaps to fund constrained buys
        #transactions, execution_order, account_cash = self._execute_cross_account_swaps(
        #    user, underweight, account_cash, transactions, execution_order, exchange_rates
        #)

        # Phase 1: deploy idle cash
        ph1_accounts = cash_phase_accounts if cash_phase_accounts is not None else user.accounts
        transactions, execution_order, account_cash = self._execute_buys(
            user, ph1_accounts, underweight, remaining_to_buy, account_cash,
            transactions, execution_order, require_existing, exchange_rates,
        )

        # Phase 1.5: targeted sells within priority accounts to fund remaining constrained buys
        transactions, execution_order, account_cash = self._execute_targeted_sells(
            user, underweight, remaining_to_buy, account_cash,
            transactions, execution_order, exchange_rates,
        )

        # Phase 2: global sells
        def account_can_buy(acc):
            if not require_existing:
                return True
            return any(
                h.security and h.security.asset_class_id == ac_id
                for h in acc.holdings
                for ac_id, _, amt, _ in underweight
                if remaining_to_buy.get(ac_id, 0) > 1
            )

        sell_accounts = [a for a in accounts_sorted if not require_existing or account_can_buy(a)]
        transactions, execution_order, account_cash = self._execute_sells(
            user, sell_accounts, overweight, remaining_to_sell, account_cash,
            transactions, execution_order, exchange_rates
        )

        # Phase 2b: apply sell limiting HERE, before Phase 3
        transactions = self._apply_sell_limiting(transactions, original_cash, user, exchange_rates)

        # Reset all account cash to original before accumulating sell amounts
        account_cash = {k: v for k, v in original_cash.items()}

        # Recompute account_cash to reflect limited sell amounts
        for txn in transactions:
            if txn.action == "SELL":
                actual_base = self._to_base(txn.amount, txn.currency, user, exchange_rates)
                account_cash[txn.account.id] += actual_base

        # Phase 3: buys funded by sell proceeds
        transactions, execution_order, account_cash = self._execute_buys(
            user, accounts_sorted, underweight, remaining_to_buy, account_cash,
            transactions, execution_order, require_existing, exchange_rates,
        )

        # Phase 4: precision tune
        updated_deltas = self._recalculate_deltas(deltas, transactions, user, exchange_rates)
        transactions, execution_order = self._precision_tune(
            user, updated_deltas, account_cash, transactions, execution_order, exchange_rates,
        )

        transactions = self._consolidate_transactions(transactions)
        return TransactionPlan(transactions, {"strategy": self.name})

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        raise NotImplementedError


# ============================================================================
# Concrete Strategies
# ============================================================================

class CashFirstStrategy(RebalancingStrategy):
    def __init__(self):
        super().__init__("Cash-First")

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        accounts_sorted = sorted(user.accounts, key=lambda a: (not a.is_registered, a.name))
        return self._assemble_plan(
            user, deltas, overweight, underweight, account_cash, exchange_rates,
            accounts_sorted=accounts_sorted, require_existing=False,
        )


class MinimizePositionsStrategy(RebalancingStrategy):
    def __init__(self):
        super().__init__("Minimize-Positions")

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        accounts_sorted = sorted(user.accounts, key=lambda a: (not a.is_registered, a.name))
        return self._assemble_plan(
            user, deltas, overweight, underweight, account_cash, exchange_rates,
            accounts_sorted=accounts_sorted, require_existing=True,
        )


class CashEfficientStrategy(RebalancingStrategy):
    def __init__(self):
        super().__init__("Cash-Efficient")

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        accounts_sorted = sorted(user.accounts, key=lambda a: (not a.is_registered, a.name))
        return self._assemble_plan(
            user, deltas, overweight, underweight, account_cash, exchange_rates,
            accounts_sorted=accounts_sorted, require_existing=True,
        )


class TaxOptimizedStrategy(RebalancingStrategy):
    def __init__(self):
        super().__init__("Tax-Optimized")

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        accounts_sorted = sorted(
            user.accounts,
            key=lambda a: (not a.is_registered, -(account_cash.get(a.id, 0))),
        )
        return self._assemble_plan(
            user, deltas, overweight, underweight, account_cash, exchange_rates,
            accounts_sorted=accounts_sorted, require_existing=False,
            cash_phase_accounts=accounts_sorted,
        )


class HeuristicStrategy(RebalancingStrategy):
    def __init__(self):
        super().__init__("Heuristic")

    def _score(self, account, ac_id, cash, pct_diff, has_existing):
        score  = abs(pct_diff) * 10
        score += 200 if has_existing else -100
        score += 50  if account.is_registered else 0
        score += min(cash, 10_000) / 100
        if has_existing and account.is_registered:
            score += 150
        log.info("SCORE account=%s ac_id=%s has_existing=%s is_registered=%s score=%.1f",
                  account.name, ac_id, has_existing, account.is_registered, score)
        return score

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        transactions    = []
        execution_order = 1
        remaining_to_buy  = {ac_id: abs(amt) for ac_id, _, amt, _ in underweight}
        remaining_to_sell = {ac_id: abs(amt) for ac_id, _, amt, _ in overweight}
        account_cash      = copy.deepcopy(account_cash)
        original_cash     = copy.deepcopy(account_cash)

        # Phase 0: cross-account swaps
        #transactions, execution_order, account_cash = self._execute_cross_account_swaps(
        #    user, underweight, account_cash, transactions, execution_order, exchange_rates
        #)

        # Pre-sort underweight by constraint score: most constrained asset classes first.
        # This ensures scarce securities (e.g. WSE200, WSE300) get scheduled before
        # flexible ones (e.g. US Equity with ITOT/XUU/DI-U).
        underweight = sorted(
            underweight,
            key=lambda x: self._compute_constraint_score(user, x[0])
        )
        log.info(
            "CONSTRAINT_ORDER %s",
            [(ac_name, self._compute_constraint_score(user, ac_id))
             for ac_id, ac_name, _, _ in underweight]
        )

        # Phase 1: greedy cash deployment
        # Process underweight asset classes in constraint order (most constrained first).
        # For each asset class, fully exhaust available cash before moving to the next.
        for ac_id, _, _, pct_diff in underweight:
            while remaining_to_buy.get(ac_id, 0) >= 500:
                best_score = -999
                best_choice = None

                for account in user.accounts:
                    cash = account_cash.get(account.id, 0.0)
                    if cash < 500:
                        continue

                    eligible = self._eligible_securities(ac_id, account, user)
                    if not eligible:
                        continue

                    my_priority = min(e.get("priority", 99) for e in eligible)
                    log.info("PRIORITY_CHECK heuristic_phase1 account=%s ac_id=%s my_priority=%s",
                             account.name, ac_id, my_priority)

                    if my_priority > 1:
                        best_possible_priority = min(
                            (
                                min(
                                    (e.get("priority", 99) for e in self._eligible_securities(ac_id, a, user)),
                                    default=99
                                )
                                for a in user.accounts
                                if a.id != account.id and self._eligible_securities(ac_id, a, user)
                            ),
                            default=99,
                        )
                        log.info("PRIORITY_CHECK _execute_buys account=%s ac_id=%s best_possible=%s",
                                 account.name, ac_id, best_possible_priority)
                        if my_priority > best_possible_priority:
                            log.info("PRIORITY_SKIP heuristic_phase1 account=%s ac_id=%s",
                                     account.name, ac_id)
                            continue

                    has_existing = any(
                        h.security and h.security.asset_class_id == ac_id
                        for h in account.holdings
                    )

                    has_explicit_priority_pref = any(
                        p.restriction_type == "prioritized_accounts"
                        for p in SecurityPreference.query.filter_by(user_id=user.id).all()
                        if Security.query.get(p.security_id) is not None
                        and Security.query.get(p.security_id).asset_class_id == ac_id
                    )
                    if has_explicit_priority_pref:
                        if my_priority >= 99:
                            continue
                    else:
                        portfolio_has_class = db.session.query(
                            db.session.query(Holding).join(Security).join(Account)
                            .filter(Account.user_id == user.id)
                            .filter(Security.asset_class_id == ac_id)
                            .exists()
                        ).scalar()
                        if portfolio_has_class and not has_existing:
                            account_has_class = db.session.query(
                                db.session.query(Holding)
                                .filter(Holding.account_id == account.id)
                                .join(Security)
                                .filter(Security.asset_class_id == ac_id)
                                .exists()
                            ).scalar()
                            if not account_has_class:
                                continue

                    sc = self._score(account, ac_id, cash, pct_diff, has_existing)
                    if sc > best_score:
                        best_score = sc
                        best_choice = (account, ac_id, min(cash, remaining_to_buy[ac_id]))

                if not best_choice:
                    break

                account, ac_id, amount_to_buy = best_choice
                if amount_to_buy < 500:
                    break

                existing_buy = next(
                    (t for t in transactions
                     if t.action == "BUY" and t.account_id == account.id
                     and t.security_id is not None
                     and Security.query.get(t.security_id) is not None
                     and Security.query.get(t.security_id).asset_class_id == ac_id),
                    None
                )
                if existing_buy and existing_buy.price and existing_buy.price > 0:
                    extra_qty = int(amount_to_buy / existing_buy.price)
                    if extra_qty > 0:
                        actual = extra_qty * existing_buy.price
                        existing_buy.quantity += extra_qty
                        existing_buy.amount += actual
                        remaining_to_buy[ac_id] -= actual
                        account_cash[account.id] -= actual
                else:
                    txn = self._create_buy_transaction(
                        user, account, ac_id, amount_to_buy, execution_order,
                        exchange_rates=exchange_rates,
                    )
                    if txn:
                        actual = self._to_base(txn.amount, txn.currency, user, exchange_rates)
                        transactions.append(txn)
                        execution_order += 1
                        remaining_to_buy[ac_id] -= actual
                        account_cash[account.id] -= actual
                    else:
                        break  # _create_buy_transaction returned None, no point retrying

        # Phase 1.5: targeted sells within priority accounts
        transactions, execution_order, account_cash = self._execute_targeted_sells(
            user, underweight, remaining_to_buy, account_cash,
            transactions, execution_order, exchange_rates,
        )

        # Phase 2: sells (registered first)
        accounts_sorted = sorted(user.accounts, key=lambda a: (not a.is_registered, a.name))
        transactions, execution_order, account_cash = self._execute_sells(
            user, accounts_sorted, overweight, remaining_to_sell, account_cash,
            transactions, execution_order, exchange_rates
        )

        # Phase 3: buys after sells
        transactions, execution_order, account_cash = self._execute_buys(
            user, accounts_sorted, underweight, remaining_to_buy, account_cash,
            transactions, execution_order, require_existing=False,
            exchange_rates=exchange_rates,
        )

        # Phase 4: precision tune
        updated_deltas = self._recalculate_deltas(deltas, transactions, user, exchange_rates)
        transactions, execution_order = self._precision_tune(
            user, updated_deltas, account_cash, transactions, execution_order, exchange_rates,
        )

        transactions = self._apply_sell_limiting(transactions, original_cash, user, exchange_rates)
        transactions = self._consolidate_transactions(transactions)
        return TransactionPlan(transactions, {"strategy": self.name})


# ============================================================================
# Engine entry-point
# ============================================================================

_STRATEGIES = [
    CashFirstStrategy(),
    MinimizePositionsStrategy(),
    CashEfficientStrategy(),
    TaxOptimizedStrategy(),
    HeuristicStrategy(),
]


def generate_rebalance_transactions(user) -> list:
    from services.fx import get_exchange_rates
    exchange_rates = get_exchange_rates(user)

    deltas, total_portfolio = calculate_asset_class_deltas(user, exchange_rates)
    if total_portfolio <= 0:
        raise ValueError("Portfolio value is zero; cannot generate rebalance plan.")

    threshold = getattr(user, "balanced_threshold", 0.5)

    overweight  = [(d["asset_class_id"], d["asset_class_name"], abs(d["dollar_diff"]), d["percentage_diff"])
                   for d in deltas if d["percentage_diff"] >  threshold]
    underweight = [(d["asset_class_id"], d["asset_class_name"], abs(d["dollar_diff"]), d["percentage_diff"])
                   for d in deltas if d["percentage_diff"] < -threshold]

    for d in deltas:
        log.info(
            "DELTA | %s | current=%.2f%% target=%.2f%% diff=%.2f%% dollar_diff=%.2f",
            d["asset_class_name"], d["current_pct"], d["target_pct"],
            d["percentage_diff"], d["dollar_diff"]
        )
    log.info("Overweight: %s", overweight)
    log.info("Underweight: %s", underweight)

    if not overweight and not underweight:
        log.info("Portfolio is within threshold; no rebalancing needed.")
        return []

    account_cash = {
        a.id: convert_to_base(a.cash_balance or 0.0, a.currency, user.base_currency, exchange_rates)
        for a in user.accounts
    }

    helper = RebalancingStrategy("tmp")
    best_plan = None
    best_residual = None

    for strategy in _STRATEGIES:
        try:
            plan = strategy.generate(
                user, deltas, overweight, underweight,
                copy.deepcopy(account_cash), exchange_rates,
            )
            updated = helper._recalculate_deltas(deltas, plan.transactions, user, exchange_rates)
            residual = max(abs(d["percentage_diff"]) for d in updated) if updated else 999

            if (best_plan is None or
                residual < best_residual or
                (residual == best_residual and plan.score(user) < best_plan.score(user))):
                best_plan     = plan
                best_residual = residual
                log.info("New best plan: %s residual=%.3f score=%s",
                         strategy.name, residual, plan.score(user))
        except Exception as exc:
            log.error("Strategy %s failed: %s", strategy.name, exc, exc_info=True)

    if best_plan is None:
        raise RuntimeError("All rebalancing strategies failed.")

    RebalanceTransaction.query.filter_by(user_id=user.id, executed=False).delete()
    for txn in best_plan.transactions:
        db.session.add(txn)
    db.session.commit()

    log.info(
        "Rebalance plan generated: %d transactions via %s strategy.",
        len(best_plan), best_plan.metadata.get("strategy"),
    )

    for txn in best_plan.transactions:
        log.info(
            "TXN | %s | %s | %s | qty=%.4f price=%.2f amount=%.2f %s",
            best_plan.metadata.get("strategy"),
            txn.action,
            txn.account.name if txn.account else txn.account_id,
            txn.quantity,
            txn.price,
            txn.amount,
            txn.security.ticker if txn.security else "N/A",
        )
    log.info("Chosen plan strategy=%s, num_txns=%d",
             best_plan.metadata.get("strategy"), len(best_plan))

    return best_plan.transactions

