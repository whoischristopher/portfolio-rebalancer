"""
rebalancer.py – Portfolio Rebalancing Engine

Generates optimal rebalancing transactions using four strategies:
  - CashFirst:          Deploy idle cash first, then sell overweight positions
  - MinimizePositions:  Never open new positions; only top-up existing holdings
  - CashEfficient:      Like MinimizePositions but cross-deploys residual cash
  - TaxOptimized:       Prioritise registered accounts for all trading
  - Heuristic:          Score-based optimisation balancing all factors

The engine evaluates all strategies and returns the best plan based on:
  fewer transactions > fewer new positions > more registered sells > tightest delta closure.
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

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ============================================================================
# TransactionPlan
# ============================================================================

class TransactionPlan:
    """Container for a complete rebalancing plan produced by one strategy."""

    def __init__(self, transactions: list, metadata: dict = None):
        self.transactions = transactions
        self.metadata = metadata or {}

    def __len__(self):
        return len(self.transactions)

    def score(self) -> tuple:
        """
        Lower score = better plan.
        Tuple comparison: (new_positions, total_transactions, -registered_sells)
        """
        new_positions = 0
        registered_sells = 0
        accounts_cache: dict = {}

        for txn in self.transactions:
            acc = accounts_cache.setdefault(txn.account_id, Account.query.get(txn.account_id))
            if txn.action == "BUY" and txn.security_id:
                if not any(h.security_id == txn.security_id for h in acc.holdings):
                    new_positions += 1
            elif txn.action == "SELL" and acc and acc.is_registered:
                registered_sells += 1

        return (new_positions, len(self.transactions), -registered_sells)


# ============================================================================
# Base Strategy
# ============================================================================

class RebalancingStrategy:
    """Shared helpers used by all concrete strategies."""

    def __init__(self, name: str):
        self.name = name

    # ------------------------------------------------------------------
    # Currency helpers
    # ------------------------------------------------------------------

    def _to_base(self, amount: float, from_currency: str, user, exchange_rates: dict) -> float:
        return convert_to_base(amount, from_currency, user.base_currency, exchange_rates)

    def _from_base(self, amount: float, to_currency: str, user, exchange_rates: dict) -> float:
        return convert_to_base(amount, user.base_currency, to_currency, exchange_rates)

    # ------------------------------------------------------------------
    # Eligible securities
    # ------------------------------------------------------------------

    def _eligible_securities(self, asset_class_id: int, account: Account, user) -> list:
        """
        Return list of dicts {security, existing} for securities that
        may be held in *account* given user preferences.
        """
        securities = Security.query.filter_by(asset_class_id=asset_class_id).all()
        prefs = {
            p.security_id: p
            for p in SecurityPreference.query.filter_by(user_id=user.id).all()
        }
        result = []
        for sec in securities:
            pref = prefs.get(sec.id)
            allowed = True
            if pref and pref.restriction_type == "restricted_to_accounts":
                allowed_ids = (pref.account_config or {}).get("allowed", [])
                allowed = account.id in allowed_ids
            if allowed:
                existing = any(h.security_id == sec.id for h in account.holdings)
                result.append({"security": sec, "existing": existing})
        return result

    # ------------------------------------------------------------------
    # Create transactions
    # ------------------------------------------------------------------

    def _create_sell_transaction(
        self, user, account: Account, holding: Holding,
        sell_value: float, execution_order: int
    ) -> RebalanceTransaction | None:
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

    def _create_buy_transaction(
        self, user, account: Account, asset_class_id: int,
        amount_base: float, execution_order: int,
        prefer_existing: bool = True,
        exchange_rates: dict = None,
    ) -> RebalanceTransaction | None:
        eligible = self._eligible_securities(asset_class_id, account, user)
        if not eligible:
            return None
        if prefer_existing:
            eligible.sort(key=lambda x: (not x["existing"], x["security"].id))

        # Multiple choices → defer to user
        if len(eligible) > 1:
            return RebalanceTransaction(
                user_id=user.id,
                account_id=account.id,
                action="BUY",
                quantity=0,
                price=0,
                amount=amount_base,
                currency=account.currency,
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

        amount_in_sec_currency = self._from_base(amount_base, sec.currency, user, exchange_rates or {})
        quantity = int(amount_in_sec_currency / holding.price)
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

    # ------------------------------------------------------------------
    # Post-processing helpers
    # ------------------------------------------------------------------

    def _apply_sell_limiting(
        self, transactions: list, original_cash: dict, user, exchange_rates: dict
    ) -> list:
        """
        Per-account: only sell what is needed to fund buys + a small buffer.
        Prevents orphaned cash from sitting idle after rebalancing.
        """
        by_account: dict = defaultdict(lambda: {"sells": [], "buys": []})
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
//                ratio = min(1.0, (cash_needed * 1.02) / total_sell)  # 2% buffer
                ratio = 1.0
                for s in sells:
                    s.quantity = int(s.quantity * ratio)
                    s.amount  *= ratio

            result.extend(sells)
            result.extend(buys)
        return result

    def _consolidate_transactions(self, transactions: list) -> list:
        """Merge duplicate BUY transactions for the same (account, security)."""
        buy_map: dict = {}
        result = []
        for txn in transactions:
            if txn.action == "SELL":
                result.append(txn)
            else:
                key = (txn.account_id, txn.security_id)
                if key in buy_map:
                    buy_map[key].quantity += txn.quantity
                    buy_map[key].amount   += txn.amount
                else:
                    buy_map[key] = txn
                    result.append(txn)
        for i, txn in enumerate(result, 1):
            txn.execution_order = i
        return result

    def _recalculate_deltas(self, deltas: list, transactions: list, user, exchange_rates: dict) -> list:
        """Return updated deltas after applying pending transactions."""
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
                d["percentage_diff"] = d["target"]["target_percentage"] - (d["current_value"] / portfolio_total * 100)

        return list(delta_map.values())

    def _precision_tune(
        self, user, deltas: list, account_cash: dict,
        transactions: list, execution_order: int, exchange_rates: dict
    ):
        """Phase 4: Deploy remaining idle cash to reduce residual deviations."""
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
            for ac_id, ac_name, _, pct_diff in underweight:
                if remaining.get(ac_id, 0) < 1 or cash < 100:
                    continue
                if self.name in ("Minimize-Positions", "Cash-Efficient"):
                    if not any(h.security and h.security.asset_class_id == ac_id for h in account.holdings):
                        continue
                amount_to_buy = min(cash, remaining[ac_id])
                txn = self._create_buy_transaction(
                    user, account, ac_id, amount_to_buy, execution_order,
                    prefer_existing=True, exchange_rates=exchange_rates,
                )
                if txn:
                    actual = self._to_base(txn.amount, txn.currency, user, exchange_rates)
                    transactions.append(txn)
                    execution_order += 1
                    remaining[ac_id]          -= actual
                    cash                      -= actual
                    account_cash[account.id]   = cash

        return transactions, execution_order

    # ------------------------------------------------------------------
    # Shared sell phase
    # ------------------------------------------------------------------

    def _execute_sells(
        self, user, accounts_sorted: list, overweight: list,
        remaining_to_sell: dict, account_cash: dict,
        transactions: list, execution_order: int,
    ):
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
                    txn = self._create_sell_transaction(
                        user, account, holding, sell_value, execution_order
                    )
                    if txn:
                        transactions.append(txn)
                        execution_order += 1
                        account_cash[account.id]  += txn.amount
                        remaining_to_sell[ac_id]  -= txn.amount
        return transactions, execution_order, account_cash

    # ------------------------------------------------------------------
    # Shared buy phase
    # ------------------------------------------------------------------

    def _execute_buys(
        self, user, accounts_sorted: list, underweight: list,
        remaining_to_buy: dict, account_cash: dict,
        transactions: list, execution_order: int,
        require_existing: bool, exchange_rates: dict,
    ):
        for account in accounts_sorted:
            for ac_id, _, _, _ in underweight:
                if remaining_to_buy.get(ac_id, 0) < 1:
                    continue
                cash = account_cash.get(account.id, 0.0)
                if cash < 100:
                    continue
                if require_existing:
                    has_existing = any(
                        h.security and h.security.asset_class_id == ac_id
                        for h in account.holdings
                    )
                    if not has_existing:
                        continue
                eligible = self._eligible_securities(ac_id, account, user)
                if not eligible:
                    continue
                amount_to_buy = min(remaining_to_buy[ac_id], cash)
                txn = self._create_buy_transaction(
                    user, account, ac_id, amount_to_buy, execution_order,
                    exchange_rates=exchange_rates,
                )
                if txn:
                    actual = self._to_base(txn.amount, txn.currency, user, exchange_rates)
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id]   -= actual
                    account_cash[account.id]  -= actual
        return transactions, execution_order, account_cash

    # ------------------------------------------------------------------
    # Full plan assembly (shared by most strategies)
    # ------------------------------------------------------------------

    def _assemble_plan(
        self, user, deltas: list, overweight: list, underweight: list,
        account_cash: dict, exchange_rates: dict,
        accounts_sorted: list, require_existing: bool,
        cash_phase_accounts=None,
    ) -> TransactionPlan:
        transactions    = []
        execution_order = 1
        remaining_to_buy  = {ac_id: amt for ac_id, _, amt, _ in underweight}
        remaining_to_sell = {ac_id: amt for ac_id, _, amt, _ in overweight}
        account_cash      = copy.deepcopy(account_cash)
        original_cash     = copy.deepcopy(account_cash)

        # Phase 1: deploy idle cash
        ph1_accounts = cash_phase_accounts if cash_phase_accounts is not None else user.accounts
        transactions, execution_order, account_cash = self._execute_buys(
            user, ph1_accounts, underweight, remaining_to_buy, account_cash,
            transactions, execution_order, require_existing, exchange_rates,
        )

        # Phase 2: sells
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
            transactions, execution_order,
        )

        # Phase 3: buys after sells
        transactions, execution_order, account_cash = self._execute_buys(
            user, accounts_sorted, underweight, remaining_to_buy, account_cash,
            transactions, execution_order, require_existing, exchange_rates,
        )

        # Phase 4: precision tune
        updated_deltas = self._recalculate_deltas(deltas, transactions, user, exchange_rates)
        transactions, execution_order = self._precision_tune(
            user, updated_deltas, account_cash, transactions, execution_order, exchange_rates,
        )

        transactions = self._apply_sell_limiting(transactions, original_cash, user, exchange_rates)
        transactions = self._consolidate_transactions(transactions)
        return TransactionPlan(transactions, {"strategy": self.name})

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        raise NotImplementedError


# ============================================================================
# Concrete Strategies
# ============================================================================

class CashFirstStrategy(RebalancingStrategy):
    """Deploy idle cash first, then sell overweight positions."""

    def __init__(self):
        super().__init__("Cash-First")

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        accounts_sorted = sorted(user.accounts, key=lambda a: (not a.is_registered, a.name))
        return self._assemble_plan(
            user, deltas, overweight, underweight, account_cash, exchange_rates,
            accounts_sorted=accounts_sorted, require_existing=False,
        )


class MinimizePositionsStrategy(RebalancingStrategy):
    """Never open new positions; only top-up existing holdings."""

    def __init__(self):
        super().__init__("Minimize-Positions")

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        accounts_sorted = sorted(user.accounts, key=lambda a: (not a.is_registered, a.name))
        return self._assemble_plan(
            user, deltas, overweight, underweight, account_cash, exchange_rates,
            accounts_sorted=accounts_sorted, require_existing=True,
        )


class CashEfficientStrategy(RebalancingStrategy):
    """
    Like MinimizePositions but deploys residual idle cash across all existing
    holdings to minimise orphaned cash after rebalancing.
    """

    def __init__(self):
        super().__init__("Cash-Efficient")

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        accounts_sorted = sorted(user.accounts, key=lambda a: (not a.is_registered, a.name))
        return self._assemble_plan(
            user, deltas, overweight, underweight, account_cash, exchange_rates,
            accounts_sorted=accounts_sorted, require_existing=True,
        )


class TaxOptimizedStrategy(RebalancingStrategy):
    """Prioritise registered accounts for all buying and selling."""

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
    """Score-based optimisation balancing all factors."""

    def __init__(self):
        super().__init__("Heuristic")

    def _score(self, account: Account, ac_id: int, cash: float,
               pct_diff: float, has_existing: bool) -> float:
        score  = abs(pct_diff) * 10
        score += 100 if has_existing else -50
        score += 50  if account.is_registered else 0
        score += min(cash, 10_000) / 100
        return score

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        transactions    = []
        execution_order = 1
        remaining_to_buy  = {ac_id: amt for ac_id, _, amt, _ in underweight}
        remaining_to_sell = {ac_id: amt for ac_id, _, amt, _ in overweight}
        account_cash      = copy.deepcopy(account_cash)
        original_cash     = copy.deepcopy(account_cash)

        # Phase 1: greedily pick best (account, asset_class) each iteration
        for _ in range(len(underweight) * len(user.accounts)):
            best_score  = -999
            best_choice = None

            for account in user.accounts:
                cash = account_cash.get(account.id, 0.0)
                if cash < 100:
                    continue
                for ac_id, _, _, pct_diff in underweight:
                    if remaining_to_buy.get(ac_id, 0) < 1:
                        continue
                    if not self._eligible_securities(ac_id, account, user):
                        continue
                    has_existing = any(
                        h.security and h.security.asset_class_id == ac_id
                        for h in account.holdings
                    )
                    sc = self._score(account, ac_id, cash, pct_diff, has_existing)
                    if sc > best_score:
                        best_score  = sc
                        best_choice = (account, ac_id, min(cash, remaining_to_buy[ac_id]))

            if not best_choice:
                break
            account, ac_id, amount_to_buy = best_choice
            txn = self._create_buy_transaction(
                user, account, ac_id, amount_to_buy, execution_order,
                exchange_rates=exchange_rates,
            )
            if txn:
                actual = self._to_base(txn.amount, txn.currency, user, exchange_rates)
                transactions.append(txn)
                execution_order += 1
                remaining_to_buy[ac_id]   -= actual
                account_cash[account.id]  -= actual

        # Phase 2: sells (registered first)
        accounts_sorted = sorted(user.accounts, key=lambda a: (not a.is_registered, a.name))
        transactions, execution_order, account_cash = self._execute_sells(
            user, accounts_sorted, overweight, remaining_to_sell, account_cash,
            transactions, execution_order,
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
    """
    Run all strategies, pick the best plan, persist transactions and return them.

    Raises on invalid state (no accounts, no targets, zero portfolio value).
    """
    from services.fx import get_exchange_rates
    exchange_rates = get_exchange_rates(user)

    deltas, total_portfolio = calculate_asset_class_deltas(user, exchange_rates)
    if total_portfolio <= 0:
        raise ValueError("Portfolio value is zero; cannot generate rebalance plan.")

    threshold = getattr(user, "balanced_threshold", 0.5)

    overweight  = [(d["asset_class_id"], d["asset_class_name"],  d["dollar_diff"], d["percentage_diff"])
                   for d in deltas if d["percentage_diff"] >  threshold]
    underweight = [(d["asset_class_id"], d["asset_class_name"], -d["dollar_diff"], -d["percentage_diff"])
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

    best_plan: TransactionPlan | None = None
    best_residual = None

    for strategy in _STRATEGIES:
        try:
            plan = strategy.generate(
                user, deltas, overweight, underweight,
                copy.deepcopy(account_cash), exchange_rates,
            )

            # Recalculate deltas after this plan
            from rebalancer import RebalancingStrategy  # ensure imported above
            helper = RebalancingStrategy("tmp")
            updated = helper._recalculate_deltas(deltas, plan.transactions, user, exchange_rates)
            # Max absolute percentage_diff after plan
            residual = max(abs(d["percentage_diff"]) for d in updated)


            if (best_plan is None or 
               residual < best_residual or
               (residual == best_residual and plan.score() < best_plan.score())):
                best_plan = plan
                best_residual = residual
                log.debug("New best plan: %s residual=%.3f score=%s",
                      strategy.name, residual, plan.score())
        except Exception:
            log.exception("Strategy %s failed; skipping.", strategy.name)

    if best_plan is None:
        raise RuntimeError("All rebalancing strategies failed.")

    # Persist
    RebalanceTransaction.query.filter_by(user_id=user.id, executed=False).delete()
    for txn in best_plan.transactions:
        db.session.add(txn)
    db.session.commit()

    log.info(
        "Rebalance plan generated: %d transactions via %s strategy.",
        len(best_plan), best_plan.metadata.get("strategy"),
    )

    for t in best_plan.transactions:
        log.info(
            "TXN | %s | %s | %s | qty=%.4f price=%.2f amount=%.2f %s",
            best_plan.metadata.get("strategy"),
            t.action,
            t.account.name if t.account else t.account_id,
            t.quantity,
            t.price,
            t.amount,
            t.security.ticker if t.security else "N/A",
        )
    log.info("Chosen plan strategy=%s, num_txns=%d",
         best_plan.metadata.get("strategy"), len(best_plan))

    return best_plan.transactions

