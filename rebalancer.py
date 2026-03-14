"""
Portfolio Rebalancing Engine

Generates optimal rebalancing transactions using multi-strategy evaluation.
Each strategy has different priorities:
- Cash-First: Deploy idle cash first, minimize sells
- Minimize-Positions: Never create new positions, consolidate holdings
- Tax-Optimized: Maximize registered account usage for tax efficiency
- Heuristic: Score-based optimization balancing all factors

The engine evaluates all strategies and selects the best based on:
- Number of transactions (fewer is better)
- New positions created (fewer is better)
- Tax efficiency (registered account sells preferred)
- Delta closure (closer to target allocation)
"""

from models import *
from collections import defaultdict
from datetime import datetime
import copy


class TransactionPlan:
    """Container for a complete rebalancing plan"""
    
    def __init__(self, transactions, metadata=None):
        self.transactions = transactions
        self.metadata = metadata or {}
    
    def __len__(self):
        return len(self.transactions)
    
    def get_stats(self):
        """Calculate statistics about this plan"""
        stats = {
            'total_transactions': len(self.transactions),
            'sell_count': sum(1 for t in self.transactions if t.action == 'SELL'),
            'buy_count': sum(1 for t in self.transactions if t.action == 'BUY'),
            'new_positions': 0,
            'registered_sells': 0,
            'accounts_touched': len(set(t.account_id for t in self.transactions))
        }
        
        # Count new positions and registered sells
        for txn in self.transactions:
            if txn.action == 'BUY' and txn.security_id:
                # Check if this creates a new position
                account = Account.query.get(txn.account_id)
                has_existing = any(
                    h.security_id == txn.security_id 
                    for h in account.holdings
                )
                if not has_existing:
                    stats['new_positions'] += 1
            
            if txn.action == 'SELL':
                account = Account.query.get(txn.account_id)
                if account.is_registered:
                    stats['registered_sells'] += 1
        
        return stats


class RebalancingStrategy:
    """Base class for rebalancing strategies"""
    
    def __init__(self, name):
        self.name = name

    def _calculate_optimal_sell_amount(self, remaining_to_buy, remaining_to_sell, 
                                       account_cash, portfolio_value, config, exchange_rates, user):
        """
        Calculate how much to actually sell to avoid excess idle cash.
        
        Only sell what's needed to fund purchases + maintain acceptable cash threshold.
        All calculations done in USD.
        """
        # Helper: Convert account amounts to USD
        def get_account_currency(account_id):
            for acc in user.accounts:
                if acc.id == account_id:
                    return acc.currency
            return 'USD'
        
        def to_usd(amount, currency):
            if currency == 'USD':
                return amount
            rate = exchange_rates.get(f'{currency}_USD', 1.0)
            return amount * rate
        
        # Convert all amounts to USD
        total_buy_needed = sum(remaining_to_buy.values())  # Already in target currency
        
        # Convert account cash to USD
        total_current_cash = sum(
            to_usd(amount, get_account_currency(acc_id)) 
            for acc_id, amount in account_cash.items()
        )
        
        # Remaining to sell is already in target currency
        total_sell_available = sum(remaining_to_sell.values())
        
        # What's our acceptable idle cash limit?
        threshold = config.get('balanced_threshold', 0.005)
        max_idle_cash = portfolio_value * threshold
        
        # Calculate projected idle cash if we sold everything
        projected_idle = total_current_cash + total_sell_available - total_buy_needed
        
        # Check if we'd create too much idle cash
        if projected_idle > max_idle_cash:
            # Limit sells: only sell enough to fund purchases + keep idle at threshold
            optimal_sell = total_buy_needed - total_current_cash + max_idle_cash
            return max(0, optimal_sell)
        else:
            # We need all the sells - no limit
            return total_sell_available

    def _recalculate_deltas(self, deltas, transactions, user, exchange_rates):
        """
        Recalculate deltas after applying a list of pending transactions.
        Returns updated deltas that reflect what still needs to be bought/sold.
        """
        # Create a copy of deltas to modify
        updated_deltas = []
        delta_map = {d['asset_class_id']: dict(d) for d in deltas}

        # Apply each transaction to the deltas
        for txn in transactions:
            if not txn.security_id:
                continue
            
            # Get the security's asset class
            from models import Security
            security = Security.query.get(txn.security_id)
            if not security:
                continue
            
            ac_id = security.asset_class_id
            if ac_id not in delta_map:
                continue
            
            # Convert transaction amount to base currency
            amount_base = self._convert_to_base(
                txn.amount,
                txn.currency,
                user.base_currency,
                exchange_rates
            )
            
            # Update the delta
            if txn.action == 'BUY':
                delta_map[ac_id]['current_value'] += amount_base
                delta_map[ac_id]['dollar_diff'] -= amount_base
            elif txn.action == 'SELL':
                delta_map[ac_id]['current_value'] -= amount_base
                delta_map[ac_id]['dollar_diff'] += amount_base

        # Calculate new portfolio total (doesn't change from internal rebalancing)
        if deltas:
            # Portfolio total = sum of all target values (represents 100%)
            portfolio_total = sum(d['target_value'] for d in deltas)
        else:
            portfolio_total = 0

        for ac_id, delta in delta_map.items():
            if portfolio_total > 0:
                current_pct = (delta['current_value'] / portfolio_total) * 100
                delta['percentage_diff'] = delta['target'].target_percentage - current_pct
            updated_deltas.append(delta)

        return updated_deltas

    def _convert_to_base(self, amount, from_currency, to_currency, exchange_rates):
        """Convert amount from one currency to base currency."""
        if from_currency == to_currency:
            return amount
        
        rate_key = f"{from_currency}_TO_{to_currency}"
        if rate_key in exchange_rates:
            return amount * exchange_rates[rate_key]
        
        # Inverse rate
        inverse_key = f"{to_currency}_TO_{from_currency}"
        if inverse_key in exchange_rates:
            return amount / exchange_rates[inverse_key]
        
        # Fallback: assume 1:1
        return amount

    def _apply_smart_sell_limiting(self, transactions, original_cash, user, exchange_rates):
        """
        Account-aware smart sell limiting: Each account sells only what it needs for its buys.
        Prevents idle cash accumulation within accounts.
        """
        from collections import defaultdict
        
        # Group transactions by account
        account_txns = defaultdict(lambda: {'sells': [], 'buys': []})
        
        for txn in transactions:
            if txn.action == 'SELL':
                account_txns[txn.account_id]['sells'].append(txn)
            else:
                account_txns[txn.account_id]['buys'].append(txn)
        
        # Process each account
        modified_txns = []
        total_original_sells = 0
        total_reduced_sells = 0
        
        for account_id, txns in account_txns.items():
            sells = txns['sells']
            buys = txns['buys']
            
            # Calculate account's cash flow needs
            total_sell_amount = sum(self._convert_to_base(s.amount, s.currency, user.base_currency, exchange_rates) for s in sells)
            total_buy_amount = sum(self._convert_to_base(b.amount, b.currency, user.base_currency, exchange_rates) for b in buys)
            account_cash = original_cash.get(account_id, 0)
            
            total_original_sells += total_sell_amount
            
            # Account needs: buys - existing cash
            cash_needed = max(0, total_buy_amount - account_cash)
            
            if total_sell_amount > 0 and cash_needed < total_sell_amount:
                # Reduce sells to match cash needed (keep small buffer)
                reduction_ratio = min(1.0, (cash_needed * 1.02) / total_sell_amount)  # 2% buffer
                
                for sell in sells:
                    sell.quantity = int(sell.quantity * reduction_ratio)
                    sell.amount *= reduction_ratio
                
                reduced_amount = total_sell_amount * reduction_ratio
                total_reduced_sells += reduced_amount
            else:
                # Keep all sells as-is
                total_reduced_sells += total_sell_amount
            
            # Add all transactions for this account
            modified_txns.extend(sells)
            modified_txns.extend(buys)
        
        if total_original_sells > 0 and total_reduced_sells < total_original_sells:
            reduction_pct = (total_reduced_sells / total_original_sells) * 100
            print(f"\n[Account-Aware Sell Limiting] Reduced sells from ${total_original_sells:,.0f} to ${total_reduced_sells:,.0f} ({reduction_pct:.1f}%)")
        
        return modified_txns

    def _precision_tune(self, user, deltas, account_cash, transactions, execution_order, exchange_rates):
        """ 
        Phase 4: Deploy remaining idle cash to reduce deviations.
        Works with any cash left after Smart Sell Limiting.
        """
        if not user.precision_rebalancing:
            return transactions, execution_order
        
        print(f"\n[{self.name}] Phase 4: Precision tuning with remaining cash")
        print(f"  DEBUG: Deltas passed to Phase 4:")
        for d in deltas:
            print(f"    {d['asset_class_name']}: ${d['dollar_diff']:,.0f} CAD ({d['percentage_diff']:+.2f}%)")

        
        # Find all underweight positions (any size)
        underweight_positions = [
            (d['asset_class_id'], d['asset_class_name'], d['dollar_diff'], abs(d['percentage_diff']))
            for d in deltas
            if d['dollar_diff'] > 0
        ]
        
        if not underweight_positions:
            print("  No underweight positions to tune")
            return transactions, execution_order
        
        # Sort by percentage deviation (worst first)
        underweight_positions.sort(key=lambda x: -x[3])
        
        remaining_to_buy = {ac_id: amount for ac_id, _, amount, _ in underweight_positions}

        # Deploy idle cash to reduce deviations
        total_deployed = 0
        for account in user.accounts:
            # cash_avail is in account currency (CAD)
            cash_avail = account_cash.get(account.id, 0)
            if cash_avail < 500:
                continue
         
            for ac_id, ac_name, _, pct_diff in underweight_positions:
                if remaining_to_buy.get(ac_id, 0) < 1 or cash_avail < 100:
                    continue
                
                # Check if account has existing holdings
                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id
                    for h in account.holdings
                )

                # Respect strategy rules
                if self.name in ["Minimize-Positions", "Cash-Efficient"] and not has_existing:
                    continue
                
                # amount_to_buy is in base currency (CAD)
                amount_to_buy = min(cash_avail, remaining_to_buy[ac_id])

                txn = self._create_buy_transaction(
                    user, account, ac_id, amount_to_buy, execution_order,
                    prefer_existing=True, exchange_rates=exchange_rates
                )

                if txn:
                    # Transaction amount is in SECURITY currency (might be USD)
                    # Convert to account currency (CAD) to get actual cost
                    actual_cost_in_cad = self._convert_to_base(
                        txn.amount,  # In security currency
                        txn.currency,  # Security currency (USD/CAD)
                        account.currency,  # Account currency (CAD)
                        exchange_rates
                    )
                    
                    print(f"  Deploying idle cash: Buy {ac_name} ${actual_cost_in_cad:,.0f} CAD in {account.name}")
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id] -= actual_cost_in_cad
                    cash_avail -= actual_cost_in_cad
                    account_cash[account.id] = cash_avail
                    total_deployed += actual_cost_in_cad

        if total_deployed > 0:
            print(f"  Total deployed: ${total_deployed:,.0f} CAD")

        return transactions, execution_order

    def _consolidate_transactions(self, transactions):
        """
        Merge duplicate BUY transactions for the same security in the same account,
        preserving the original execution order.
        """
        from collections import defaultdict

        print(f"\n[CONSOLIDATE] Before: {len(transactions)} transactions")
        for txn in transactions:
            if txn.security_id:
                sec = Security.query.get(txn.security_id)
                acc = Account.query.get(txn.account_id)
                print(f"  {txn.action} {sec.ticker if sec else '?'} qty={txn.quantity} in {acc.name if acc else '?'}")
        
        # Track which BUYs we've seen
        buy_map = {}  # (account_id, security_id) -> transaction
        result = []
        
        for txn in transactions:
            if txn.action == 'SELL':
                # Keep all SELLs as-is
                result.append(txn)
            else:
                # BUY: check if we've seen this combo before
                key = (txn.account_id, txn.security_id)
                
                if key in buy_map:
                    # Merge into existing BUY
                    existing = buy_map[key]
                    existing.quantity += txn.quantity
                    existing.amount += txn.amount
                else:
                    # First time seeing this BUY
                    buy_map[key] = txn
                    result.append(txn)
        
        # Renumber execution order
        for i, txn in enumerate(result, 1):
            txn.execution_order = i
        
        return result

    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        """Generate transactions using this strategy
        
        Args:
            user: User object
            deltas: List of all asset class deltas
            overweight: List of (ac_id, ac_name, amount, pct_diff) for overweight classes
            underweight: List of (ac_id, ac_name, amount, pct_diff) for underweight classes
            account_cash: Dict of {account_id: cash_available}
            exchange_rates: Dict of exchange rates
            
        Returns:
            TransactionPlan object
        """
        raise NotImplementedError
    
    def _get_eligible_securities_for_account(self, asset_class_id, account, user):
        """Helper: Get securities that can be held in account"""
        securities = Security.query.filter_by(asset_class_id=asset_class_id).all()
        eligible = []
        
        for security in securities:
            preference = SecurityPreference.query.filter_by(
                user_id=user.id,
                security_id=security.id
            ).first()
            
            can_use = True
            if preference:
                if preference.restriction_type == 'restricted_to_accounts':
                    if preference.account_config and preference.account_config.get('allowed'):
                        can_use = account.id in preference.account_config['allowed']
            
            if can_use:
                existing = any(h.security_id == security.id for h in account.holdings)
                eligible.append({'security': security, 'existing': existing})
        
        return eligible
    
    def _create_buy_transaction(self, user, account, asset_class_id, amount, 
                                execution_order, prefer_existing=True, exchange_rates=None):
        """Helper: Create a buy transaction"""
        eligible = self._get_eligible_securities_for_account(asset_class_id, account, user)
        
        if not eligible:
            return None
        
        if prefer_existing:
            eligible.sort(key=lambda x: (not x['existing'], x['security'].id))
        
        # Multiple options - user selection required
        if len(eligible) > 1:
            return RebalanceTransaction(
                user_id=user.id,
                account_id=account.id,
                action='BUY',
                quantity=0,
                price=0,
                amount=amount,
                currency=account.currency,
                execution_order=execution_order,
                requires_user_selection=True,
                available_securities=[s['security'].id for s in eligible],
                is_final_trade=False
            )
        
        # Single option - specific transaction
        security = eligible[0]['security']
        
        # Get price
        existing_holding = next((h for h in account.holdings if h.security_id == security.id), None)
        if existing_holding:
            price = existing_holding.price
        else:
            any_holding = Holding.query.filter_by(security_id=security.id).first()
            price = any_holding.price if any_holding else None
        
        if not price or price <= 0:
            return None

        # Convert amount from base currency to security currency
        amount_in_security_currency = amount
        if exchange_rates and security.currency != user.base_currency:
            rate_key = f"{user.base_currency}_TO_{security.currency}"
            rate = exchange_rates.get(rate_key, 1.0)
            amount_in_security_currency = amount * rate
        
        quantity = int(amount_in_security_currency / price)
        actual_buy = quantity * price
        
        if quantity < 1:
            return None
        
        return RebalanceTransaction(
            user_id=user.id,
            account_id=account.id,
            security_id=security.id,
            action='BUY',
            quantity=quantity,
            price=price,
            amount=actual_buy,
            currency=security.currency,
            execution_order=execution_order,
            is_final_trade=False
        )


class CashFirstStrategy(RebalancingStrategy):
    """Deploy all idle cash first, then sell overweight positions"""
    
    def __init__(self):
        super().__init__("Cash-First")
    
    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        transactions = []
        execution_order = 1
        
        # Track remaining
        remaining_to_buy = {ac_id: amount for ac_id, _, amount, _ in underweight}
        remaining_to_sell = {ac_id: amount for ac_id, _, amount, _ in overweight}
        account_cash = copy.deepcopy(account_cash)
        original_cash = copy.deepcopy(account_cash)

        
        print(f"\n[{self.name}] Phase 1: Deploy idle cash")
        
        # Phase 1: Deploy cash
        accounts_with_cash = [
            (account, account_cash[account.id]) 
            for account in user.accounts 
            if account_cash[account.id] > 100
        ]
        accounts_with_cash.sort(key=lambda x: -x[1])
        
        for account, cash_amount in accounts_with_cash:
            # Find best underweight option for this account
            best_option = None
            best_score = -1
            
            for ac_id, ac_name, amount_needed, pct_diff in underweight:
                if remaining_to_buy.get(ac_id, 0) < 1:
                    continue
                
                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id 
                    for h in account.holdings
                )
                
                eligible = self._get_eligible_securities_for_account(ac_id, account, user)
                if not eligible:
                    continue
                
                score = abs(pct_diff) * 10
                if has_existing:
                    score += 50
                
                if score > best_score:
                    best_score = score
                    best_option = (ac_id, ac_name, has_existing)
            
            if not best_option:
                continue
            
            ac_id, ac_name, has_existing = best_option
            amount_to_buy = min(cash_amount, remaining_to_buy[ac_id])
            
            txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)
            if txn:
                transactions.append(txn)
                execution_order += 1
                remaining_to_buy[ac_id] -= amount_to_buy
                cash_avail -= amount_to_buy
                account_cash[account.id] = cash_avail

        print(f"[{self.name}] Phase 2-3: Sell overweight and buy remaining")
        
        # Phase 2-3: Sell and buy grouped by account
        accounts_sorted = sorted(user.accounts, key=lambda x: (not x.is_registered, x.name))
        
        for account in accounts_sorted:
            account_sells = []
            
            # Generate sells
            for ac_id, ac_name, amount, pct_diff in overweight:
                if remaining_to_sell.get(ac_id, 0) < 1:
                    continue
                
                for holding in account.holdings:
                    if remaining_to_sell[ac_id] < 1:
                        break
                    
                    if not holding.security or holding.security.asset_class_id != ac_id:
                        continue
                    
                    sell_value = min(remaining_to_sell[ac_id], holding.market_value)
                    quantity = int(sell_value / holding.price)
                    
                    if quantity < 1:  # ADD THIS CHECK
                        continue
                    
                    actual_sell = quantity * holding.price
                    
                    if quantity < 1:
                        continue
                    
                    txn = RebalanceTransaction(
                        user_id=user.id,
                        account_id=account.id,
                        security_id=holding.security_id,
                        action='SELL',
                        quantity=quantity,
                        price=holding.price,
                        amount=actual_sell,
                        currency=holding.security.currency,
                        execution_order=execution_order,
                        is_final_trade=False
                    )
                    transactions.append(txn)
                    execution_order += 1
                    
                    account_cash[account.id] += actual_sell
                    remaining_to_sell[ac_id] -= actual_sell
            
            # Generate buys with available cash
            for ac_id, ac_name, amount, pct_diff in underweight:
                if remaining_to_buy.get(ac_id, 0) < 1:
                    continue
                
                cash_avail = account_cash.get(account.id, 0)
                if cash_avail < 100:
                    continue
                
                eligible = self._get_eligible_securities_for_account(ac_id, account, user)
                if not eligible:
                    continue
                
                amount_to_buy = min(remaining_to_buy[ac_id], cash_avail)
                
                txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)

                # DEBUG: Show buy details
                if txn and txn.security_id:
                    from models import Security
                    sec = Security.query.get(txn.security_id)
                    if sec and sec.asset_class.name == 'US Equity':
                        print(f"    DEBUG BUY: {sec.ticker} in {account.name} - amount_to_buy=${amount_to_buy:,.0f}, txn.amount=${txn.amount:,.2f} {txn.currency}, qty={txn.quantity}")

                if txn:
                    # Convert actual transaction amount to CAD for tracking
                    actual_cost_cad = self._convert_to_base(
                        txn.amount,
                        txn.currency,
                        user.base_currency,
                        exchange_rates
                    )
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id] -= actual_cost_cad
                    account_cash[account.id] -= actual_cost_cad

        # Recalculate deltas based on transactions so far
        updated_deltas = self._recalculate_deltas(deltas, transactions, user, exchange_rates)

        # DEBUG: Show what we bought in Phase 2-3
        from collections import defaultdict
        buys_by_ac = defaultdict(float)
        for txn in transactions:
            if txn.action == 'BUY' and txn.security_id:
                from models import Security
                sec = Security.query.get(txn.security_id)
                if sec:
                    amount_cad = self._convert_to_base(txn.amount, txn.currency, user.base_currency, exchange_rates)
                    buys_by_ac[sec.asset_class.name] += amount_cad

        print(f"\n[{self.name}] DEBUG: Phase 2-3 purchases:")
        for ac_name, total in buys_by_ac.items():
            print(f"  {ac_name}: ${total:,.0f} CAD")

        transactions, execution_order = self._precision_tune(
            user, updated_deltas, account_cash, transactions, execution_order, exchange_rates
        )

        # Apply smart sell limiting to avoid excess idle cash
        transactions = self._apply_smart_sell_limiting(transactions, original_cash, user, exchange_rates)

        transactions = self._consolidate_transactions(transactions)

        return TransactionPlan(transactions, {'strategy': self.name})


class MinimizePositionsStrategy(RebalancingStrategy):
    """Never create new positions - only add to existing holdings"""
    
    def __init__(self):
        super().__init__("Minimize-Positions")
    
    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        transactions = []
        execution_order = 1
        
        remaining_to_buy = {ac_id: amount for ac_id, _, amount, _ in underweight}
        remaining_to_sell = {ac_id: amount for ac_id, _, amount, _ in overweight}
        account_cash = copy.deepcopy(account_cash)
        original_cash = copy.deepcopy(account_cash)
        
        print(f"\n[{self.name}] Strict rule: Only buy in accounts with existing holdings")
        
        accounts_sorted = sorted(user.accounts, key=lambda x: (not x.is_registered, x.name))
        
        # Phase 1: Deploy cash only to existing holdings
        for account in user.accounts:
            cash_avail = account_cash.get(account.id, 0)
            if cash_avail < 100:
                continue
            
            for ac_id, ac_name, amount, pct_diff in sorted(underweight, key=lambda x: -abs(x[3])):
                if remaining_to_buy.get(ac_id, 0) < 1 or cash_avail < 100:
                    continue
                
                # STRICT: Only if account already holds this asset class
                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id 
                    for h in account.holdings
                )
                
                if not has_existing:
                    continue
                
                amount_to_buy = min(cash_avail, remaining_to_buy[ac_id])
                
                txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)

                if txn:
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id] -= amount_to_buy
                    cash_avail -= amount_to_buy
                    account_cash[account.id] = cash_avail
        
        # Phase 2-3: Sell and buy (still only existing)
        for account in accounts_sorted:
            # Check if this account can buy ANY underweight asset class
            can_buy_something = False
            for ac_id, ac_name, amount, pct_diff in underweight:
                if remaining_to_buy.get(ac_id, 0) < 1:
                    continue
                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id 
                    for h in account.holdings
                )
                if has_existing:
                    can_buy_something = True
                    break
            
            # Skip selling from accounts that can't buy anything (would create orphaned cash)
            if not can_buy_something:
                continue
            
            # Sells
            for ac_id, ac_name, amount, pct_diff in overweight:
                if remaining_to_sell.get(ac_id, 0) < 1:
                    continue
                
                for holding in account.holdings:
                    if remaining_to_sell[ac_id] < 1:
                        break
                    
                    if not holding.security or holding.security.asset_class_id != ac_id:
                        continue
                    
                    sell_value = min(remaining_to_sell[ac_id], holding.market_value)
                    quantity = int(sell_value / holding.price)
                    
                    if quantity < 1:  # ADD THIS CHECK
                        continue
                    
                    actual_sell = quantity * holding.price
                    
                    if quantity < 1:
                        continue
                    
                    txn = RebalanceTransaction(
                        user_id=user.id,
                        account_id=account.id,
                        security_id=holding.security_id,
                        action='SELL',
                        quantity=quantity,
                        price=holding.price,
                        amount=actual_sell,
                        currency=holding.security.currency,
                        execution_order=execution_order,
                        is_final_trade=False
                    )
                    transactions.append(txn)
                    execution_order += 1
                    
                    account_cash[account.id] += actual_sell
                    remaining_to_sell[ac_id] -= actual_sell
            
            # Buys (only existing holdings)
            for ac_id, ac_name, amount, pct_diff in underweight:
                if remaining_to_buy.get(ac_id, 0) < 1:
                    continue
                
                cash_avail = account_cash.get(account.id, 0)
                if cash_avail < 100:
                    continue
                
                # STRICT: Only if existing
                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id 
                    for h in account.holdings
                )
                
                if not has_existing:
                    continue
                
                amount_to_buy = min(remaining_to_buy[ac_id], cash_avail)
                
                txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)

                # DEBUG: Show buy details
                if txn and txn.security_id:
                    from models import Security
                    sec = Security.query.get(txn.security_id)
                    if sec and sec.asset_class.name == 'US Equity':
                        print(f"    DEBUG BUY: {sec.ticker} in {account.name} - amount_to_buy=${amount_to_buy:,.0f}, txn.amount=${txn.amount:,.2f} {txn.currency}, qty={txn.quantity}")

                if txn:
                    # Convert actual transaction amount to CAD for tracking
                    actual_cost_cad = self._convert_to_base(
                        txn.amount,
                        txn.currency,
                        user.base_currency,
                        exchange_rates
                    )
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id] -= actual_cost_cad
                    account_cash[account.id] -= actual_cost_cad
        
        # Recalculate deltas based on transactions so far
        updated_deltas = self._recalculate_deltas(deltas, transactions, user, exchange_rates)

        # DEBUG: Show what we bought in Phase 2-3
        from collections import defaultdict
        buys_by_ac = defaultdict(float)
        for txn in transactions:
            if txn.action == 'BUY' and txn.security_id:
                from models import Security
                sec = Security.query.get(txn.security_id)
                if sec:
                    amount_cad = self._convert_to_base(txn.amount, txn.currency, user.base_currency, exchange_rates)
                    buys_by_ac[sec.asset_class.name] += amount_cad

        print(f"\n[{self.name}] DEBUG: Phase 2-3 purchases:")
        for ac_name, total in buys_by_ac.items():
            print(f"  {ac_name}: ${total:,.0f} CAD")

        transactions, execution_order = self._precision_tune(
            user, updated_deltas, account_cash, transactions, execution_order, exchange_rates
        )

        # Apply smart sell limiting to avoid excess idle cash
        transactions = self._apply_smart_sell_limiting(transactions, original_cash, user, exchange_rates)

        transactions = self._consolidate_transactions(transactions)

        return TransactionPlan(transactions, {'strategy': self.name})


class CashEfficientStrategy(RebalancingStrategy):
    """Never create new positions - only add to existing holdings"""
    
    def __init__(self):
        super().__init__("Cash-Efficient")
    
    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        transactions = []
        execution_order = 1
        
        remaining_to_buy = {ac_id: amount for ac_id, _, amount, _ in underweight}
        remaining_to_sell = {ac_id: amount for ac_id, _, amount, _ in overweight}
        account_cash = copy.deepcopy(account_cash)
        original_cash = copy.deepcopy(account_cash)
        
        print(f"\n[{self.name}] Strict rule: Only buy in accounts with existing holdings")
        
        accounts_sorted = sorted(user.accounts, key=lambda x: (not x.is_registered, x.name))
        
        # Phase 1: Deploy cash only to existing holdings
        for account in user.accounts:
            cash_avail = account_cash.get(account.id, 0)
            if cash_avail < 100:
                continue
            
            for ac_id, ac_name, amount, pct_diff in sorted(underweight, key=lambda x: -abs(x[3])):
                if remaining_to_buy.get(ac_id, 0) < 1 or cash_avail < 100:
                    continue
                
                # STRICT: Only if account already holds this asset class
                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id 
                    for h in account.holdings
                )
                
                if not has_existing:
                    continue
                
                amount_to_buy = min(cash_avail, remaining_to_buy[ac_id])
                
                txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)

                if txn:
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id] -= amount_to_buy
                    cash_avail -= amount_to_buy
                    account_cash[account.id] = cash_avail
        
        # Phase 2-3: Sell and buy (still only existing)
        for account in accounts_sorted:
            # Sells
            for ac_id, ac_name, amount, pct_diff in overweight:
                if remaining_to_sell.get(ac_id, 0) < 1:
                    continue
                
                for holding in account.holdings:
                    if remaining_to_sell[ac_id] < 1:
                        break
                    
                    if not holding.security or holding.security.asset_class_id != ac_id:
                        continue
                    
                    sell_value = min(remaining_to_sell[ac_id], holding.market_value)
                    quantity = int(sell_value / holding.price)
                    
                    if quantity < 1:  # ADD THIS CHECK
                        continue
                    
                    actual_sell = quantity * holding.price
                    
                    if quantity < 1:
                        continue
                    
                    txn = RebalanceTransaction(
                        user_id=user.id,
                        account_id=account.id,
                        security_id=holding.security_id,
                        action='SELL',
                        quantity=quantity,
                        price=holding.price,
                        amount=actual_sell,
                        currency=holding.security.currency,
                        execution_order=execution_order,
                        is_final_trade=False
                    )
                    transactions.append(txn)
                    execution_order += 1
                    
                    account_cash[account.id] += actual_sell
                    remaining_to_sell[ac_id] -= actual_sell
            
            # Buys (only existing holdings)
            for ac_id, ac_name, amount, pct_diff in underweight:
                if remaining_to_buy.get(ac_id, 0) < 1:
                    continue
                
                cash_avail = account_cash.get(account.id, 0)
                if cash_avail < 100:
                    continue
                
                # STRICT: Only if existing
                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id 
                    for h in account.holdings
                )
                
                if not has_existing:
                    continue
                
                amount_to_buy = min(remaining_to_buy[ac_id], cash_avail)
                
                txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)

                # DEBUG: Show buy details
                if txn and txn.security_id:
                    from models import Security
                    sec = Security.query.get(txn.security_id)
                    if sec and sec.asset_class.name == 'US Equity':
                        print(f"    DEBUG BUY: {sec.ticker} in {account.name} - amount_to_buy=${amount_to_buy:,.0f}, txn.amount=${txn.amount:,.2f} {txn.currency}, qty={txn.quantity}")

                if txn:
                    # Convert actual transaction amount to CAD for tracking
                    actual_cost_cad = self._convert_to_base(
                        txn.amount,
                        txn.currency,
                        user.base_currency,
                        exchange_rates
                    )
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id] -= actual_cost_cad
                    account_cash[account.id] -= actual_cost_cad
        
        # Phase 4: Cross-account cash optimization
        # Use idle cash for OTHER underweight positions the account holds
        for account in accounts_sorted:
            idle_cash = account_cash.get(account.id, 0)
            if idle_cash < 100:
                continue
            
            # Find other underweight asset classes this account holds
            for ac_id, ac_name, amount, pct_diff in sorted(underweight, key=lambda x: -abs(x[3])):
                if remaining_to_buy.get(ac_id, 0) < 100 or idle_cash < 100:
                    continue
                
                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id 
                    for h in account.holdings
                )
                
                if has_existing:
                    amount_to_buy = min(idle_cash, remaining_to_buy[ac_id])
                    
                    txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)
                    if txn:
                        transactions.append(txn)
                        execution_order += 1
                        remaining_to_buy[ac_id] -= amount_to_buy
                        idle_cash -= amount_to_buy
                        account_cash[account.id] = idle_cash
        
        # Recalculate deltas based on transactions so far
        updated_deltas = self._recalculate_deltas(deltas, transactions, user, exchange_rates)

        # DEBUG: Show what we bought in Phase 2-3
        from collections import defaultdict
        buys_by_ac = defaultdict(float)
        for txn in transactions:
            if txn.action == 'BUY' and txn.security_id:
                from models import Security
                sec = Security.query.get(txn.security_id)
                if sec:
                    amount_cad = self._convert_to_base(txn.amount, txn.currency, user.base_currency, exchange_rates)
                    buys_by_ac[sec.asset_class.name] += amount_cad

        print(f"\n[{self.name}] DEBUG: Phase 2-3 purchases:")
        for ac_name, total in buys_by_ac.items():
            print(f"  {ac_name}: ${total:,.0f} CAD")

        transactions, execution_order = self._precision_tune(
            user, updated_deltas, account_cash, transactions, execution_order, exchange_rates
        )

        # Apply smart sell limiting to avoid excess idle cash
        transactions = self._apply_smart_sell_limiting(transactions, original_cash, user, exchange_rates)

        transactions = self._consolidate_transactions(transactions)

        return TransactionPlan(transactions, {'strategy': self.name})


class TaxOptimizedStrategy(RebalancingStrategy):
    """Maximize registered account usage for tax-efficient rebalancing"""
    
    def __init__(self):
        super().__init__("Tax-Optimized")
    
    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        # DEBUG: Show exchange rates
        print(f"\n[{self.name}] Exchange rates available: {list(exchange_rates.keys())}")
        # DEBUG: Show initial deltas
        for d in deltas:
            if d['asset_class_name'] == 'US Equity':
                print(f"  [{self.name}] Initial US Equity delta: dollar_diff=${d['dollar_diff']:,.0f} CAD, current=${d['current_value']:,.0f}, target=${d['target_value']:,.0f}")
    
        transactions = []
        execution_order = 1
        
        remaining_to_buy = {ac_id: amount for ac_id, _, amount, _ in underweight}
        remaining_to_sell = {ac_id: amount for ac_id, _, amount, _ in overweight}
        account_cash = copy.deepcopy(account_cash)
        original_cash = copy.deepcopy(account_cash)
        
        print(f"\n[{self.name}] Prioritize registered accounts for all trading")
        
        # Sort: registered accounts first
        accounts_sorted = sorted(user.accounts, key=lambda x: (not x.is_registered, -account_cash.get(x.id, 0)))
        
        # Phase 1: Deploy cash (registered first)
        for account in accounts_sorted:
            cash_avail = account_cash.get(account.id, 0)
            if cash_avail < 100:
                continue
            
            # Prefer registered accounts
            tax_bonus = 50 if account.is_registered else 0
            
            for ac_id, ac_name, amount, pct_diff in sorted(underweight, key=lambda x: -abs(x[3])):
                if remaining_to_buy.get(ac_id, 0) < 1 or cash_avail < 100:
                    continue
                
                has_existing = any(
                    h.security and h.security.asset_class_id == ac_id 
                    for h in account.holdings
                )
                
                eligible = self._get_eligible_securities_for_account(ac_id, account, user)
                if not eligible:
                    continue
                
                amount_to_buy = min(cash_avail, remaining_to_buy[ac_id])
                
                txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)
                if txn:
                    # Convert actual transaction amount to CAD
                    actual_cost_cad = self._convert_to_base(
                        txn.amount,
                        txn.currency,
                        user.base_currency,
                        exchange_rates
                    )

                    rate_key = f"{txn.currency}_TO_{user.base_currency}"
                    rate = exchange_rates.get(rate_key, "NOT_FOUND")
                    print(f"  [Phase 1] DEBUG: txn.amount=${txn.amount:,.2f} {txn.currency}, converted to ${actual_cost_cad:,.2f} CAD using rate key {rate_key}, rate value={rate}")
                    print(f"  [Phase 1] Bought {ac_name} ${actual_cost_cad:,.0f} CAD in {account.name}, reducing remaining_to_buy from ${remaining_to_buy[ac_id]:,.0f}")

                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id] -= actual_cost_cad  # Use actual, not requested!
                    cash_avail -= actual_cost_cad
                    account_cash[account.id] = cash_avail

        # Phase 2-3: Sell and buy (registered first)
        for account in accounts_sorted:
            # Sells
            for ac_id, ac_name, amount, pct_diff in overweight:
                if remaining_to_sell.get(ac_id, 0) < 1:
                    continue
                
                for holding in account.holdings:
                    if remaining_to_sell[ac_id] < 1:
                        break
                    
                    if not holding.security or holding.security.asset_class_id != ac_id:
                        continue
                    
                    sell_value = min(remaining_to_sell[ac_id], holding.market_value)
                    quantity = int(sell_value / holding.price)
                    
                    if quantity < 1:  # ADD THIS CHECK
                        continue

                    actual_sell = quantity * holding.price
                    
                    if quantity < 1:
                        continue
                    
                    txn = RebalanceTransaction(
                        user_id=user.id,
                        account_id=account.id,
                        security_id=holding.security_id,
                        action='SELL',
                        quantity=quantity,
                        price=holding.price,
                        amount=actual_sell,
                        currency=holding.security.currency,
                        execution_order=execution_order,
                        is_final_trade=False
                    )
                    transactions.append(txn)
                    execution_order += 1
                    
                    account_cash[account.id] += actual_sell
                    remaining_to_sell[ac_id] -= actual_sell
            
            # Buys
            for ac_id, ac_name, amount, pct_diff in underweight:
                if remaining_to_buy.get(ac_id, 0) < 1:
                    continue
                
                cash_avail = account_cash.get(account.id, 0)
                if cash_avail < 100:
                    continue
                
                eligible = self._get_eligible_securities_for_account(ac_id, account, user)
                if not eligible:
                    continue
                
                amount_to_buy = min(remaining_to_buy[ac_id], cash_avail)
                
                txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)

                # DEBUG: Show buy details
                if txn and txn.security_id:
                    from models import Security
                    sec = Security.query.get(txn.security_id)
                    if sec and sec.asset_class.name == 'US Equity':
                        print(f"    DEBUG BUY: {sec.ticker} in {account.name} - amount_to_buy=${amount_to_buy:,.0f}, txn.amount=${txn.amount:,.2f} {txn.currency}, qty={txn.quantity}")

                if txn:
                    # Convert actual transaction amount to CAD for tracking
                    actual_cost_cad = self._convert_to_base(
                        txn.amount,
                        txn.currency,
                        user.base_currency,
                        exchange_rates
                    )
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id] -= actual_cost_cad
                    account_cash[account.id] -= actual_cost_cad
        
        # DEBUG: Show remaining_to_buy after Phase 2-3
        print(f"\n[{self.name}] Remaining to buy after Phase 2-3:")
        for ac_id, remaining in remaining_to_buy.items():
            ac = next((d for d in deltas if d['asset_class_id'] == ac_id), None)
            if ac:
                print(f"  {ac['asset_class_name']}: ${remaining:,.0f} CAD")

        # Recalculate deltas based on transactions so far
        updated_deltas = self._recalculate_deltas(deltas, transactions, user, exchange_rates)

        # DEBUG: Show what we bought in Phase 2-3
        from collections import defaultdict
        buys_by_ac = defaultdict(float)
        for txn in transactions:
            if txn.action == 'BUY' and txn.security_id:
                from models import Security
                sec = Security.query.get(txn.security_id)
                if sec:
                    amount_cad = self._convert_to_base(txn.amount, txn.currency, user.base_currency, exchange_rates)
                    buys_by_ac[sec.asset_class.name] += amount_cad

        print(f"\n[{self.name}] DEBUG: Phase 2-3 purchases:")
        for ac_name, total in buys_by_ac.items():
            print(f"  {ac_name}: ${total:,.0f} CAD")

        transactions, execution_order = self._precision_tune(
            user, updated_deltas, account_cash, transactions, execution_order, exchange_rates
        )

        # Apply smart sell limiting to avoid excess idle cash
        transactions = self._apply_smart_sell_limiting(transactions, original_cash, user, exchange_rates)

        transactions = self._consolidate_transactions(transactions)

        return TransactionPlan(transactions, {'strategy': self.name})


class HeuristicStrategy(RebalancingStrategy):
    """Score-based optimization balancing all factors"""
    
    def __init__(self):
        super().__init__("Heuristic")
    
    def generate(self, user, deltas, overweight, underweight, account_cash, exchange_rates):
        transactions = []
        execution_order = 1
        
        remaining_to_buy = {ac_id: amount for ac_id, _, amount, _ in underweight}
        remaining_to_sell = {ac_id: amount for ac_id, _, amount, _ in overweight}
        account_cash = copy.deepcopy(account_cash)
        original_cash = copy.deepcopy(account_cash)
        
        print(f"\n[{self.name}] Using score-based optimization")
        
        # Phase 1: Deploy cash using scoring
        for iteration in range(len(underweight)):
            best_score = -999
            best_choice = None
            
            # Score every possible (account, asset_class) combination
            for account in user.accounts:
                cash_avail = account_cash.get(account.id, 0)
                if cash_avail < 100:
                    continue
                
                for ac_id, ac_name, amount, pct_diff in underweight:
                    if remaining_to_buy.get(ac_id, 0) < 1:
                        continue
                    
                    has_existing = any(
                        h.security and h.security.asset_class_id == ac_id 
                        for h in account.holdings
                    )
                    
                    eligible = self._get_eligible_securities_for_account(ac_id, account, user)
                    if not eligible:
                        continue
                    
                    # Calculate score
                    score = 0
                    score += abs(pct_diff) * 10  # Larger imbalance = higher priority
                    score += 100 if has_existing else 0  # Strongly prefer existing
                    score += 50 if account.is_registered else 0  # Tax efficiency
                    score += min(cash_avail, 10000) / 100  # More cash = better
                    score -= 50 if not has_existing else 0  # Penalty for new position
                    
                    if score > best_score:
                        best_score = score
                        amount_to_buy = min(cash_avail, remaining_to_buy[ac_id])
                        best_choice = (account, ac_id, ac_name, amount_to_buy, has_existing)
            
            if not best_choice:
                break
            
            account, ac_id, ac_name, amount_to_buy, has_existing = best_choice
            
            txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)
            if txn:
                transactions.append(txn)
                execution_order += 1
                remaining_to_buy[ac_id] -= amount_to_buy
                account_cash[account.id] -= amount_to_buy
        
        # Phase 2-3: Sell and buy using scoring
        accounts_sorted = sorted(user.accounts, key=lambda x: (not x.is_registered, x.name))
        
        for account in accounts_sorted:
            # Sells
            for ac_id, ac_name, amount, pct_diff in overweight:
                if remaining_to_sell.get(ac_id, 0) < 1:
                    continue
                
                for holding in account.holdings:
                    if remaining_to_sell[ac_id] < 1:
                        break
                    
                    if not holding.security or holding.security.asset_class_id != ac_id:
                        continue
                    
                    sell_value = min(remaining_to_sell[ac_id], holding.market_value)
                    quantity = int(sell_value / holding.price)
                    
                    if quantity < 1:  # ADD THIS CHECK
                        continue
                    
                    actual_sell = quantity * holding.price
                    
                    if quantity < 1:
                        continue
                    
                    txn = RebalanceTransaction(
                        user_id=user.id,
                        account_id=account.id,
                        security_id=holding.security_id,
                        action='SELL',
                        quantity=quantity,
                        price=holding.price,
                        amount=actual_sell,
                        currency=holding.security.currency,
                        execution_order=execution_order,
                        is_final_trade=False
                    )
                    transactions.append(txn)
                    execution_order += 1
                    
                    account_cash[account.id] += actual_sell
                    remaining_to_sell[ac_id] -= actual_sell
            
            # Buys with scoring
            for iteration in range(len(underweight)):
                best_score = -999
                best_ac = None
                
                cash_avail = account_cash.get(account.id, 0)
                if cash_avail < 100:
                    break
                
                for ac_id, ac_name, amount, pct_diff in underweight:
                    if remaining_to_buy.get(ac_id, 0) < 1:
                        continue
                    
                    has_existing = any(
                        h.security and h.security.asset_class_id == ac_id 
                        for h in account.holdings
                    )
                    
                    eligible = self._get_eligible_securities_for_account(ac_id, account, user)
                    if not eligible:
                        continue
                    
                    score = abs(pct_diff) * 10
                    score += 100 if has_existing else 0
                    score -= 50 if not has_existing else 0
                    
                    if score > best_score:
                        best_score = score
                        best_ac = (ac_id, ac_name)
                
                if not best_ac:
                    break
                
                ac_id, ac_name = best_ac
                amount_to_buy = min(remaining_to_buy[ac_id], cash_avail)
                
                txn = self._create_buy_transaction(user, account, ac_id, amount_to_buy, execution_order, exchange_rates=exchange_rates)

                # DEBUG: Show buy details
                if txn and txn.security_id:
                    from models import Security
                    sec = Security.query.get(txn.security_id)
                    if sec and sec.asset_class.name == 'US Equity':
                        print(f"    DEBUG BUY: {sec.ticker} in {account.name} - amount_to_buy=${amount_to_buy:,.0f}, txn.amount=${txn.amount:,.2f} {txn.currency}, qty={txn.quantity}")

                if txn:
                    # Convert actual transaction amount to CAD for tracking
                    actual_cost_cad = self._convert_to_base(
                        txn.amount,
                        txn.currency,
                        user.base_currency,
                        exchange_rates
                    )
                    transactions.append(txn)
                    execution_order += 1
                    remaining_to_buy[ac_id] -= actual_cost_cad
                    account_cash[account.id] -= actual_cost_cad
                    cash_avail = account_cash[account.id]
        
        # Recalculate deltas based on transactions so far
        updated_deltas = self._recalculate_deltas(deltas, transactions, user, exchange_rates)

        # DEBUG: Show what we bought in Phase 2-3
        from collections import defaultdict
        buys_by_ac = defaultdict(float)
        for txn in transactions:
            if txn.action == 'BUY' and txn.security_id:
                from models import Security
                sec = Security.query.get(txn.security_id)
                if sec:
                    amount_cad = self._convert_to_base(txn.amount, txn.currency, user.base_currency, exchange_rates)
                    buys_by_ac[sec.asset_class.name] += amount_cad

        print(f"\n[{self.name}] DEBUG: Phase 2-3 purchases:")
        for ac_name, total in buys_by_ac.items():
            print(f"  {ac_name}: ${total:,.0f} CAD")

        transactions, execution_order = self._precision_tune(
            user, updated_deltas, account_cash, transactions, execution_order, exchange_rates
        )


        # Apply smart sell limiting to avoid excess idle cash
        transactions = self._apply_smart_sell_limiting(transactions, original_cash, user, exchange_rates)

        transactions = self._consolidate_transactions(transactions)

        return TransactionPlan(transactions, {'strategy': self.name})


def score_transaction_plan(plan, user):
    """Score a transaction plan based on multiple criteria
    
    Higher score = better plan
    """
    stats = plan.get_stats()
    
    score = 100  # Start at 100
    
    # Fewer transactions is better (max penalty: -20)
    score -= min(stats['total_transactions'] * 1, 20)
    
    # New positions are bad (penalty: -15 per new position)
    score -= stats['new_positions'] * 15
    
    # Registered sells are good (bonus: +2 per registered sell)
    score += stats['registered_sells'] * 2
    
    # Fewer accounts touched is better
    score -= stats['accounts_touched'] * 2
    
    return max(score, 0)  # Don't go negative

def generate_rebalance_transactions(user):
    """
    Generate rebalancing transactions using per-security net deltas.

    Steps:
    - Refresh prices and FX
    - Compute asset-class targets (existing logic)
    - Allocate each asset class's target proportionally across its securities
    - For each security, compute a single net delta (target - current)
    - Allocate that net delta across accounts according to preferences and cash
    """
    from app import (
        fetch_prices_from_user_sheet,
        fetch_exchange_rate,
        get_exchange_rates,
        calculate_asset_class_deltas,
    )
    from collections import defaultdict

    # -------------------------
    # 1) Refresh prices and FX
    # -------------------------
    try:
        prices = fetch_prices_from_user_sheet(user)

        if prices:
            holdings = (
                Holding.query
                .join(Account)
                .join(Security)
                .filter(
                    Account.user_id == user.id,
                    Security.is_public.is_(True),
                    Security.auto_update_price.is_(True),
                )
                .all()
            )

            for holding in holdings:
                symbol = holding.security.ticker
                price = prices.get(symbol)
                if price is not None:
                    holding.price = float(price)
                    holding.updated_at = datetime.utcnow()

            db.session.commit()

        fetch_exchange_rate("USD", "CAD")
        fetch_exchange_rate("CAD", "USD")

    except Exception as e:
        print(f"Warning: Could not refresh prices: {e}")

    exchange_rates = get_exchange_rates(user)

    # Clear old unexecuted transactions
    RebalanceTransaction.query.filter_by(
        user_id=user.id, executed=False
    ).delete()

    # -------------------------------
    # 2) Asset-class deltas as before
    # -------------------------------
    deltas, total_portfolio = calculate_asset_class_deltas(user, exchange_rates)

    balanced_threshold = user.balanced_threshold or 0.5
    deltas = [d for d in deltas if abs(d["percentage_diff"]) > balanced_threshold]

    if not deltas:
        db.session.commit()
        return []

    if user.trading_costs_enabled:
        deltas = [d for d in deltas if abs(d["percentage_diff"]) >= 0.1]

    # Map asset_class_id -> target value (dollar)
    target_by_class = {d["asset_class_id"]: d["target_value"] for d in deltas}

    # -------------------------------------------------
    # 3) Aggregate current holdings by security & class
    # -------------------------------------------------
    security_totals = {}
    current_by_class = defaultdict(float)

    for account in user.accounts:
        for holding in account.holdings:
            if not holding.security or not holding.security.asset_class_id:
                continue

            sec = holding.security
            sec_id = sec.id
            cls_id = sec.asset_class_id
            value = holding.market_value_in_base_currency(exchange_rates)

            if sec_id not in security_totals:
                security_totals[sec_id] = {
                    "security": sec,
                    "asset_class_id": cls_id,
                    "total_value": 0.0,
                    "by_account": defaultdict(float),
                }

            security_totals[sec_id]["total_value"] += value
            security_totals[sec_id]["by_account"][account.id] += value
            current_by_class[cls_id] += value

    if not security_totals:
        db.session.commit()
        return []

    # ------------------------------------------------------------
    # 4) Compute per-security net deltas from asset-class targets
    # ------------------------------------------------------------
    desired_security_delta = {}

    for sec_id, info in security_totals.items():
        cls_id = info["asset_class_id"]
        current_total = info["total_value"]
        class_current = current_by_class[cls_id]

        # if this class is not in deltas, we skip (treated as "balanced enough")
        class_target = target_by_class.get(cls_id, class_current)

        if class_current <= 0:
            # no current value in this class - leave unchanged for now
            delta = 0.0
        else:
            # proportional allocation: keep intra-class proportions
            target_for_security = (current_total / class_current) * class_target
            delta = target_for_security - current_total

        desired_security_delta[sec_id] = delta

    # ---------------------------------------
    # 5) Turn per-security deltas into trades
    # ---------------------------------------
    transactions = []
    execution_order = 1
    tolerance_value = 1.0  # ignore tiny drifts (1 unit of base currency)

    # Helper: account order for sells (registered first, then by priority)
    def sell_account_order(accounts):
        return sorted(
            accounts,
            key=lambda a: (
                not a.is_registered,  # registered first (False < True)
                -a.priority,
            ),
        )

    # Helper: account order for buys (existing position, then registered, then size)
    def buy_account_order(sec_id, accounts):
        ordered = []
        for account in accounts:
            has_existing = any(
                h.security_id == sec_id for h in account.holdings
            )
            account_value = sum(h.market_value for h in account.holdings)
            ordered.append(
                {
                    "account": account,
                    "has_existing": has_existing,
                    "value": account_value,
                }
            )
        ordered.sort(
            key=lambda x: (
                not x["has_existing"],
                not x["account"].is_registered,
                -x["value"],
            )
        )
        return [x["account"] for x in ordered]

    # Main loop: one pass per security
    for sec_id, delta_value in desired_security_delta.items():
        if abs(delta_value) < tolerance_value:
            continue

        info = security_totals[sec_id]
        sec = info["security"]

        # get a usable price
        any_holding = (
            Holding.query.filter_by(security_id=sec_id)
            .order_by(Holding.updated_at.desc())
            .first()
        )
        price = any_holding.price if any_holding and any_holding.price > 0 else None
        if not price or price <= 0:
            continue

        delta_shares = delta_value / price

        # ---------------------------------
        # NET SELL (reduce this security)
        # ---------------------------------
        if delta_shares < 0:
            shares_to_sell = abs(delta_shares)
            for account in sell_account_order(user.accounts):
                if shares_to_sell <= 0:
                    break

                holding = next(
                    (h for h in account.holdings if h.security_id == sec_id),
                    None,
                )
                if not holding or holding.quantity <= 0:
                    continue

                # Max shares we can/should sell from this account
                sell_qty = min(holding.quantity, int(shares_to_sell))
                if sell_qty < 1:
                    continue

                amount = sell_qty * price
                txn = RebalanceTransaction(
                    user_id=user.id,
                    account_id=account.id,
                    security_id=sec_id,
                    action="SELL",
                    quantity=sell_qty,
                    price=price,
                    amount=amount,
                    currency=sec.currency,
                    execution_order=execution_order,
                    is_final_trade=False,
                )
                db.session.add(txn)
                transactions.append(txn)
                execution_order += 1

                shares_to_sell -= sell_qty

        # ---------------------------------
        # NET BUY (increase this security)
        # ---------------------------------
        else:
            shares_to_buy = delta_shares
            for account in buy_account_order(sec_id, user.accounts):
                if shares_to_buy <= 0:
                    break

                if account.cash_balance <= 0:
                    continue

                max_by_cash = account.cash_balance / price
                buy_qty = int(min(shares_to_buy, max_by_cash))
                if buy_qty < 1:
                    continue

                amount = buy_qty * price
                txn = RebalanceTransaction(
                    user_id=user.id,
                    account_id=account.id,
                    security_id=sec_id,
                    action="BUY",
                    quantity=buy_qty,
                    price=price,
                    amount=amount,
                    currency=sec.currency,
                    execution_order=execution_order,
                    is_final_trade=False,
                )
                db.session.add(txn)
                transactions.append(txn)
                execution_order += 1

                shares_to_buy -= buy_qty

    # Mark last BUY per account as final (fractional-friendly)
    account_last_buy = {}
    for txn in transactions:
        if txn.action == "BUY":
            account_last_buy[txn.account_id] = txn

    for _, last_txn in account_last_buy.items():
        last_txn.is_final_trade = True

    db.session.commit()
    return transactions
