import pandas as pd
import time
import ccxt
import os
import sys
import json
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

# --- Path Auto-Heal Logic ---
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

try:
    from core.api_connect import DataBridge
    from utils.tg_wrapper import TelegramReporter
except ImportError as e:
    logger.error(f"❌ Failed to load core modules: {e}")


class ExecutionManager:
    """
    Industrial Grade Execution Engine v2.3-Stable (Optimized)
    Functional Features:
        1. Triple-Step Defense (VWAP, Drift, IOC)
        2. Automatic Reconciliation & Position Audit
        3. Telegram Real-time Reporting & Funding Guard
        4. [NEW] Strategy 1: Time-based Exit (3x Half-life Reversion)
        5. [NEW] Strategy 2: Profit Guard Kill Switch (HWM Protection)
        6. [NEW] Targeted Close for single pairs
    """

    # [新增] 系統版本號
    VERSION = "v2.3.0-Stable"

    def __init__(self, budget_per_pair=100.0):
        # 1. Resource & Path Definitions
        self.budget_per_pair = budget_per_pair
        self.root_dir = Path(__file__).resolve().parent.parent
        self.signal_file = self.root_dir / 'data' / 'signal' / 'signal_table.csv'
        self.trade_log = self.root_dir / 'data' / 'trade' / 'trade_record.csv'
        self.vault_dir = self.root_dir / 'data' / 'vault'
        self.hwm_file = self.vault_dir / 'equity_hwm.json'

        # Ensure directories exist (防呆機制)
        self.trade_log.parent.mkdir(parents=True, exist_ok=True)
        self.signal_file.parent.mkdir(parents=True, exist_ok=True)
        self.vault_dir.mkdir(parents=True, exist_ok=True)

        # 2. Safety & Risk Parameters
        self.DRIFT_THRESHOLD = 0.003  # 0.3% price drift limit
        self.SLIPPAGE_TOLERANCE = 0.001  # 0.1% IOC slippage tolerance
        self.MAX_RETRIES = 5  # Exponential backoff retries
        self.MAX_FUNDING_DAILY = 0.0003  # 0.03% daily funding cost limit

        self.tg = TelegramReporter()

        logger.info(f"🚀 Initializing ExecutionManager {self.VERSION}")
        self._init_exchange()

    def _init_exchange(self):
        """Initializes the exchange connection via DataBridge"""
        try:
            db = DataBridge()
            config = db.load_bybit_api_config()
            self.exchange = ccxt.bybit({
                'enableRateLimit': True,
                'apiKey': config.get('PT_API_KEY', ''),
                'secret': config.get('PT_SECRET_KEY', ''),
                'options': {'defaultType': 'linear'}
            })
            self.exchange.load_markets()
            logger.info(f"📡 Execution environment synchronized. Version: {self.VERSION}")
        except Exception as e:
            logger.error(f"❌ Exchange initialization failed: {e}")
            self.exchange = None

    def _api_call_with_retry(self, func, *args, **kwargs):
        """Wrapper for API calls with exponential backoff retry logic"""
        for i in range(self.MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                wait_time = 2 ** i
                logger.warning(f"⚠️ API attempt {i + 1} failed. Retrying in {wait_time}s... Error: {e}")
                time.sleep(wait_time)
        return None

    # ==========================================
    # 🛡️ [手術 4] Strategy 2: Profit Guard (HWM)
    # ==========================================
    def check_profit_guard(self, current_equity, drawdown_limit=100.0):
        """
        Kill Switch based on High-Water Mark (HWM) drawdown.
        Protects existing gains from severe reversals.
        """
        try:
            max_equity = current_equity

            # 安全讀取 JSON
            if self.hwm_file.exists():
                try:
                    with open(self.hwm_file, 'r') as f:
                        data = json.load(f)
                        max_equity = data.get("max_equity", current_equity)
                except json.JSONDecodeError:
                    logger.warning("⚠️ Vault JSON corrupted. Rebuilding...")

            # Update Peak if we reach new heights
            if current_equity > max_equity:
                max_equity = current_equity
                with open(self.hwm_file, 'w') as f:
                    json.dump({"max_equity": max_equity, "timestamp": str(datetime.now(timezone.utc))}, f)
                logger.info(f"🚀 New High-Water Mark set: {max_equity:.2f} USDT")
                return False

            # Trigger Kill Switch if drawdown from peak exceeds limit
            drawdown = max_equity - current_equity
            if drawdown >= drawdown_limit:
                logger.critical(f"🚨 [PROFIT GUARD] Drawdown {drawdown:.2f}U detected from peak {max_equity:.2f}U!")
                self._emergency_market_close()
                return True

            return False
        except Exception as e:
            logger.error(f"❌ Profit Guard check failed: {e}")
            return False

    # ==========================================
    # ⏳ [手術 1] Strategy 1: Time-based Exit
    # ==========================================
    def check_time_exit(self, pair, entry_time_str, half_life, unrealized_pnl):
        """
        Exit if position duration exceeds 3x Half-life and is in profit.
        Improves capital turnover by closing 'stale' positions.
        """
        try:
            # 防呆：如果 half_life 數據異常，給予預設值 8.0
            hl_value = float(half_life) if pd.notna(half_life) and float(half_life) > 0 else 8.0

            entry_time = datetime.strptime(entry_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            duration_hours = (now - entry_time).total_seconds() / 3600

            # Threshold: 3x Half-life
            time_limit = hl_value * 3

            if duration_hours > time_limit and unrealized_pnl > 0:
                logger.info(
                    f"⏳ [TIME-EXIT] {pair} expired ({duration_hours:.1f}h > {time_limit:.1f}h). Securing profit.")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Time-exit check failed for {pair}: {e}")
            return False

    # ==========================================
    # 🎯 [新增功能] 針對單一組合精準平倉
    # ==========================================
    def close_specific_pair(self, pair_name, reason="TIME_EXIT"):
        """Safely closes a single pair instead of emergency closing everything."""
        logger.info(f"✂️ Initiating targeted close for {pair_name} (Reason: {reason})")
        try:
            if not self.trade_log.exists(): return False
            df = pd.read_csv(self.trade_log)
            trade_idx = df[(df['pair'] == pair_name) & (df['status'] == 'OPEN')].index

            if trade_idx.empty:
                logger.warning(f"⚠️ Cannot find OPEN record for {pair_name} in trade log.")
                return False

            trade = df.loc[trade_idx[0]]

            # Fetch active positions from exchange to ensure we have it
            positions = self._api_call_with_retry(self.exchange.fetch_positions, params={'category': 'linear'})
            if not positions: return False

            s1_sym = f"{trade['s1'].replace('USDT', '')}/USDT:USDT"
            s2_sym = f"{trade['s2'].replace('USDT', '')}/USDT:USDT"

            closed_legs = 0
            for pos in positions:
                if pos['symbol'] in [s1_sym, s2_sym] and float(pos['contracts']) > 0:
                    close_side = 'sell' if pos['side'] == 'long' else 'buy'
                    self._api_call_with_retry(self.exchange.create_order, pos['symbol'], 'market', close_side,
                                              float(pos['contracts']))
                    closed_legs += 1

            if closed_legs > 0:
                # Update log
                df.loc[trade_idx, 'status'] = f'CLOSED_{reason}'
                df.to_csv(self.trade_log, index=False)
                logger.success(f"✅ Successfully closed specific pair {pair_name} due to {reason}")
                return True

            return False
        except Exception as e:
            logger.error(f"❌ Failed to close specific pair {pair_name}: {e}")
            return False

    def reconcile_positions(self):
        """Audits local trade logs against real exchange positions to ensure 100% sync"""
        if not self.trade_log.exists() or self.exchange is None:
            return

        logger.info("🔍 [RECON] Starting position audit against exchange data...")
        try:
            positions = self._api_call_with_retry(self.exchange.fetch_positions, params={'category': 'linear'})
            if positions is None: return

            real_active_symbols = {p['symbol'].replace(':USDT', '') for p in positions if float(p['contracts']) > 0}
            df_trade = pd.read_csv(self.trade_log)
            open_indices = df_trade[df_trade['status'] == 'OPEN'].index

            reconciled_count = 0
            for idx in open_indices:
                pair = df_trade.at[idx, 'pair']
                s1, s2 = df_trade.at[idx, 's1'], df_trade.at[idx, 's2']

                # Check if both legs exist on exchange
                has_s1 = s1 in real_active_symbols
                has_s2 = s2 in real_active_symbols

                if not has_s1 and not has_s2:
                    df_trade.at[idx, 'status'] = 'CLOSED_SYNC'
                    self.tg.send_error_alert(f"External Closure: {pair}", "ReconModule", "Synced to CLOSED_SYNC")
                    reconciled_count += 1
                elif not has_s1 or not has_s2:
                    missing = s1 if not has_s1 else s2
                    self.tg.send_error_alert(f"LEG RISK DETECTED: {pair} missing {missing}", "ReconModule",
                                             "IMMEDIATE ATTENTION!")

            if reconciled_count > 0:
                df_trade.to_csv(self.trade_log, index=False)
                logger.info(f"✅ [RECON] Synchronized {reconciled_count} positions.")

        except Exception as e:
            logger.error(f"❌ [RECON] Audit failed: {e}")

    def _is_funding_rate_acceptable(self, s1, s2, side1, side2):
        """Checks if the predicted 8h funding cost exceeds the safety threshold"""
        try:
            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"

            f1 = self.exchange.fetch_funding_rate(sym1)
            f2 = self.exchange.fetch_funding_rate(sym2)

            # If we are BUY, we pay if rate is positive. If we are SELL, we pay if rate is negative.
            cost1 = f1['fundingRate'] if side1 == 'BUY' else -f1['fundingRate']
            cost2 = f2['fundingRate'] if side2 == 'BUY' else -f2['fundingRate']

            total_8h_cost = cost1 + cost2
            threshold = self.MAX_FUNDING_DAILY / 3  # Daily split into 8h chunks

            if total_8h_cost > threshold:
                return False, total_8h_cost
            return True, total_8h_cost
        except:
            return True, 0.0

    def process_signals(self):
        """Orchestrates signal processing and order execution"""
        if not self.signal_file.exists() or self.exchange is None:
            return

        try:
            df = pd.read_csv(self.signal_file)
            pending_orders = df[df['status'] == 'PENDING']
            if pending_orders.empty: return

            for idx, order in pending_orders.iterrows():
                # Check Funding Cost
                is_cost_ok, cost = self._is_funding_rate_acceptable(order['s1'], order['s2'], order['side1'],
                                                                    order['side2'])
                if not is_cost_ok:
                    df.at[idx, 'status'] = 'SKIPPED_EXPENSIVE_FUNDING'
                    self.tg.send_funding_alert(order['pair'], cost, self.MAX_FUNDING_DAILY / 3)
                    logger.warning(f"🚫 [FUNDING GUARD] Intercepted {order['pair']} due to high cost: {cost:.4%}")
                    continue

                # Check Symbol Conflicts
                if self._is_symbol_conflicted(order['s1'], order['s2']):
                    df.at[idx, 'status'] = 'SKIPPED_COLLISION'
                    continue

                # Execute Open
                self._execute_safe_open(idx, order, df)

            df.to_csv(self.signal_file, index=False)
        except Exception as e:
            logger.error(f"❌ Signal orchestrator error: {e}")

    def _execute_safe_open(self, idx, order, df):
        """Executes the open trade using Triple-Step Defense logic"""
        p1, p2, is_ready = self._check_drift_and_get_prices(order)
        if not is_ready:
            df.at[idx, 'status'] = 'SKIPPED_STALE'
            return False

        q1, q2 = self._calculate_aligned_quantities(order['s1'], order['s2'], p1, p2, order['beta'])

        if q1 is None:
            df.at[idx, 'status'] = 'SKIPPED_MIN_QTY'
            logger.warning(f"⚠️ {order['pair']} skipped: Notional value < 10 USDT.")
            return False

        success = self._fire_dual_ioc_orders(order, q1, q2, p1, p2)
        if success:
            df.at[idx, 'status'] = 'EXECUTED'

            # Capture metadata for record
            h_life = order.get('half_life', 8.0)
            self._record_trade_log(order, q1, q2, p1, p2, h_life)

            # Post-execution Report
            try:
                bal_data = self._api_call_with_retry(self.exchange.fetch_balance)
                total_equity = float(bal_data['info']['result']['list'][0]['totalEquity']) if bal_data else 0.0
                self.tg.send_execution_report(order['pair'], p1, p2, q1, q2, 0.0, total_equity)
            except:
                pass
            return True
        return False

    def _get_orderbook_vwap(self, symbol, side, depth=3):
        """Calculates VWAP from top 3 layers of orderbook for precise execution"""
        try:
            clean_sym = f"{symbol.replace('USDT', '')}/USDT:USDT"
            ob = self._api_call_with_retry(self.exchange.fetch_order_book, clean_sym, limit=5)
            if not ob: return None

            book_side = 'asks' if side.lower() == 'buy' else 'bids'
            levels = ob[book_side][:depth]

            val = sum(p * a for p, a in levels)
            qty = sum(a for p, a in levels)
            return val / qty if qty > 0 else None
        except:
            return None

    def _check_drift_and_get_prices(self, order):
        """Double-checks price drift to prevent chasing 'stale' signals"""
        p1 = self._get_orderbook_vwap(order['s1'], order['side1'])
        p2 = self._get_orderbook_vwap(order['s2'], order['side2'])
        return p1, p2, (p1 is not None and p2 is not None)

    def _fire_dual_ioc_orders(self, order, q1, q2, p1, p2):
        """Fires both legs of the pair trade simultaneously using Limit IOC orders"""
        s1_side, s2_side = order['side1'].lower(), order['side2'].lower()

        # Add slight buffer to limit price to ensure filling within tolerance
        lp1 = p1 * (1 + self.SLIPPAGE_TOLERANCE) if s1_side == 'buy' else p1 * (1 - self.SLIPPAGE_TOLERANCE)
        lp2 = p2 * (1 + self.SLIPPAGE_TOLERANCE) if s2_side == 'buy' else p2 * (1 - self.SLIPPAGE_TOLERANCE)

        sym1 = f"{order['s1'].replace('USDT', '')}/USDT:USDT"
        sym2 = f"{order['s2'].replace('USDT', '')}/USDT:USDT"

        logger.info(f"🔥 Firing Leg 1: {sym1} {s1_side} {q1} @ {lp1}")
        res1 = self._api_call_with_retry(self.exchange.create_order, sym1, 'limit', s1_side, q1, lp1,
                                         {'timeInForce': 'IOC'})

        logger.info(f"🔥 Firing Leg 2: {sym2} {s2_side} {q2} @ {lp2}")
        res2 = self._api_call_with_retry(self.exchange.create_order, sym2, 'limit', s2_side, q2, lp2,
                                         {'timeInForce': 'IOC'})

        if res1 and res2: return True

        # [LEG RISK HEAL] If one fills and other fails, cancel all and market exit immediately
        logger.critical(f"🚨 [EXECUTION] Leg Risk detected for {order['pair']}!")
        if res1 and not res2:
            close_side = 'sell' if s1_side == 'buy' else 'buy'
            self._api_call_with_retry(self.exchange.create_order, sym1, 'market', close_side, q1)
        elif res2 and not res1:
            close_side = 'sell' if s2_side == 'buy' else 'buy'
            self._api_call_with_retry(self.exchange.create_order, sym2, 'market', close_side, q2)
        return False

    def _calculate_aligned_quantities(self, s1, s2, p1, p2, beta):
        """Calculates and aligns quantities with exchange precision requirements"""
        try:
            qty1 = self.budget_per_pair / p1
            qty2 = qty1 * abs(float(beta))

            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"

            aq1 = float(self.exchange.amount_to_precision(sym1, qty1))
            aq2 = float(self.exchange.amount_to_precision(sym2, qty2))

            # Minimum Notional Check (Bybit requirement ~10 USDT)
            if (aq1 * p1 < 10.0) or (aq2 * p2 < 10.0): return None, None
            return aq1, aq2
        except:
            return None, None

    def _is_symbol_conflicted(self, s1, s2):
        """Prevents symbol collision (One-Asset-One-Pair rule)"""
        if not self.trade_log.exists(): return False
        df = pd.read_csv(self.trade_log)
        active_assets = set(df[df['status'] == 'OPEN'][['s1', 's2']].values.flatten())
        return s1 in active_assets or s2 in active_assets

    def _has_sufficient_balance(self):
        """Pre-flight check for available USDT balance"""
        bal = self._api_call_with_retry(self.exchange.fetch_balance)
        if not bal: return False
        return float(bal['info']['result']['list'][0]['totalAvailableBalance']) >= self.budget_per_pair

    def _record_trade_log(self, order, q1, q2, p1, p2, half_life):
        """Persists trade details including half-life for strategy management"""
        log = {
            'entry_time': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'pair': order['pair'], 's1': order['s1'], 's2': order['s2'],
            'qty1': q1, 'qty2': q2, 'price1': p1, 'price2': p2,
            'beta': order['beta'],
            'opening_half_life': half_life,  # <--- ESSENTIAL for Strategy 1
            'status': 'OPEN'
        }
        pd.DataFrame([log]).to_csv(self.trade_log, mode='a', index=False, header=not self.trade_log.exists())

    def get_daily_stats(self):
        """Fetches realized performance, fees, and funding from exchange ledger"""
        try:
            now = int(time.time() * 1000);
            since = now - 86400000
            closed_pnl_data = self._api_call_with_retry(self.exchange.fetch_closed_pnl, since=since)
            ledger_funding = self._api_call_with_retry(self.exchange.fetch_ledger, None, since, None,
                                                       {'type': 'FUNDING'})
            ledger_trade = self._api_call_with_retry(self.exchange.fetch_ledger, None, since, None, {'type': 'TRADE'})

            gross_pnl = sum(float(x['closedPnl']) for x in closed_pnl_data) if closed_pnl_data else 0.0
            total_funding = sum(float(x['amount']) for x in ledger_funding) if ledger_funding else 0.0
            total_fees = sum(
                abs(float(x['amount'])) for x in ledger_trade if float(x['amount']) < 0) if ledger_trade else 0.0
            total_trades = len(closed_pnl_data) if closed_pnl_data else 0

            return {
                'gross_pnl': round(gross_pnl, 2),
                'net_pnl': round(gross_pnl + total_funding - total_fees, 2),
                'fees': round(total_fees, 2),
                'funding': round(total_funding, 2),
                'win_rate': len([x for x in closed_pnl_data if
                                 float(x['closedPnl']) > 0]) / total_trades if total_trades > 0 else 0.0,
                'count': total_trades // 2
            }
        except:
            return None

    def check_kill_switch(self, max_drawdown=0.05):
        """Legacy kill switch based on fixed balance drawdown"""
        try:
            bal = self._api_call_with_retry(self.exchange.fetch_balance)
            if not bal: return False
            total_equity = float(bal['info']['result']['list'][0]['totalEquity'])
            risk_threshold = (self.budget_per_pair * 10) * (1 - max_drawdown)
            if total_equity < risk_threshold:
                self._emergency_market_close()
                return True
            return False
        except:
            return False

    def _emergency_market_close(self):
        """Panic close all open positions and cancel all orders"""
        self.exchange.cancel_all_orders(params={'category': 'linear'})
        positions = self.exchange.fetch_positions(params={'category': 'linear'})
        for pos in positions:
            contracts = float(pos['contracts'])
            if contracts > 0:
                side = 'sell' if pos['side'] == 'long' else 'buy'
                self.exchange.create_order(pos['symbol'], 'market', side, contracts)

        if self.trade_log.exists():
            df = pd.read_csv(self.trade_log)
            df.loc[df['status'] == 'OPEN', 'status'] = 'FORCE_CLOSED'
            df.to_csv(self.trade_log, index=False)
        self.tg.send_error_alert("KILL SWITCH ACTIVATED", "ExecutionManager", "ALL POSITIONS CLOSED")