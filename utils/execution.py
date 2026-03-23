import pandas as pd
import time
import ccxt
import os
import sys
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

# --- 路徑自愈邏輯 (Path Auto-Heal) ---
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
    工業級執行引擎 v2.1
    功能特點：
        1. 三部曲防禦 (VWAP, Drift, IOC)
        2. 第四階段：自動對帳 (Reconciliation)
        3. 第五階段：Telegram 即時執行通報與攔截回報
        4. 第六階段：資金費率過濾 (Funding Guard)
    """

    def __init__(self, budget_per_pair=100.0):
        self.budget_per_pair = budget_per_pair
        self.root_dir = Path(__file__).resolve().parent.parent
        self.signal_file = self.root_dir / 'data' / 'signal' / 'signal_table.csv'
        self.trade_log = self.root_dir / 'data' / 'trade' / 'trade_record.csv'

        self.trade_log.parent.mkdir(parents=True, exist_ok=True)
        self.signal_file.parent.mkdir(parents=True, exist_ok=True)

        self.DRIFT_THRESHOLD = 0.003
        self.SLIPPAGE_TOLERANCE = 0.001
        self.MAX_RETRIES = 5
        self.MAX_FUNDING_DAILY = 0.0003 # 0.03% 日總利息容忍度

        self.tg = TelegramReporter()
        self._init_exchange()

    def _init_exchange(self):
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
            logger.info("📡 Execution environment synchronized.")
        except Exception as e:
            logger.error(f"❌ Exchange initialization failed: {e}")
            self.exchange = None

    def _api_call_with_retry(self, func, *args, **kwargs):
        for i in range(self.MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                wait_time = 2 ** i
                logger.warning(f"⚠️ API attempt {i + 1} failed. Retrying in {wait_time}s... Error: {e}")
                time.sleep(wait_time)
        return None

    def reconcile_positions(self):
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
                has_s1 = s1 in real_active_symbols
                has_s2 = s2 in real_active_symbols
                if not has_s1 and not has_s2:
                    df_trade.at[idx, 'status'] = 'CLOSED_SYNC'
                    self.tg.send_error_alert(f"External Closure: {pair}", "ReconModule", "Synced to CLOSED_SYNC")
                    reconciled_count += 1
                elif not has_s1 or not has_s2:
                    missing = s1 if not has_s1 else s2
                    self.tg.send_error_alert(f"LEG RISK DETECTED: {pair} missing {missing}", "ReconModule", "IMMEDIATE ATTENTION!")
            if reconciled_count > 0:
                df_trade.to_csv(self.trade_log, index=False)
        except Exception as e:
            logger.error(f"❌ [RECON] Audit failed: {e}")

    def _is_funding_rate_acceptable(self, s1, s2, side1, side2):
        """檢查資金費率"""
        try:
            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"
            f1 = self.exchange.fetch_funding_rate(sym1)
            f2 = self.exchange.fetch_funding_rate(sym2)
            cost1 = f1['fundingRate'] if side1 == 'BUY' else -f1['fundingRate']
            cost2 = f2['fundingRate'] if side2 == 'BUY' else -f2['fundingRate']
            total_8h_cost = cost1 + cost2
            threshold = self.MAX_FUNDING_DAILY / 3
            if total_8h_cost > threshold:
                return False, total_8h_cost
            return True, total_8h_cost
        except:
            return True, 0.0

    def process_signals(self):
        """核心邏輯：修正攔截訊息發送"""
        if not self.signal_file.exists() or self.exchange is None:
            return
        try:
            df = pd.read_csv(self.signal_file)
            pending_orders = df[df['status'] == 'PENDING']
            if pending_orders.empty: return

            for idx, order in pending_orders.iterrows():
                # 1. 資金費率檢查與攔截通報
                is_cost_ok, cost = self._is_funding_rate_acceptable(order['s1'], order['s2'], order['side1'], order['side2'])
                if not is_cost_ok:
                    df.at[idx, 'status'] = 'SKIPPED_EXPENSIVE_FUNDING'
                    self.tg.send_funding_alert(
                        order['pair'],
                        cost,
                        self.MAX_FUNDING_DAILY / 3
                    )
                    logger.warning(f"🚫 [FUNDING GUARD] Intercepted {order['pair']} due to high cost: {cost:.4%}")
                    continue

                if self._is_symbol_conflicted(order['s1'], order['s2']):
                    df.at[idx, 'status'] = 'SKIPPED_COLLISION'
                    continue
                if not self._has_sufficient_balance():
                    break
                self._execute_safe_open(idx, order, df)
            df.to_csv(self.signal_file, index=False)
        except Exception as e:
            logger.error(f"❌ Signal orchestrator error: {e}")

    def _execute_safe_open(self, idx, order, df):
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
            self._record_trade_log(order, q1, q2, p1, p2)
            try:
                bal_data = self._api_call_with_retry(self.exchange.fetch_balance)
                balance = float(bal_data.get('USDT', {}).get('free', 0)) if bal_data else 0.0
                self.tg.send_execution_report(order['pair'], p1, p2, q1, q2, 0.0, balance)
            except: pass
            return True
        return False

    def _get_orderbook_vwap(self, symbol, side, depth=3):
        try:
            clean_sym = f"{symbol.replace('USDT', '')}/USDT:USDT"
            ob = self._api_call_with_retry(self.exchange.fetch_order_book, clean_sym, limit=5)
            if not ob: return None
            book_side = 'asks' if side.lower() == 'buy' else 'bids'
            levels = ob[book_side][:depth]
            val = sum(p * a for p, a in levels)
            qty = sum(a for p, a in levels)
            return val / qty if qty > 0 else None
        except: return None

    def _check_drift_and_get_prices(self, order):
        p1 = self._get_orderbook_vwap(order['s1'], order['side1'])
        p2 = self._get_orderbook_vwap(order['s2'], order['side2'])
        return p1, p2, (p1 is not None and p2 is not None)

    def _fire_dual_ioc_orders(self, order, q1, q2, p1, p2):
        s1_side, s2_side = order['side1'].lower(), order['side2'].lower()
        lp1 = p1 * (1 + self.SLIPPAGE_TOLERANCE) if s1_side == 'buy' else p1 * (1 - self.SLIPPAGE_TOLERANCE)
        lp2 = p2 * (1 + self.SLIPPAGE_TOLERANCE) if s2_side == 'buy' else p2 * (1 - self.SLIPPAGE_TOLERANCE)

        def place():
            sym1 = f"{order['s1'].replace('USDT', '')}/USDT:USDT"
            sym2 = f"{order['s2'].replace('USDT', '')}/USDT:USDT"

            params1 = {'timeInForce': 'IOC', 'positionIdx': 1 if s1_side == 'buy' else 2}
            params2 = {'timeInForce': 'IOC', 'positionIdx': 1 if s2_side == 'buy' else 2}

            self.exchange.create_order(sym1, 'limit', s1_side, q1, lp1, params1)
            self.exchange.create_order(sym2, 'limit', s2_side, q2, lp2, params2)
            return True

        return self._api_call_with_retry(place)

    def _calculate_aligned_quantities(self, s1, s2, p1, p2, beta):
        try:
            qty1 = self.budget_per_pair / p1
            qty2 = qty1 * abs(float(beta))

            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"
            aq1 = float(self.exchange.amount_to_precision(sym1, qty1))
            aq2 = float(self.exchange.amount_to_precision(sym2, qty2))

            if (aq1 * p1 < 10.0) or (aq2 * p2 < 10.0): return None, None
            return aq1, aq2
        except: return None, None

    def _is_symbol_conflicted(self, s1, s2):
        if not self.trade_log.exists(): return False
        df = pd.read_csv(self.trade_log)
        active = set(df[df['status'] == 'OPEN'][['s1', 's2']].values.flatten())
        return s1 in active or s2 in active

    def _has_sufficient_balance(self):
        bal = self._api_call_with_retry(self.exchange.fetch_balance)
        return float(bal.get('USDT', {}).get('free', 0)) >= self.budget_per_pair if bal else False

    def _record_trade_log(self, order, q1, q2, p1, p2):
        log = {
            'entry_time': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'pair': order['pair'], 's1': order['s1'], 's2': order['s2'],
            'qty1': q1, 'qty2': q2, 'price1': p1, 'price2': p2,
            'beta': order['beta'], 'status': 'OPEN'
        }
        pd.DataFrame([log]).to_csv(self.trade_log, mode='a', index=False, header=not self.trade_log.exists())

    def get_daily_stats(self):
        """精密帳務統計"""
        try:
            now = int(time.time() * 1000)
            since = now - 86400000
            closed_pnl_data = self._api_call_with_retry(self.exchange.fetch_closed_pnl, since=since)
            ledger_funding = self._api_call_with_retry(self.exchange.fetch_ledger, None, since, None, {'type': 'FUNDING'})
            ledger_trade = self._api_call_with_retry(self.exchange.fetch_ledger, None, since, None, {'type': 'TRADE'})
            gross_pnl = sum(float(x['closedPnl']) for x in closed_pnl_data) if closed_pnl_data else 0.0
            total_funding = sum(float(x['amount']) for x in ledger_funding) if ledger_funding else 0.0
            total_fees = sum(abs(float(x['amount'])) for x in ledger_trade if float(x['amount']) < 0) if ledger_trade else 0.0
            total_trades = len(closed_pnl_data) if closed_pnl_data else 0
            return {
                'gross_pnl': round(gross_pnl, 2),
                'net_pnl': round(gross_pnl + total_funding - total_fees, 2),
                'fees': round(total_fees, 2),
                'funding': round(total_funding, 2),
                'win_rate': len([x for x in closed_pnl_data if float(x['closedPnl']) > 0]) / total_trades if total_trades > 0 else 0.0,
                'count': total_trades // 2
            }
        except Exception as e:
            logger.error(f"❌ Refined accounting failed: {e}")
            return None

    def check_kill_switch(self, max_drawdown=0.05):
        """檢查權益斷路器"""
        try:
            bal = self._api_call_with_retry(self.exchange.fetch_balance)
            if not bal: return False
            total_equity = float(bal['info']['result']['list'][0]['totalEquity'])
            risk_threshold = (self.budget_per_pair * 10) * (1 - max_drawdown)
            if total_equity < risk_threshold:
                self._emergency_market_close()
                return True
            return False
        except: return False

    def _emergency_market_close(self):
        self.exchange.cancel_all_orders(params={'category': 'linear'})
        positions = self.exchange.fetch_positions(params={'category': 'linear'})
        for pos in positions:
            contracts = float(pos['contracts'])
            if contracts > 0:
                side = 'sell' if pos['side'] == 'long' else 'buy'
                pos_idx = 1 if pos['side'] == 'long' else 2
                self.exchange.create_order(
                    pos['symbol'],
                    'market',
                    side,
                    contracts,
                    params={'positionIdx': pos_idx}
                )

        if self.trade_log.exists():
            df = pd.read_csv(self.trade_log)
            df.loc[df['status'] == 'OPEN', 'status'] = 'FORCE_CLOSED'
            df.to_csv(self.trade_log, index=False)

        self.tg.send_error_alert("KILL SWITCH ACTIVATED", "ExecutionManager", "ALL POSITIONS CLOSED")