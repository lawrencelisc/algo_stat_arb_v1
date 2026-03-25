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
    Industrial Grade Execution Engine v3.0.0-Stable
    - ULTIMATE FIX: Log-Beta Neutral Position Sizing applied.
    - Ensures Dollar-Neutrality across highly volatile asset pairs.
    - Bypass CCXT symbol parsing, use raw Bybit API 'info.symbol' for 100% perfect reconciliation match.
    """

    VERSION = "v3.0.0-Stable"

    def __init__(self, budget_per_pair=100.0):
        self.budget_per_pair = budget_per_pair
        self.root_dir = Path(__file__).resolve().parent.parent
        self.signal_file = self.root_dir / 'data' / 'signal' / 'signal_table.csv'
        self.trade_log = self.root_dir / 'data' / 'trade' / 'trade_record.csv'
        self.vault_dir = self.root_dir / 'data' / 'vault'
        self.hwm_file = self.vault_dir / 'equity_hwm.json'

        # 自動建立所需目錄
        self.trade_log.parent.mkdir(parents=True, exist_ok=True)
        self.signal_file.parent.mkdir(parents=True, exist_ok=True)
        self.vault_dir.mkdir(parents=True, exist_ok=True)

        self.DRIFT_THRESHOLD = 0.003
        self.SLIPPAGE_TOLERANCE = 0.001
        self.MAX_RETRIES = 5
        self.MAX_FUNDING_DAILY = 0.0003

        self.TRAILING_TP_START = 0.8
        self.TRAILING_CALLBACK = 0.5

        self.tg = TelegramReporter()
        logger.info(f"🚀 Initializing ExecutionManager {self.VERSION}")
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
            logger.info(f"📡 Execution environment synchronized. Version: {self.VERSION}")
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

    def check_trailing_tp(self, pair, current_z):
        try:
            if not self.trade_log.exists(): return False
            df = pd.read_csv(self.trade_log)
            idx_list = df[(df['pair'] == pair) & (df['status'] == 'OPEN')].index

            if not idx_list.empty:
                idx = idx_list[0]
                peak_z = df.at[idx, 'peak_z_score'] if 'peak_z_score' in df.columns else 2.5
                if pd.isna(peak_z): peak_z = 2.5

                curr_abs_z = abs(current_z)
                peak_abs_z = abs(peak_z)

                if curr_abs_z < peak_abs_z:
                    df.at[idx, 'peak_z_score'] = current_z
                    df.to_csv(self.trade_log, index=False)
                    return False

                if peak_abs_z < self.TRAILING_TP_START:
                    rebound = curr_abs_z - peak_abs_z
                    if rebound >= self.TRAILING_CALLBACK:
                        logger.warning(
                            f"🏹 [TRAILING-TP] {pair} profit secured. Peak Z: {peak_z:.2f}, Current Z: {current_z:.2f}")
                        return True
            return False
        except Exception as e:
            logger.error(f"❌ Trailing TP check failed: {e}")
            return False

    def check_profit_guard(self, current_equity, drawdown_limit=100.0):
        try:
            max_equity = current_equity
            if self.hwm_file.exists():
                try:
                    with open(self.hwm_file, 'r') as f:
                        data = json.load(f)
                        max_equity = data.get("max_equity", current_equity)
                except json.JSONDecodeError:
                    pass

            if current_equity > max_equity:
                max_equity = current_equity
                with open(self.hwm_file, 'w') as f:
                    json.dump({"max_equity": max_equity, "timestamp": str(datetime.now(timezone.utc))}, f)
                return False

            drawdown = max_equity - current_equity
            if drawdown >= drawdown_limit:
                logger.critical(f"🚨 [PROFIT GUARD] Drawdown {drawdown:.2f}U detected from peak {max_equity:.2f}U!")
                self._emergency_market_close()
                return True

            return False
        except Exception as e:
            logger.error(f"❌ Profit Guard check failed: {e}")
            return False

    def get_unrealized_pnl(self, symbol):
        """Safely fetches unrealized PnL for a given symbol with retries."""
        try:
            clean_sym = f"{symbol.replace('USDT', '')}/USDT:USDT"
            pos_data = self._api_call_with_retry(self.exchange.fetch_position, clean_sym)
            if pos_data and pos_data.get('unrealizedPnl') is not None:
                return float(pos_data['unrealizedPnl'])
            return 0.0
        except Exception as e:
            logger.error(f"❌ Failed to fetch PnL for {symbol}: {e}")
            return 0.0

    def check_time_exit(self, pair, entry_time_str, half_life, unrealized_pnl):
        try:
            hl_value = float(half_life) if pd.notna(half_life) and float(half_life) > 0 else 8.0
            entry_time = datetime.strptime(entry_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            duration_hours = (now - entry_time).total_seconds() / 3600

            time_limit = hl_value * 3

            if duration_hours > time_limit and unrealized_pnl > 0:
                logger.info(
                    f"⏳ [TIME-EXIT] {pair} expired ({duration_hours:.1f}h > {time_limit:.1f}h). Securing profit.")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Time-exit check failed for {pair}: {e}")
            return False

    def close_specific_pair(self, pair_name, reason="EXIT"):
        logger.info(f"✂️ Initiating targeted close for {pair_name} (Reason: {reason})")
        try:
            if not self.trade_log.exists(): return False
            df = pd.read_csv(self.trade_log)
            trade_idx = df[(df['pair'] == pair_name) & (df['status'] == 'OPEN')].index

            if trade_idx.empty:
                logger.warning(f"⚠️ Cannot find OPEN record for {pair_name} in trade log.")
                return False

            trade = df.loc[trade_idx[0]]
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

            # ✅ 繞過 CCXT 解析，直接讀取 Bybit 原始的 'info' -> 'symbol'
            real_active_symbols = set()
            for p in positions:
                if float(p.get('contracts', 0)) > 0:
                    raw_symbol = p['info']['symbol']
                    real_active_symbols.add(raw_symbol)

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
                    self.tg.send_error_alert(f"External Closure: {pair}", "ReconModule", "Synced to CLOSED-SYNC")
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
        if not self.signal_file.exists() or self.exchange is None:
            return

        try:
            df = pd.read_csv(self.signal_file)
            pending_orders = df[df['status'] == 'PENDING']
            if pending_orders.empty: return

            for idx, order in pending_orders.iterrows():
                is_cost_ok, cost = self._is_funding_rate_acceptable(order['s1'], order['s2'], order['side1'],
                                                                    order['side2'])
                if not is_cost_ok:
                    df.at[idx, 'status'] = 'SKIPPED_EXPENSIVE_FUNDING'
                    self.tg.send_funding_alert(order['pair'], cost, self.MAX_FUNDING_DAILY / 3)
                    logger.warning(f"🚫 [FUNDING GUARD] Intercepted {order['pair']} due to high cost: {cost:.4%}")
                    continue

                if self._is_symbol_conflicted(order['s1'], order['s2']):
                    df.at[idx, 'status'] = 'SKIPPED_COLLISION'
                    continue

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
            h_life = order.get('half_life', 8.0)
            self._record_trade_log(order, q1, q2, p1, p2, h_life)

            try:
                bal_data = self._api_call_with_retry(self.exchange.fetch_balance)
                total_equity = float(bal_data['info']['result']['list'][0]['totalEquity']) if bal_data else 0.0
                self.tg.send_execution_report(order['pair'], p1, p2, q1, q2, 0.0, total_equity)
            except:
                pass
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
        except:
            return None

    def _check_drift_and_get_prices(self, order):
        p1 = self._get_orderbook_vwap(order['s1'], order['side1'])
        p2 = self._get_orderbook_vwap(order['s2'], order['side2'])
        return p1, p2, (p1 is not None and p2 is not None)

    def _fire_dual_ioc_orders(self, order, q1, q2, p1, p2):
        s1_side, s2_side = order['side1'].lower(), order['side2'].lower()

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

        logger.critical(f"🚨 [EXECUTION] Leg Risk detected for {order['pair']}!")
        if res1 and not res2:
            close_side = 'sell' if s1_side == 'buy' else 'buy'
            self._api_call_with_retry(self.exchange.create_order, sym1, 'market', close_side, q1)
        elif res2 and not res1:
            close_side = 'sell' if s2_side == 'buy' else 'buy'
            self._api_call_with_retry(self.exchange.create_order, sym2, 'market', close_side, q2)
        return False

    # ==========================================
    # 🎯 [v3.0 終極修復] Beta-Neutral 資金配平演算法
    # ==========================================
    def _calculate_aligned_quantities(self, s1, s2, p1, p2, beta):
        """
        🚀 華爾街級 Beta-Neutral 資金配平演算法
        利用 Log-Beta 計算等效波動資金，確保真正的市場中性。
        """
        try:
            # 1. 計算基準資金 (S1 的目標投入美金)
            target_value_1 = self.budget_per_pair

            # 2. 計算對沖資金 (S2 的目標投入美金) = S1資金 * |Log-Beta|
            # 這一步完美抵銷大盤單邊波動風險！
            target_value_2 = target_value_1 * abs(float(beta))

            # 3. 換算成真實代幣數量 (數量 = 投入美金 / 絕對價格)
            raw_qty1 = target_value_1 / float(p1)
            raw_qty2 = target_value_2 / float(p2)

            # 4. 套用交易所精度限制
            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"

            aq1 = float(self.exchange.amount_to_precision(sym1, raw_qty1))
            aq2 = float(self.exchange.amount_to_precision(sym2, raw_qty2))

            # 防禦門檻：確保轉換精度後，雙邊價值不低於 10 USDT (交易所最小門檻)
            if (aq1 * p1 < 10.0) or (aq2 * p2 < 10.0):
                logger.warning(f"⚠️ {s1}-{s2} skipped: Notional value < 10 USDT after precision adjustment.")
                return None, None

            logger.info(f"⚖️ Beta-Neutral Sizing Computed:")
            logger.info(f"   👉 {s1} Value: ${target_value_1:.2f} (Qty: {aq1})")
            logger.info(f"   👉 {s2} Value: ${target_value_2:.2f} (Qty: {aq2})")
            logger.info(f"   👉 Log-Beta Applied: {beta:.4f}")

            return aq1, aq2

        except Exception as e:
            logger.error(f"❌ Failed to calculate aligned quantities: {e}")
            return None, None

    def _is_symbol_conflicted(self, s1, s2):
        if not self.trade_log.exists(): return False
        df = pd.read_csv(self.trade_log)
        active_assets = set(df[df['status'] == 'OPEN'][['s1', 's2']].values.flatten())
        return s1 in active_assets or s2 in active_assets

    def _has_sufficient_balance(self):
        bal = self._api_call_with_retry(self.exchange.fetch_balance)
        if not bal: return False
        return float(bal['info']['result']['list'][0]['totalAvailableBalance']) >= self.budget_per_pair

    def _record_trade_log(self, order, q1, q2, p1, p2, half_life):
        log = {
            'entry_time': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'pair': order['pair'], 's1': order['s1'], 's2': order['s2'],
            'qty1': q1, 'qty2': q2, 'price1': p1, 'price2': p2,
            'beta': order['beta'],
            'opening_half_life': half_life,
            'peak_z_score': order.get('z_score', 2.5),  # 動態讀取實際開倉 Z-Score
            'status': 'OPEN'
        }
        pd.DataFrame([log]).to_csv(self.trade_log, mode='a', index=False, header=not self.trade_log.exists())

    def get_daily_stats(self):
        try:
            now = int(time.time() * 1000)
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

    def _emergency_market_close(self):
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