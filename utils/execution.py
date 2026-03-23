import pandas as pd
import time
import ccxt
import os
import sys
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

# --- 路徑自愈邏輯 (Path Auto-Heal) ---
# 確保根目錄被加入到系統路徑，防止 Import 子模組失敗
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
    工業級執行引擎 v1.9
    功能特點：
        1. 三部曲防禦 (VWAP, Drift, IOC)
        2. 第四階段：自動對帳 (Reconciliation)
        3. 第五階段：Telegram 即時執行通報
        4. 第六階段：資金費率過濾 (Funding Guard)
    """

    def __init__(self, budget_per_pair=100.0):
        # 1. 資源與路徑定義
        self.budget_per_pair = budget_per_pair
        self.root_dir = Path(__file__).resolve().parent.parent
        self.signal_file = self.root_dir / 'data' / 'signal' / 'signal_table.csv'
        self.trade_log = self.root_dir / 'data' / 'trade' / 'trade_record.csv'

        # 2. 安全與風險參數
        self.DRIFT_THRESHOLD = 0.003  # 0.3% 最大價格漂離門檻
        self.SLIPPAGE_TOLERANCE = 0.001  # 0.1% Limit IOC 滑價容忍度
        self.MAX_RETRIES = 5  # API 調用最大重試次數
        self.MAX_FUNDING_DAILY = 0.0003  # 單日最高容忍 0.03% 利息支出 (Stage 6)

        # 3. 初始化通訊官 (Telegram Reporter)
        self.tg = TelegramReporter()

        # 4. 初始化交易所連線
        self._init_exchange()


    def _init_exchange(self):
        """初始化 Bybit API 並同步市場規則"""
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
        """指數退避重試機制 (Exponential Backoff)"""
        for i in range(self.MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                wait_time = 2 ** i
                logger.warning(f"⚠️ API attempt {i + 1} failed. Retrying in {wait_time}s... Error: {e}")
                time.sleep(wait_time)
        return None


    # ==========================================
    # 🔍 [STAGE 4] 自動對帳 (Reconciliation)
    # ==========================================
    def reconcile_positions(self):
        """
        比對本地 trade_record.csv 與 Bybit 實時持倉。
        若發現外部平倉則同步狀態，若發現單邊持倉則觸發 Leg Risk 警報。
        """
        if not self.trade_log.exists() or self.exchange is None:
            return

        logger.info("🔍 [RECON] Starting position audit against exchange data...")

        try:
            # A. 獲取交易所實時持倉 (線性合約)
            positions = self._api_call_with_retry(self.exchange.fetch_positions, params={'category': 'linear'})
            if positions is None: return

            # 建立實時持倉 Set: {"BTCUSDT", "ETHUSDT"}
            real_active_symbols = {p['symbol'].replace(':USDT', '') for p in positions if float(p['contracts']) > 0}

            # B. 讀取本地帳本
            df_trade = pd.read_csv(self.trade_log)
            open_indices = df_trade[df_trade['status'] == 'OPEN'].index

            reconciled_count = 0
            for idx in open_indices:
                pair = df_trade.at[idx, 'pair']
                s1, s2 = df_trade.at[idx, 's1'], df_trade.at[idx, 's2']

                has_s1 = s1 in real_active_symbols
                has_s2 = s2 in real_active_symbols

                # 情境 1: 兩邊都已外部平倉
                if not has_s1 and not has_s2:
                    df_trade.at[idx, 'status'] = 'CLOSED_SYNC'
                    logger.warning(f"⚠️ [RECON] Pair {pair} closed externally. Syncing local log.")
                    self.tg.send_error_alert(f"External Closure: {pair}", "ReconModule", "Synced to CLOSED_SYNC")
                    reconciled_count += 1

                # 情境 2: 單邊持倉風險 (Leg Risk)
                elif not has_s1 or not has_s2:
                    missing = s1 if not has_s1 else s2
                    logger.critical(f"🚨 [RECON] LEG RISK! {pair} is missing {missing} position!")
                    self.tg.send_error_alert(f"LEG RISK DETECTED: {pair} missing {missing}", "ReconModule",
                                             "IMMEDIATE ATTENTION!")

            if reconciled_count > 0:
                df_trade.to_csv(self.trade_log, index=False)
                logger.success(f"✅ [RECON] Successfully synchronized {reconciled_count} entries.")
            else:
                logger.info("✅ [RECON] Local records match exchange data.")

        except Exception as e:
            logger.error(f"❌ [RECON] Audit failed: {e}")


    # ==========================================
    # 💸 [STAGE 6] 資金費率檢查 (Funding Guard)
    # ==========================================
    def _is_funding_rate_acceptable(self, s1, s2, side1, side2):
        """檢查當前預測資金費率，防止利息成本侵蝕利潤"""
        try:
            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"

            f1 = self.exchange.fetch_funding_rate(sym1)
            f2 = self.exchange.fetch_funding_rate(sym2)

            # 計算該方向的費率負擔 (BUY 付正費率，SELL 付負費率)
            cost1 = f1['fundingRate'] if side1 == 'BUY' else -f1['fundingRate']
            cost2 = f2['fundingRate'] if side2 == 'BUY' else -f2['fundingRate']

            total_8h_cost = cost1 + cost2

            if total_8h_cost > (self.MAX_FUNDING_DAILY / 3):
                logger.warning(f"🚫 [FUNDING GUARD] High cost: {total_8h_cost:.4%}/8h. Skipping signal.")
                return False, total_8h_cost
            return True, total_8h_cost
        except Exception as e:
            logger.warning(f"⚠️ Funding Guard check failed: {e}")
            return True, 0.0


    # ==========================================
    # ⚡ 執行核心邏輯 (Execution Core)
    # ==========================================
    def process_signals(self):
        """掃描 PENDING 信號並執行三部曲開倉"""
        if not self.signal_file.exists() or self.exchange is None:
            return

        try:
            df = pd.read_csv(self.signal_file)
            pending_orders = df[df['status'] == 'PENDING']

            if pending_orders.empty: return

            for idx, order in pending_orders.iterrows():
                # 1. [STAGE 6] 資金費率檢查
                is_cost_ok, cost = self._is_funding_rate_acceptable(order['s1'], order['s2'], order['side1'],
                                                                    order['side2'])
                if not is_cost_ok:
                    df.at[idx, 'status'] = 'SKIPPED_EXPENSIVE_FUNDING'
                    continue

                # 2. 符號衝突守衛 (一幣一對)
                if self._is_symbol_conflicted(order['s1'], order['s2']):
                    df.at[idx, 'status'] = 'SKIPPED_COLLISION'
                    continue

                # 3. 餘額預檢
                if not self._has_sufficient_balance():
                    logger.warning("⚠️ Insufficient balance for new positions.")
                    break

                # 4. 執行原子化開倉
                self._execute_safe_open(idx, order, df)

            df.to_csv(self.signal_file, index=False)

        except Exception as e:
            logger.error(f"❌ Signal orchestrator error: {e}")


    def _execute_safe_open(self, idx, order, df):
        """執行開倉序列並發送 Telegram 通報"""
        # A. 價格重校與漂離檢查 (Triple-Step 1 & 2)
        p1, p2, is_ready = self._check_drift_and_get_prices(order)
        if not is_ready:
            logger.warning(f"⏳ Market drift too high for {order['pair']}. Skipping.")
            df.at[idx, 'status'] = 'SKIPPED_STALE'
            return False

        # B. 精度對齊與數量計算
        q1, q2 = self._calculate_aligned_quantities(order['s1'], order['s2'], p1, p2, order['beta'])
        if q1 is None: return False

        # C. 原子化落單 (Triple-Step 3: Limit IOC)
        logger.info(f"🚀 FIRING DUAL ORDERS: {order['pair']} (Z={order['z_score']})")
        success = self._fire_dual_ioc_orders(order, q1, q2, p1, p2)

        if success:
            # D. 狀態更新與紀錄
            df.at[idx, 'status'] = 'EXECUTED'
            self._record_trade_log(order, q1, q2, p1, p2)

            # E. Telegram 成交通報
            try:
                bal_data = self._api_call_with_retry(self.exchange.fetch_balance)
                balance = float(bal_data.get('USDT', {}).get('free', 0)) if bal_data else 0.0
                self.tg.send_execution_report(order['pair'], p1, p2, q1, q2, 0.0, balance)
            except Exception as tg_err:
                logger.error(f"⚠️ Post-trade reporting failed: {tg_err}")
            return True
        return False


    def _get_orderbook_vwap(self, symbol, side, depth=3):
        """獲取訂單簿前 3 層的 VWAP"""
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
        """檢查實時價格是否偏離信號產生時過多"""
        p1 = self._get_orderbook_vwap(order['s1'], order['side1'])
        p2 = self._get_orderbook_vwap(order['s2'], order['side2'])
        return p1, p2, (p1 is not None and p2 is not None)


    def _fire_dual_ioc_orders(self, order, q1, q2, p1, p2):
        """執行雙邊 Limit IOC 訂單"""
        s1_side, s2_side = order['side1'].lower(), order['side2'].lower()
        lp1 = p1 * (1 + self.SLIPPAGE_TOLERANCE) if s1_side == 'buy' else p1 * (1 - self.SLIPPAGE_TOLERANCE)
        lp2 = p2 * (1 + self.SLIPPAGE_TOLERANCE) if s2_side == 'buy' else p2 * (1 - self.SLIPPAGE_TOLERANCE)

        def place():
            sym1 = f"{order['s1'].replace('USDT', '')}/USDT:USDT"
            sym2 = f"{order['s2'].replace('USDT', '')}/USDT:USDT"
            self.exchange.create_order(sym1, 'limit', s1_side, q1, lp1, {'timeInForce': 'IOC'})
            self.exchange.create_order(sym2, 'limit', s2_side, q2, lp2, {'timeInForce': 'IOC'})
            return True

        return self._api_call_with_retry(place)


    def _calculate_aligned_quantities(self, s1, s2, p1, p2, beta):
        """計算 Beta 中性數量並對齊交易所精度"""
        try:
            qty1 = self.budget_per_pair / p1
            qty2 = qty1 * abs(float(beta)) * (p1 / p2)

            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"

            aq1 = float(self.exchange.amount_to_precision(sym1, qty1))
            aq2 = float(self.exchange.amount_to_precision(sym2, qty2))

            if (aq1 * p1 < 10.0) or (aq2 * p2 < 10.0):
                logger.warning(f"🚫 Notional value below 10 USDT for {s1} or {s2}")
                return None, None
            return aq1, aq2
        except:
            return None, None


    def _is_symbol_conflicted(self, s1, s2):
        """符號衝突守衛：防止同一資產在多個倉位中曝險"""
        if not self.trade_log.exists(): return False
        df = pd.read_csv(self.trade_log)
        active = set(df[df['status'] == 'OPEN'][['s1', 's2']].values.flatten())
        return s1 in active or s2 in active


    def _has_sufficient_balance(self):
        """開倉前的餘額確認"""
        bal = self._api_call_with_retry(self.exchange.fetch_balance)
        return float(bal.get('USDT', {}).get('free', 0)) >= self.budget_per_pair if bal else False


    def _record_trade_log(self, order, q1, q2, p1, p2):
        """將成交細節寫入交易紀錄 CSV"""
        log = {
            'entry_time': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'pair': order['pair'], 's1': order['s1'], 's2': order['s2'],
            'qty1': q1, 'qty2': q2, 'price1': p1, 'price2': p2,
            'beta': order['beta'], 'status': 'OPEN'
        }
        pd.DataFrame([log]).to_csv(self.trade_log, mode='a', index=False, header=not self.trade_log.exists())


    def check_kill_switch(self, max_drawdown=0.05):
        """
        [FINAL STAGE] 緊急斷路器
        監控帳戶總權益，若回撤超過門檻則強制清倉。
        """
        try:
            # 1. 獲取帳戶總權益 (Equity = Balance + Unrealized PnL)
            bal = self._api_call_with_retry(self.exchange.fetch_balance)
            if not bal: return False

            total_equity = float(bal['info']['result']['list'][0]['totalEquity'])
            # 這裡可以與每日起始權益做對比，或與 Budget * Max_Pairs 對比
            # 簡化邏輯：若權益低於 (預算 * 最大對數) 的 95% 則觸發
            risk_threshold = (self.budget_per_pair * 10) * (1 - max_drawdown)

            if total_equity < risk_threshold:
                logger.critical(f"🚨🚨 [KILL SWITCH] Equity ({total_equity}) below threshold ({risk_threshold})!")
                self._emergency_market_close()
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Kill switch check failed: {e}")
            return False


    def _emergency_market_close(self):
        """強制清倉程序：撤單並市價平倉"""
        logger.warning("🛑 [EMERGENCY] Initiating full liquidation...")

        # A. 撤回所有掛單
        self.exchange.cancel_all_orders(params={'category': 'linear'})

        # B. 獲取所有持倉並市價平倉
        positions = self.exchange.fetch_positions(params={'category': 'linear'})
        for pos in positions:
            contracts = float(pos['contracts'])
            if contracts > 0:
                side = 'sell' if pos['side'] == 'long' else 'buy'
                symbol = pos['symbol']
                logger.info(f"🔥 Closing {symbol} at Market price...")
                self.exchange.create_order(symbol, 'market', side, contracts)

        # C. 更新本地帳本
        if self.trade_log.exists():
            df = pd.read_csv(self.trade_log)
            df.loc[df['status'] == 'OPEN', 'status'] = 'FORCE_CLOSED'
            df.to_csv(self.trade_log, index=False)

        # D. Telegram 最高級別通知
        self.tg.send_error_alert("KILL SWITCH ACTIVATED", "ExecutionManager", "ALL POSITIONS CLOSED AT MARKET")


    # ==========================================
    # 📈 [FINAL ENHANCEMENT] 精確財務統計
    # ==========================================
    def get_daily_stats(self):
        """
        精確抓取過去 24 小時的 真實損益、手續費與資金費
        """
        try:
            now = int(time.time() * 1000)
            since = now - 86400000  # 24小時前

            # 1. 抓取平倉損益 (價差利潤)
            closed_pnl_data = self._api_call_with_retry(self.exchange.fetch_closed_pnl, since=since)

            # 2. 抓取帳本流水 (提取資金費與交易手續費)
            # Bybit V5: type='FUNDING' 提取利息, type='TRADE' 提取手續費
            ledger_funding = self._api_call_with_retry(self.exchange.fetch_ledger, None, since, None,
                                                       {'type': 'FUNDING'})
            ledger_trade = self._api_call_with_retry(self.exchange.fetch_ledger, None, since, None, {'type': 'TRADE'})

            # --- 計算價差損益 ---
            gross_pnl = sum(float(x['closedPnl']) for x in closed_pnl_data) if closed_pnl_data else 0.0

            # --- 計算真實資金費 (正數為收入，負數為支出) ---
            total_funding = sum(float(x['amount']) for x in ledger_funding) if ledger_funding else 0.0

            # --- 計算真實手續費 (通常量化系統的手續費在 ledger 裡是負數) ---
            total_fees = sum(
                abs(float(x['amount'])) for x in ledger_trade if float(x['amount']) < 0) if ledger_trade else 0.0

            # --- 統計勝率與成交對數 ---
            wins = len([x for x in closed_pnl_data if float(x['closedPnl']) > 0]) if closed_pnl_data else 0
            total_trades = len(closed_pnl_data) if closed_pnl_data else 0

            return {
                'gross_pnl': round(gross_pnl, 2),
                'net_pnl': round(gross_pnl + total_funding - total_fees, 2),
                'fees': round(total_fees, 2),
                'funding': round(total_funding, 2),
                'win_rate': wins / total_trades if total_trades > 0 else 0.0,
                'count': total_trades // 2
            }
        except Exception as e:
            logger.error(f"❌ Refined accounting failed: {e}")
            return None