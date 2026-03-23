import os
import pandas as pd
import numpy as np
import ccxt
import statsmodels.api as sm
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

# 確保引入通訊模組
try:
    from utils.tg_wrapper import TelegramReporter
except ImportError:
    # 處理路徑識別問題，確保能找到工具模組
    import sys

    root_path = Path(__file__).resolve().parent.parent
    if str(root_path) not in sys.path:
        sys.path.append(str(root_path))
    from utils.tg_wrapper import TelegramReporter


class PairMonitor:
    """
    [STAGE 6] 監控雷達模組
    負責實時監控 Z-Score 並計算 Rolling Beta 檢測對沖漂移。
    """
    root_dir = Path(__file__).resolve().parent.parent
    result_folder = root_dir / 'result'
    log_filepath = result_folder / 'master_research_log.csv'
    signal_folder = root_dir / 'data' / 'signal'
    signal_table_path = signal_folder / 'signal_table.csv'

    def __init__(self):
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        self.tg = TelegramReporter()
        self.signal_folder.mkdir(parents=True, exist_ok=True)
        logger.info('🛰️ PairMonitor scanner deployed with Rolling Beta & Telegram support')

    def fetch_latest_prices(self, symbols):
        """獲取實時報價"""
        try:
            mapping = {f"{s.replace('USDT', '')}/USDT:USDT": s for s in symbols}
            ccxt_symbols = list(mapping.keys())
            tickers = self.exchange.fetch_tickers(ccxt_symbols, params={'category': 'linear'})

            prices = {mapping[ccxt_id]: float(data['last']) for ccxt_id, data in tickers.items() if ccxt_id in mapping}
            return prices
        except Exception as e:
            logger.error(f"❌ Error fetching prices from Bybit: {e}")
            return {}

    def _check_beta_drift(self, s1, s2, historical_beta):
        """
        [STAGE 6] 計算最近 24 小時的 Rolling Beta，檢查是否與歷史數據偏離過大
        """
        try:
            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"

            # 獲取最近 24 小時的 1h 數據 (Window=24)
            ohlcv1 = self.exchange.fetch_ohlcv(sym1, timeframe='1h', limit=24)
            ohlcv2 = self.exchange.fetch_ohlcv(sym2, timeframe='1h', limit=24)

            p1 = pd.Series([x[4] for x in ohlcv1])
            p2 = pd.Series([x[4] for x in ohlcv2])

            if len(p1) < 20 or len(p2) < 20:
                return historical_beta, 0.0

            # 計算 Rolling Beta (OLS 回歸)
            x = sm.add_constant(p2)
            model = sm.OLS(p1, x).fit()
            rolling_beta = model.params[1]

            # 計算漂離比例
            drift = abs(rolling_beta - historical_beta) / historical_beta if historical_beta != 0 else 0
            return rolling_beta, drift
        except Exception as e:
            logger.warning(f"⚠️ Beta drift calculation failed for {s1}-{s2}: {e}")
            return historical_beta, 0.0

    def generate_signal(self, pair_name, s1, s2, z_score, beta):
        """產出指令並發送 Telegram 警報"""
        # 檢查是否已有 PENDING 指令
        if self.signal_table_path.exists():
            df_existing = pd.read_csv(self.signal_table_path)
            is_pending = df_existing[(df_existing['pair'] == pair_name) & (df_existing['status'] == 'PENDING')]
            if not is_pending.empty:
                return

        # [STAGE 6] 產生訊號前進行最後的對沖比例檢查
        r_beta, drift = self._check_beta_drift(s1, s2, beta)

        side1 = 'BUY' if z_score < -2.0 else 'SELL'
        side2 = 'SELL' if z_score < -2.0 else 'BUY'

        signal_entry = {
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'pair': pair_name, 's1': s1, 's2': s2,
            'side1': side1, 'side2': side2,
            'z_score': round(z_score, 4), 'beta': beta,
            'rolling_beta': round(r_beta, 4),
            'status': 'PENDING'
        }

        df_new = pd.DataFrame([signal_entry])
        df_new.to_csv(self.signal_table_path, mode='a', index=False, header=not self.signal_table_path.exists())

        drift_tag = f" [BETA DRIFT: {drift:.1%}]" if drift > 0.15 else ""
        logger.success(f"🎯 SIGNAL: {pair_name}{drift_tag} ({side1}/{side2})")

        # 發送警報至 Telegram (傳遞漂移數據)
        self.tg.send_signal_alert(pair_name, z_score, side1, side2, beta)

    def check_all_pairs(self):
        """監控主邏輯"""
        if not self.log_filepath.exists():
            logger.warning(f"⚠️ Research log not found at {self.log_filepath}")
            return

        try:
            df_pairs = pd.read_csv(self.log_filepath)
            latest_ts = df_pairs['timestamp'].max()
            df_latest = df_pairs[df_pairs['timestamp'] == latest_ts]

            # 只監控共整合顯著的配對 (P-Value < 0.05)
            watchlist = df_latest[df_latest['p_value'] < 0.05].copy()
            if watchlist.empty: return

            all_needed_symbols = list(set(watchlist['s1'].tolist() + watchlist['s2'].tolist()))
            current_prices = self.fetch_latest_prices(all_needed_symbols)

            for _, row in watchlist.iterrows():
                s1, s2 = row['s1'], row['s2']
                if s1 in current_prices and s2 in current_prices:
                    p1, p2 = current_prices[s1], current_prices[s2]
                    beta, alpha, std = float(row['beta']), float(row['alpha']), float(row['spread_std'])

                    # 計算當前 Z-Score
                    z_score = (p1 - (beta * p2 + alpha)) / std
                    pair_name = row['pair']

                    logger.info(f"📊 {pair_name:20} | Z-Score: {z_score:6.2f}")

                    if abs(z_score) >= 2.0:
                        self.generate_signal(pair_name, s1, s2, z_score, beta)
                else:
                    logger.warning(f"⚠️ Price data missing for {s1} or {s2}")

        except Exception as e:
            logger.error(f"❌ Critical error in monitor: {e}")