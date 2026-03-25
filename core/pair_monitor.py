import os
import pandas as pd
import numpy as np
import ccxt
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone


class PairMonitor:

    VERSION = "v3.1.0-Stable"

    # 定義路徑 (對齊 algo_stat_arb_v1 架構)
    root_dir = Path(__file__).resolve().parent.parent
    result_folder = root_dir / 'result'
    log_filepath = result_folder / 'master_research_log.csv'

    # 🛡️ 定位攻擊指令存放處
    signal_folder = root_dir / 'data' / 'signal'
    signal_table_path = signal_folder / 'signal_table.csv'

    def __init__(self):
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        self.signal_folder.mkdir(parents=True, exist_ok=True)
        logger.info('🛰️ PairMonitor scanner deployed with Half-Life & LOG-SCALE support')

    def fetch_latest_prices(self, symbols):
        """獲取實時報價"""
        try:
            mapping = {f"{s.replace('USDT', '')}/USDT:USDT": s for s in symbols}
            ccxt_symbols = list(mapping.keys())
            tickers = self.exchange.fetch_tickers(ccxt_symbols, params={'category': 'linear'})

            prices = {}
            for ccxt_id, data in tickers.items():
                if ccxt_id in mapping:
                    csv_key = mapping[ccxt_id]
                    prices[csv_key] = float(data['last'])
            return prices
        except Exception as e:
            logger.error(f"❌ Error fetching prices from Bybit: {e}")
            return {}

    def generate_signal(self, pair_name, s1, s2, z_score, beta, half_life):
        """
        [PHASE 1 UPGRADE] 新增 half_life 參數
        產出指令並傳遞預期壽命數據
        """
        if self.signal_table_path.exists():
            df_existing = pd.read_csv(self.signal_table_path)
            is_pending = df_existing[(df_existing['pair'] == pair_name) & (df_existing['status'] == 'PENDING')]
            if not is_pending.empty:
                return

        side1 = 'BUY' if z_score < -2.0 else 'SELL'
        side2 = 'SELL' if z_score < -2.0 else 'BUY'

        signal_entry = {
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'pair': pair_name,
            's1': s1, 's2': s2,
            'side1': side1, 'side2': side2,
            'z_score': round(z_score, 4),
            'beta': beta,
            'half_life': round(half_life, 2),  # <--- 將半衰期數據寫入指令表
            'status': 'PENDING'
        }

        df_new = pd.DataFrame([signal_entry])
        file_exists = self.signal_table_path.exists()
        df_new.to_csv(self.signal_table_path, mode='a', index=False, header=not file_exists)
        logger.success(f"🎯 SIGNAL: {pair_name} (Z: {z_score:.2f}, HL: {half_life:.1f}h)")

    def check_all_pairs(self):
        """監控主邏輯"""
        if not self.log_filepath.exists():
            logger.warning(f"⚠️ Research log not found")
            return

        try:
            df_pairs = pd.read_csv(self.log_filepath)
            latest_ts = df_pairs['timestamp'].max()
            df_latest = df_pairs[df_pairs['timestamp'] == latest_ts]
            watchlist = df_latest[df_latest['p_value'] < 0.05].copy()

            if watchlist.empty: return

            all_needed_symbols = list(set(watchlist['s1'].tolist() + watchlist['s2'].tolist()))
            current_prices = self.fetch_latest_prices(all_needed_symbols)

            for index, row in watchlist.iterrows():
                s1, s2 = row['s1'], row['s2']
                if s1 in current_prices and s2 in current_prices:
                    p1, p2 = current_prices[s1], current_prices[s2]
                    beta, alpha, std = float(row['beta']), float(row['alpha']), float(row['spread_std'])

                    # 🎯 絕對核心修復：把抓到的實時價格轉成「對數 (Log)」才能和 Log-Beta 對話！
                    p1_log = np.log(p1)
                    p2_log = np.log(p2)

                    # 現在這裡的單位終於對齊了！
                    z_score = (p1_log - (beta * p2_log + alpha)) / std
                    pair_name = row['pair']

                    if abs(z_score) >= 2.0:
                        self.generate_signal(pair_name, s1, s2, z_score, beta, row['half_life'])
        except Exception as e:
            logger.error(f"❌ Critical error in monitor: {e}")