import os
import pandas as pd
import numpy as np
import ccxt
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

class PairMonitor:
    """
    [v3.2.2-Safety] Pair Monitor Module
    Location: /core/pair_monitor.py
    Responsibility: Real-time Z-Score calculation and Cointegration health guarding.
    """
    VERSION = "v3.2.2-Safety"

    def __init__(self):
        # Path definitions
        self.root_dir = Path(__file__).resolve().parent.parent
        self.result_folder = self.root_dir / 'result'
        self.log_filepath = self.result_folder / 'master_research_log.csv'

        # Trade records and Signal paths
        self.trade_record_path = self.root_dir / 'data' / 'trade' / 'trade_record.csv'
        self.signal_folder = self.root_dir / 'data' / 'signal'
        self.signal_table_path = self.signal_folder / 'signal_table.csv'

        # Initialize Exchange (Bybit)
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        self.signal_folder.mkdir(parents=True, exist_ok=True)

        logger.info(f"🛰️ PairMonitor {self.VERSION} Guardian mode online.")

    def get_active_trade_pairs(self):
        if not self.trade_record_path.exists():
            return []
        try:
            df = pd.read_csv(self.trade_record_path)
            if df.empty:
                return []
            active_pairs = df[df['status'] == 'OPEN']['pair'].unique().tolist()
            return active_pairs
        except Exception as e:
            logger.error(f"❌ Failed to read trade records: {e}")
            return []

    def fetch_latest_prices(self, symbols):
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
            logger.error(f"❌ Failed to fetch real-time prices: {e}")
            return {}

    def check_all_pairs(self):
        if not self.log_filepath.exists():
            logger.warning("⚠️ Master research log not found. Monitoring aborted.")
            return

        try:
            df_all = pd.read_csv(self.log_filepath)
            if df_all.empty: return

            latest_ts = df_all['timestamp'].max()
            df_latest = df_all[df_all['timestamp'] == latest_ts]

            active_pairs = self.get_active_trade_pairs()

            watchlist = df_latest[
                (df_latest['p_value'] < 0.05) |
                (df_latest['pair'].isin(active_pairs))
                ].copy()

            if watchlist.empty:
                logger.info("📡 Market is stable. No pairs to monitor.")
                return

            all_needed_symbols = list(set(watchlist['s1'].tolist() + watchlist['s2'].tolist()))
            current_prices = self.fetch_latest_prices(all_needed_symbols)

            signal_data = []

            for _, row in watchlist.iterrows():
                pair_name = row['pair']
                s1, s2 = row['s1'], row['s2']
                p_value = float(row['p_value'])
                beta = float(row['beta'])  # 提取 Beta 值

                # --- [SAFETY GUARD: SIGNAL_EXPIRED] ---
                if pair_name in active_pairs and p_value >= 0.05:
                    logger.critical(f"🚨 {pair_name} relationship broken (P={p_value:.3f})! Forcing exit signal.")
                    signal_data.append({
                        'pair': pair_name,
                        'z_score': 0.0,
                        'p_value': p_value,
                        'beta': beta,  # [修復] 補上 beta
                        'action': 'FORCE_EXIT_EXPIRED',
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                    continue

                # --- [STANDARD MONITORING] ---
                if s1 in current_prices and s2 in current_prices:
                    p1, p2 = current_prices[s1], current_prices[s2]
                    alpha, std = float(row['alpha']), float(row['spread_std'])

                    p1_log = np.log(p1)
                    p2_log = np.log(p2)
                    z_score = (p1_log - (beta * p2_log + alpha)) / std

                    signal_data.append({
                        'pair': pair_name,
                        'z_score': round(z_score, 4),
                        'p_value': round(p_value, 4),
                        'beta': round(beta, 4),  # [修復] 補上 beta，讓執行官能計算雙腿倉位
                        'action': 'MONITORING',
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })

            if signal_data:
                pd.DataFrame(signal_data).to_csv(self.signal_table_path, index=False)

        except Exception as e:
            logger.error(f"❌ Monitoring loop failed: {e}")

    def update_signal_table(self, pair, z_score, p_value, beta, action='MONITORING'):
        try:
            new_data = {
                'pair': [pair],
                'z_score': [z_score],
                'p_value': [p_value],
                'beta': [beta],  # [修復] 補上 beta
                'action': [action],
                'timestamp': [datetime.now(timezone.utc).isoformat()]
            }
            df = pd.DataFrame(new_data)
            df.to_csv(self.signal_table_path, mode='a', header=not self.signal_table_path.exists(), index=False)
        except Exception as e:
            logger.error(f"❌ Manual signal update failed: {e}")