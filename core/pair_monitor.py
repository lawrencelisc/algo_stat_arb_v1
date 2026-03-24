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
    import sys

    root_path = Path(__file__).resolve().parent.parent
    if str(root_path) not in sys.path:
        sys.path.append(str(root_path))
    from utils.tg_wrapper import TelegramReporter


class PairMonitor:
    """
    [STAGE 6 + OPTIMIZATION] Monitoring Radar Module
    Responsible for real-time Z-Score monitoring and Rolling Beta drift detection.
    Updated: Now fully supports Half-Life capture for Time-based Exits.
    """

    # 🚀 [新增] 系統版本號同步
    VERSION = "v2.3.0-Stable"

    root_dir = Path(__file__).resolve().parent.parent
    result_folder = root_dir / 'result'
    log_filepath = result_folder / 'master_research_log.csv'
    signal_folder = root_dir / 'data' / 'signal'
    signal_table_path = signal_folder / 'signal_table.csv'

    def __init__(self):
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        self.tg = TelegramReporter()
        self.signal_folder.mkdir(parents=True, exist_ok=True)
        # 於啟動日誌中顯示版本號
        logger.info(f'🛰️ PairMonitor {self.VERSION} scanner deployed with Rolling Beta & Half-Life support')

    def fetch_latest_prices(self, symbols):
        """Fetches real-time tickers for a list of symbols"""
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
        [STAGE 6] Calculates Rolling Beta from past 24h of 1h OHLCV data.
        """
        try:
            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"

            # Fetch recent 24h window
            ohlcv1 = self.exchange.fetch_ohlcv(sym1, timeframe='1h', limit=24)
            ohlcv2 = self.exchange.fetch_ohlcv(sym2, timeframe='1h', limit=24)

            p1 = pd.Series([x[4] for x in ohlcv1])
            p2 = pd.Series([x[4] for x in ohlcv2])

            if len(p1) < 20 or len(p2) < 20:
                return historical_beta, 0.0

            # Rolling Beta Calculation (OLS)
            x = sm.add_constant(p2)
            model = sm.OLS(p1, x).fit()

            # ✅ [SCO FIX] 修復 Pandas 索引報錯 KeyError: 1，改用 .iloc[1]
            rolling_beta = float(model.params.iloc[1])

            # Calculate Drift Percentage
            drift = abs(rolling_beta - historical_beta) / historical_beta if historical_beta != 0 else 0
            return rolling_beta, drift
        except Exception as e:
            logger.warning(f"⚠️ Beta drift calculation failed for {s1}-{s2}: {e}")
            return historical_beta, 0.0

    def generate_signal(self, pair_name, s1, s2, z_score, beta, half_life):
        """Generates signal table entry and sends Telegram alert"""
        # Check for existing PENDING signals
        if self.signal_table_path.exists():
            df_existing = pd.read_csv(self.signal_table_path)
            is_pending = df_existing[(df_existing['pair'] == pair_name) & (df_existing['status'] == 'PENDING')]
            if not is_pending.empty:
                return

        # Perform Beta Drift Check
        r_beta, drift = self._check_beta_drift(s1, s2, beta)

        side1 = 'BUY' if z_score < -2.0 else 'SELL'
        side2 = 'SELL' if z_score < -2.0 else 'BUY'

        signal_entry = {
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'pair': pair_name, 's1': s1, 's2': s2,
            'side1': side1, 'side2': side2,
            'z_score': round(z_score, 4),
            'beta': beta,
            'rolling_beta': round(r_beta, 4),
            'half_life': round(half_life, 2),  # <--- [OPTIMIZATION] Capture Half-Life
            'status': 'PENDING'
        }

        df_new = pd.DataFrame([signal_entry])
        df_new.to_csv(self.signal_table_path, mode='a', index=False, header=not self.signal_table_path.exists())

        drift_tag = f" [BETA DRIFT: {drift:.1%}]" if drift > 0.15 else ""
        logger.success(f"🎯 SIGNAL: {pair_name}{drift_tag} ({side1}/{side2})")

        # Send TG alert with drift info
        self.tg.send_signal_alert(pair_name, z_score, side1, side2, beta, drift=drift)

    def check_all_pairs(self):
        """Main monitoring radar logic"""
        if not self.log_filepath.exists():
            logger.warning(f"⚠️ Research log not found at {self.log_filepath}")
            return

        try:
            df_pairs = pd.read_csv(self.log_filepath)
            latest_ts = df_pairs['timestamp'].max()
            df_latest = df_pairs[df_pairs['timestamp'] == latest_ts]

            # Monitor Significant Pairs (P-Value < 0.05)
            watchlist = df_latest[df_latest['p_value'] < 0.05].copy()
            if watchlist.empty: return

            all_needed_symbols = list(set(watchlist['s1'].tolist() + watchlist['s2'].tolist()))
            current_prices = self.fetch_latest_prices(all_needed_symbols)

            for _, row in watchlist.iterrows():
                s1, s2 = row['s1'], row['s2']
                if s1 in current_prices and s2 in current_prices:
                    p1, p2 = current_prices[s1], current_prices[s2]
                    beta, alpha, std = float(row['beta']), float(row['alpha']), float(row['spread_std'])

                    # Calculate Z-Score
                    z_score = (p1 - (beta * p2 + alpha)) / std
                    pair_name = row['pair']

                    logger.info(f"📊 {pair_name:20} | Z-Score: {z_score:6.2f}")

                    if abs(z_score) >= 2.0:
                        # [OPTIMIZATION] Passing row['half_life'] to signal generator
                        self.generate_signal(pair_name, s1, s2, z_score, beta, row['half_life'])
                else:
                    logger.warning(f"⚠️ Price data missing for {s1} or {s2}")

        except Exception as e:
            logger.error(f"❌ Critical error in monitor: {e}")