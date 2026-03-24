import os
import pandas as pd
import numpy as np
import ccxt
import statsmodels.api as sm
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

class PairMonitor:
    VERSION = "v2.4.0-Stable"

    def __init__(self):
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        self.root_dir = Path(__file__).resolve().parent.parent
        self.log_filepath = self.root_dir / 'result' / 'master_research_log.csv'
        self.signal_table_path = self.root_dir / 'data' / 'signal' / 'signal_table.csv'
        logger.info(f'🛰️ PairMonitor {self.VERSION} radar online.')

    def fetch_latest_prices(self, symbols):
        try:
            mapping = {f"{s.replace('USDT', '')}/USDT:USDT": s for s in symbols}
            tickers = self.exchange.fetch_tickers(list(mapping.keys()), params={'category': 'linear'})
            return {mapping[k]: float(v['last']) for k, v in tickers.items() if k in mapping}
        except: return {}

    def _check_beta_drift(self, s1, s2, historical_beta):
        """修復 iloc[1] 索引錯誤"""
        try:
            sym1 = f"{s1.replace('USDT', '')}/USDT:USDT"
            sym2 = f"{s2.replace('USDT', '')}/USDT:USDT"
            ohlcv1 = self.exchange.fetch_ohlcv(sym1, timeframe='1h', limit=24)
            ohlcv2 = self.exchange.fetch_ohlcv(sym2, timeframe='1h', limit=24)
            p1, p2 = pd.Series([x[4] for x in ohlcv1]), pd.Series([x[4] for x in ohlcv2])
            x = sm.add_constant(p2)
            model = sm.OLS(p1, x).fit()
            rolling_beta = float(model.params.iloc[1]) # ✅ 使用 iloc[1]
            drift = abs(rolling_beta - historical_beta) / historical_beta if historical_beta != 0 else 0
            return rolling_beta, drift
        except: return historical_beta, 0.0

    def generate_signal(self, pair_name, s1, s2, z_score, beta, half_life):
        r_beta, drift = self._check_beta_drift(s1, s2, beta)
        signal = {
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'pair': pair_name, 's1': s1, 's2': s2,
            'side1': 'BUY' if z_score < -2.0 else 'SELL',
            'side2': 'SELL' if z_score < -2.0 else 'BUY',
            'z_score': round(z_score, 4), 'beta': beta, 'rolling_beta': round(r_beta, 4),
            'half_life': round(half_life, 2), 'status': 'PENDING'
        }
        pd.DataFrame([signal]).to_csv(self.signal_table_path, mode='a', index=False, header=not self.signal_table_path.exists())
        logger.success(f"🎯 SIGNAL: {pair_name} [Drift: {drift:.1%}]")

    def check_all_pairs(self):
        if not self.log_filepath.exists(): return
        df = pd.read_csv(self.log_filepath)
        latest = df[df['timestamp'] == df['timestamp'].max()]
        watchlist = latest[latest['p_value'] < 0.05]
        prices = self.fetch_latest_prices(list(set(watchlist['s1'].tolist() + watchlist['s2'].tolist())))
        for _, row in watchlist.iterrows():
            if row['s1'] in prices and row['s2'] in prices:
                z = (prices[row['s1']] - (float(row['beta']) * prices[row['s2']] + float(row['alpha']))) / float(row['spread_std'])
                logger.info(f"📊 {row['pair']:20} | Z: {z:6.2f}")
                if abs(z) >= 2.0:
                    self.generate_signal(row['pair'], row['s1'], row['s2'], z, row['beta'], row['half_life'])