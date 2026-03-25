import pandas as pd
import numpy as np
import statsmodels.api as sm
from pathlib import Path
from statsmodels.tsa.stattools import coint
from itertools import combinations
from datetime import datetime, timezone
from loguru import logger

class PairCombine:
    """
    [STAGE 2] Pair Screener Module v3.0.1-Stable
    - ULTIMATE FIX: Log-Price Transformation Applied.
    - Beta is now Elasticity (Log-Beta) to ensure Dollar-Neutrality.
    """
    VERSION = "v3.1.0-Stable"

    def __init__(self):
        self.root_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.root_dir / 'data' / 'rawdata'
        self.result_folder = self.root_dir / 'result'
        logger.info(f'🛰️ PairCombine {self.VERSION} module initialized.')

    def calculate_half_life(self, spread):
        spread = spread.dropna()
        if len(spread) <= 1: return np.nan
        spread_lag = spread.shift(1)
        spread_ret = spread - spread_lag
        spread_ret = spread_ret.dropna()
        spread_lag = spread_lag.dropna()
        spread_lag_with_const = sm.add_constant(spread_lag)
        try:
            model = sm.OLS(spread_ret, spread_lag_with_const)
            res = model.fit()
            theta = res.params.iloc[1]
            if theta >= 0: return np.nan
            return -np.log(2) / theta
        except Exception:
            return np.nan

    def pair_screener(self, coin_list, timeframe='1h'):
        logger.info(f"🚀 PairCombine {self.VERSION} (Log-OLS) starting scan...")

        self.result_folder.mkdir(parents=True, exist_ok=True)
        log_filepath = self.result_folder / 'master_research_log.csv'

        if not self.data_dir.exists(): return None
        files = [f for f in self.data_dir.iterdir() if f.name.endswith('.parquet')]
        if not files: return None

        price_data = {}
        clean_coin_list = [c.split('/')[0] + "USDT" if '/' in c else c.upper() for c in coin_list]
        pd_timeframe = timeframe.lower().replace('m', 'min') if timeframe.endswith('m') else timeframe

        for file_path in files:
            symbol = file_path.name.split('_')[0]
            if symbol not in clean_coin_list: continue
            try:
                df = pd.read_parquet(file_path)
                time_col = next((col for col in ['timestamp', 'time', 'date', 'ts'] if col in df.columns), None)
                if time_col:
                    if pd.api.types.is_numeric_dtype(df[time_col]):
                        df[time_col] = pd.to_datetime(df[time_col], unit='ms' if df[time_col].max() > 1e11 else 's')
                    else:
                        df[time_col] = pd.to_datetime(df[time_col])
                    df.set_index(time_col, inplace=True)
                else:
                    df.index = pd.to_datetime(df.index)

                df = df.sort_index()
                df.columns = [c.lower() for c in df.columns]
                df_resampled = df.resample(pd_timeframe).agg({'c': 'last'}).dropna()
                price_data[symbol] = df_resampled['c']
            except Exception as e:
                continue

        df_prices = pd.DataFrame(price_data).dropna()
        symbols = df_prices.columns.tolist()
        data_points = len(df_prices)

        if len(symbols) < 2: return None

        results = []
        scan_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        for sym1, sym2 in combinations(symbols, 2):
            raw_y = df_prices[sym1]
            raw_x = df_prices[sym2]

            correlation = raw_y.corr(raw_x)
            if correlation < 0.4: continue # 保持正相關過濾

            try:
                # 🎯 核心修復：強制轉為對數計算 (Log-Prices)
                y = np.log(raw_y)
                x = np.log(raw_x)

                score, p_value, _ = coint(y, x)
                x_with_const = sm.add_constant(x)
                ols_result = sm.OLS(y, x_with_const).fit()

                alpha = float(ols_result.params.iloc[0])
                beta = float(ols_result.params.iloc[1]) # 這是正確的 Log-Beta!

                spread = y - (beta * x + alpha)
                half_life = self.calculate_half_life(spread)

                # OLS 的殘差平均值理論上無限趨近於 0
                spread_mean = spread.mean()
                spread_std = spread.std()
                last_z_score = (spread.iloc[-1] - spread_mean) / spread_std if spread_std != 0 else 0

                results.append({
                    'timestamp': scan_time,
                    'pair': f"{sym1}-{sym2}", 's1': sym1, 's2': sym2,
                    'p_value': float(p_value), 'correlation': float(correlation),
                    'beta': beta, 'alpha': alpha, 'half_life': float(half_life) if not np.isnan(half_life) else 9999.0,
                    'last_z_score': float(last_z_score), 'spread_std': float(spread_std),
                    'last_p1': float(raw_y.iloc[-1]), 'last_p2': float(raw_x.iloc[-1]),
                    'data_points': data_points
                })
            except: continue

        if not results: return None

        df_results = pd.DataFrame(results).sort_values(by=['p_value']).reset_index(drop=True)
        df_results['rank'] = df_results.index + 1
        df_results['is_top_10'] = df_results['rank'] <= 10
        cols_order = ['timestamp', 'pair', 's1', 's2', 'p_value', 'correlation', 'beta', 'alpha', 'half_life', 'last_z_score', 'spread_std', 'last_p1', 'last_p2', 'is_top_10', 'rank', 'data_points']
        df_results = df_results[cols_order]

        file_exists = log_filepath.exists()
        df_results.to_csv(log_filepath, mode='a', header=not file_exists, index=False)
        return df_results