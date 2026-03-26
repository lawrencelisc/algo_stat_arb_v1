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
    [STAGE 2] Pair Screener Module v3.1.2-Stable
    - 核心修復：應用對數價格轉化 (Log-Price Transformation)。
    - 強制監控：確保持倉組合無論數學指標如何，均維持監控，解決 Wait Scan 與「有始有終」問題。
    """
    VERSION = "v3.1.2-Stable"

    def __init__(self):
        self.root_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.root_dir / 'data' / 'rawdata'
        self.result_folder = self.root_dir / 'result'
        logger.info(f'🛰️ PairCombine {self.VERSION} 模組初始化完成。')

    def calculate_half_life(self, spread):
        """計算殘差回歸的半衰期"""
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

    def pair_screener(self, coin_list, timeframe='1h', active_pairs=None):
        """
        全市場配對篩選器
        :param active_pairs: 傳入當前持倉的 Pair 名稱列表 (例如: ['DOGEUSDT-BNBUSDT'])
        """
        logger.info(f"🚀 PairCombine {self.VERSION} 啟動全市場掃描...")
        active_pairs = active_pairs or []

        self.result_folder.mkdir(parents=True, exist_ok=True)
        log_filepath = self.result_folder / 'master_research_log.csv'

        if not self.data_dir.exists():
            logger.error("❌ 原始數據目錄不存在。")
            return None

        files = [f for f in self.data_dir.iterdir() if f.name.endswith('.parquet')]
        if not files:
            logger.warning("⚠️ 未發現任何 Parquet 數據文件。")
            return None

        price_data = {}
        clean_coin_list = [c.split('/')[0] + "USDT" if '/' in c else c.upper() for c in coin_list]
        pd_timeframe = timeframe.lower().replace('m', 'min') if timeframe.endswith('m') else timeframe

        # 1. 載入並重採樣價格數據
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
                # 取最後一筆成交價作為 K 線價格
                df_resampled = df.resample(pd_timeframe).agg({'c': 'last'}).dropna()
                price_data[symbol] = df_resampled['c']
            except Exception as e:
                logger.error(f"❌ 處理 {symbol} 數據失敗: {e}")
                continue

        df_prices = pd.DataFrame(price_data).dropna()
        symbols = df_prices.columns.tolist()
        data_points = len(df_prices)

        if len(symbols) < 2:
            logger.warning("⚠️ 有效幣種不足，無法進行配對。")
            return None

        results = []
        scan_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # 2. 遍歷所有可能的對沖組合
        for sym1, sym2 in combinations(symbols, 2):
            pair_name = f"{sym1}-{sym2}"
            # 🔄 檢查該組合是否正在持倉中 (支援雙向名稱檢查)
            is_active = (pair_name in active_pairs) or (f"{sym2}-{sym1}" in active_pairs)

            raw_y = df_prices[sym1]
            raw_x = df_prices[sym2]

            correlation = raw_y.corr(raw_x)

            # 🛡️ [修正]：如果是持倉中組合，即使相關性低於 0.4 也要計算，否則不予通過
            if correlation < 0.4 and not is_active:
                continue

            try:
                # 🎯 核心運算：將價格轉為對數尺度計算 Log-Beta
                y = np.log(raw_y)
                x = np.log(raw_x)

                score, p_value, _ = coint(y, x)

                # 🛡️ [修正]：如果是持倉中組合，無視 P-Value > 0.05 的過濾門檻
                if p_value >= 0.05 and not is_active:
                    continue

                x_with_const = sm.add_constant(x)
                ols_result = sm.OLS(y, x_with_const).fit()

                alpha = float(ols_result.params.iloc[0])
                beta = float(ols_result.params.iloc[1])  # Log-Beta (彈性系數)

                spread = y - (beta * x + alpha)
                half_life = self.calculate_half_life(spread)

                spread_mean = spread.mean()
                spread_std = spread.std()
                # 計算最新 Z-Score 用於監控
                last_z_score = (spread.iloc[-1] - spread_mean) / spread_std if spread_std != 0 else 0

                results.append({
                    'timestamp': scan_time,
                    'pair': pair_name, 's1': sym1, 's2': sym2,
                    'p_value': float(p_value), 'correlation': float(correlation),
                    'beta': beta, 'alpha': alpha, 'half_life': float(half_life) if not np.isnan(half_life) else 9999.0,
                    'last_z_score': float(last_z_score), 'spread_std': float(spread_std),
                    'last_p1': float(raw_y.iloc[-1]), 'last_p2': float(raw_x.iloc[-1]),
                    'data_points': data_points,
                    'is_active': is_active  # 標註是否為強制計算的單位
                })
            except Exception as e:
                continue

        if not results: return None

        # 3. 排序並存檔
        df_results = pd.DataFrame(results).sort_values(by=['p_value']).reset_index(drop=True)
        df_results['rank'] = df_results.index + 1
        df_results['is_top_10'] = df_results['rank'] <= 10

        cols_order = [
            'timestamp', 'pair', 's1', 's2', 'p_value', 'correlation', 'beta',
            'alpha', 'half_life', 'last_z_score', 'spread_std', 'last_p1',
            'last_p2', 'is_top_10', 'rank', 'data_points', 'is_active'
        ]
        df_results = df_results[cols_order]

        file_exists = log_filepath.exists()
        df_results.to_csv(log_filepath, mode='a', header=not file_exists, index=False)
        logger.success(f"✅ 掃描完成。已將 {len(df_results)} 組結果寫入 master_research_log.csv")
        return df_results