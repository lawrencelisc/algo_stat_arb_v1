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
    [大腦] Pair Screener: 嚴格篩選同向、共整合的配對
    """
    VERSION = "v4.0.0-FirstPrinciples"

    def __init__(self):
        self.root_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.root_dir / 'data' / 'rawdata'
        self.result_folder = self.root_dir / 'result'
        self.log_filepath = self.result_folder / 'master_research_log.csv'
        self.result_folder.mkdir(parents=True, exist_ok=True)

    def calculate_half_life(self, spread):
        spread = spread.dropna()
        if len(spread) <= 1: return 9999.0
        spread_lag = spread.shift(1)
        spread_ret = spread - spread_lag
        valid_idx = spread_ret.index[1:]
        y = spread_ret.loc[valid_idx]
        x = sm.add_constant(spread_lag.loc[valid_idx])
        try:
            res = sm.OLS(y, x).fit()
            theta = res.params.iloc[1]
            return -np.log(2) / theta if theta < 0 else 9999.0
        except:
            return 9999.0

    def load_log_prices(self, symbol_list, timeframe='1h'):
        """載入數據並對齊時間軸"""
        series_dict = {}
        for sym in symbol_list:
            files = list(self.data_dir.glob(f"{sym}_{timeframe}_*.parquet"))
            if not files: continue
            latest_file = max(files, key=lambda x: x.stat().st_mtime)
            try:
                df = pd.read_parquet(latest_file)
                if df.empty: continue
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index)
                series_dict[sym] = np.log(df.sort_index()['c'])
            except Exception as e:
                logger.error(f"Failed to load {sym}: {e}")

        if not series_dict: return pd.DataFrame()
        # concat 後 ffill 填補微小空缺，確保矩陣穩健
        return pd.concat(series_dict, axis=1).sort_index().ffill(limit=3)

    def pair_screener(self, symbol_list, timeframe='1h', active_pairs=None):
        logger.info(f"🧠 系統重置：開始嚴格共整合篩選 ({len(symbol_list)} symbols)")
        active_pairs = active_pairs or []
        price_matrix = self.load_log_prices(symbol_list, timeframe)
        if price_matrix.empty: return

        all_combos = list(combinations(price_matrix.columns, 2))
        for ap in active_pairs:
            try:
                s1, s2 = ap.split('-')
                if s1 in price_matrix.columns and s2 in price_matrix.columns and (s1, s2) not in all_combos:
                    all_combos.append((s1, s2))
            except:
                continue

        results = []
        scan_time = datetime.now(timezone.utc).isoformat()

        for s1, s2 in all_combos:
            try:
                # 獨立去除 NaN，確保即使長度不同也能計算
                pair_df = price_matrix[[s1, s2]].dropna()
                if len(pair_df) < 100: continue
                y, x = pair_df[s1], pair_df[s2]
                pair_name = f"{s1}-{s2}"
                is_active = pair_name in active_pairs

                # 🛡️ 鐵血防線 1：必須是強正相關 (Correlation > 0.5)
                correlation = y.corr(x)
                if correlation < 0.5 and not is_active: continue

                x_with_const = sm.add_constant(x)
                model = sm.OLS(y, x_with_const).fit()
                beta = model.params.iloc[1]

                # 🛡️ 鐵血防線 2：Beta 必須大於 0
                if beta <= 0 and not is_active: continue

                # 🛡️ 鐵血防線 3：P-Value 必須過關 (強共整合)
                _, p_value, _ = coint(y, x)
                if p_value >= 0.05 and not is_active: continue

                alpha = model.params.iloc[0]
                spread = y - (beta * x + alpha)
                spread_std = spread.std()
                # Z-Score 正規化
                last_z = (spread.iloc[-1] - spread.mean()) / spread_std if spread_std != 0 else 0

                results.append({
                    'timestamp': scan_time, 'pair': pair_name, 's1': s1, 's2': s2,
                    'p_value': round(float(p_value), 5), 'correlation': round(float(correlation), 4),
                    'beta': round(float(beta), 4), 'alpha': round(float(alpha), 4),
                    'spread_std': round(float(spread_std), 6), 'last_z_score': round(float(last_z), 4),
                    'half_life': round(self.calculate_half_life(spread), 2),
                    'last_p1': round(np.exp(y.iloc[-1]), 6), 'last_p2': round(np.exp(x.iloc[-1]), 6),
                    'data_points': len(y), 'is_active': is_active
                })
            except:
                continue

        if results:
            df_results = pd.DataFrame(results).sort_values(by='p_value').reset_index(drop=True)
            df_results['rank'] = df_results.index + 1
            df_results['is_top_10'] = df_results['rank'] <= 10
            df_results.to_csv(self.log_filepath, mode='a', index=False, header=not self.log_filepath.exists())
            logger.success(f"✅ 篩選完成！獲得 {len(df_results)} 對「高純度正相關」組合。")