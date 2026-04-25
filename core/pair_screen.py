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
    VERSION = "v4.2.0-RobustScreening"

    def __init__(self):
        self.root_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.root_dir / 'data' / 'rawdata'
        self.result_folder = self.root_dir / 'result'
        # 每次覆寫，供 PairMonitor 實時讀取（不會膨脹）
        self.log_filepath = self.result_folder / 'master_research_log.csv'
        # 追加記錄，供事後研究分析用
        self.history_filepath = self.result_folder / 'research_history.csv'
        self.result_folder.mkdir(parents=True, exist_ok=True)

    def calculate_half_life(self, spread):
        spread = spread.dropna()
        # 需至少 3 點：shift 後有效觀測 = len-1，OLS 需 >= 2 點（2 個參數）
        if len(spread) <= 2: return 9999.0
        spread_lag = spread.shift(1)
        spread_ret = spread - spread_lag
        valid_idx = spread_ret.index[1:]
        y = spread_ret.loc[valid_idx]
        x = sm.add_constant(spread_lag.loc[valid_idx])
        try:
            res = sm.OLS(y, x).fit()
            theta = res.params.iloc[1]
            return -np.log(2) / theta if theta < 0 else 9999.0
        except Exception:
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
                close = df.sort_index()['c']
                # 過濾非正值，防止 log(0)=-inf 或 log(負)=nan 靜默污染後續計算
                close = close[close > 0]
                log_prices = np.log(close).replace([np.inf, -np.inf], np.nan)
                series_dict[sym] = log_prices
            except Exception as e:
                logger.error(f"Failed to load {sym}: {e}")

        if not series_dict: return pd.DataFrame()
        # ffill(limit=1) 只填補單根孤立缺值，避免大量插補扭曲共整合檢定
        return pd.concat(series_dict, axis=1).sort_index().ffill(limit=1)

    def pair_screener(self, symbol_list, timeframe='1h', active_pairs=None):
        logger.info(f"🧠 系統重置：開始嚴格共整合篩選 ({len(symbol_list)} symbols)")
        active_pairs = active_pairs or []
        price_matrix = self.load_log_prices(symbol_list, timeframe)
        if price_matrix.empty: return pd.DataFrame()

        all_combos = list(combinations(price_matrix.columns, 2))
        for ap in active_pairs:
            try:
                s1, s2 = ap.split('-')
                if s1 in price_matrix.columns and s2 in price_matrix.columns \
                        and (s1, s2) not in all_combos and (s2, s1) not in all_combos:
                    all_combos.append((s1, s2))
            except Exception:
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

                # 🛡️ 鐵血防線 1：用 log returns 計算相關性，避免 level 序列的偽相關
                # NaN 守衛：標準差為 0 時 corr() 回傳 NaN，NaN < 0.65 == False 會意外通過
                correlation = y.diff().corr(x.diff())
                if (pd.isna(correlation) or correlation < 0.60) and not is_active: continue

                x_with_const = sm.add_constant(x)
                model = sm.OLS(y, x_with_const).fit()
                beta = model.params.iloc[1]

                # 🛡️ 鐵血防線 2：Beta 必須大於 0
                if beta <= 0 and not is_active: continue

                # 🛡️ 鐵血防線 3：P-Value 必須過關 (強共整合)
                # 只測 coint(y, x)，與上方 OLS(y ~ x) 方向一致
                # 若改用 min(p_yx, p_xy)，可能用 x~y 方向通過篩選，
                # 但 spread 仍按 y~x 方向計算，導致 spread 未必平穩
                _, p_value, _ = coint(y, x)
                if p_value >= 0.05 and not is_active: continue

                alpha = model.params.iloc[0]
                spread = y - (beta * x + alpha)
                spread_std = spread.std()
                # Z-Score 正規化
                last_z = (spread.iloc[-1] - spread.mean()) / spread_std if spread_std != 0 else 0

                half_life = self.calculate_half_life(spread)

                # 🛡️ 鐵血防線 4：當前 Z-Score 絕對值 > 3.5 代表 spread 極度偏離，
                # 共整合結構可能已破裂，非活躍持倉一律過濾
                if abs(last_z) > 3.5 and not is_active: continue

                results.append({
                    'timestamp': scan_time, 'pair': pair_name, 's1': s1, 's2': s2,
                    'p_value': round(float(p_value), 5), 'correlation': round(float(correlation), 4),
                    'beta': round(float(beta), 4), 'alpha': round(float(alpha), 4),
                    'spread_std': round(float(spread_std), 6), 'last_z_score': round(float(last_z), 4),
                    'half_life': round(half_life, 2),
                    'last_p1': round(np.exp(y.iloc[-1]), 6), 'last_p2': round(np.exp(x.iloc[-1]), 6),
                    'data_points': len(y), 'is_active': is_active
                })
            except Exception:
                continue

        if not results:
            logger.warning("⚠️ 篩選完成，但無任何配對通過所有防線。")
            return pd.DataFrame()

        df_results = pd.DataFrame(results).sort_values(by='p_value').reset_index(drop=True)
        df_results['rank'] = df_results.index + 1
        df_results['is_shortlisted'] = df_results['rank'] <= 20

        # 覆寫最新結果（PairMonitor 直接讀此檔，無需 timestamp 過濾，不會無限膨脹）
        df_results.to_csv(self.log_filepath, mode='w', index=False)
        # 追加至 history 供研究分析，與實時監控分離
        # 用 stat().st_size > 0 確保即使文件存在但為空，也能正確補上 header
        need_header = not (self.history_filepath.exists() and self.history_filepath.stat().st_size > 0)
        df_results.to_csv(self.history_filepath, mode='a', index=False, header=need_header)

        logger.success(f"✅ 篩選完成！獲得 {len(df_results)} 對「高純度正相關」組合。")
        return df_results