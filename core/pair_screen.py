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
    [STAGE 2] Pair Screener Module
    Location: /core/pair_screen.py
    Responsibility: Log-price transformation, Cointegration testing (EG Test),
                    calculating Beta, Half-life, and restoring dashboard fields.
    Optimized: Handles index alignment to prevent "Empty Matrix" errors.
    """
    VERSION = "v3.2.3-Safety"

    def __init__(self):
        # 路徑定義
        self.root_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.root_dir / 'data' / 'rawdata'
        self.result_folder = self.root_dir / 'result'
        self.log_filepath = self.result_folder / 'master_research_log.csv'

        # 確保結果目錄存在
        self.result_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f'🛰️ PairCombine {self.VERSION} initialized.')

    def calculate_half_life(self, spread):
        """
        計算殘差回歸的半衰期 (Mean Reversion Speed)
        """
        spread = spread.dropna()
        if len(spread) <= 1:
            return 9999.0

        spread_lag = spread.shift(1)
        spread_ret = spread - spread_lag

        # 移除空值以進行回歸
        valid_idx = spread_ret.index[1:]
        y = spread_ret.loc[valid_idx]
        x = sm.add_constant(spread_lag.loc[valid_idx])

        try:
            model = sm.OLS(y, x)
            res = model.fit()
            theta = res.params.iloc[1]
            if theta >= 0:
                return 9999.0  # 不具備回歸特性
            half_life = -np.log(2) / theta
            return half_life
        except Exception:
            return 9999.0

    def load_log_prices(self, symbol_list, timeframe='1h'):
        """
        載入 Parquet 並使用 concat 確保對齊，防止不同幣種時間點不一時產生全空問題。
        """
        series_dict = {}
        for sym in symbol_list:
            # 搜尋對應的 Parquet 檔案
            files = list(self.data_dir.glob(f"{sym}_{timeframe}_*.parquet"))
            if not files:
                continue

            # 讀取最新的數據檔案
            latest_file = max(files, key=lambda x: x.stat().st_mtime)
            try:
                df = pd.read_parquet(latest_file)
                if df.empty: continue

                # 確保 index 是時間格式以便對齊
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index)

                # [核心] 應用對數價格轉化 (Log-Price)
                series_dict[sym] = np.log(df['c'])
            except Exception as e:
                logger.error(f"❌ Failed to load {sym}: {e}")

        if not series_dict:
            return pd.DataFrame()

        # [修復邏輯] 使用 concat axis=1 進行 index 對齊
        # 使用 ffill 容許微小的數據缺失 (最多 1 根 K 線)，確保矩陣不會因為極少數缺失而變空
        final_df = pd.concat(series_dict, axis=1).sort_index()
        final_df = final_df.ffill(limit=1).dropna()

        if final_df.empty:
            logger.warning("⚠️ Data alignment failed: No common time overlap found.")

        return final_df

    def pair_screener(self, symbol_list, timeframe='1h', active_pairs=None):
        """
        核心篩選邏輯：遍歷所有組合並強制計算持倉配對
        """
        logger.info(f"🔍 Starting co-integration scan for {len(symbol_list)} symbols...")
        active_pairs = active_pairs or []

        # 1. 載入並對齊對數價格矩陣
        price_matrix = self.load_log_prices(symbol_list, timeframe)
        if price_matrix.empty:
            logger.warning("⚠️ Price matrix is empty. Research log will not be updated.")
            return

        # 2. 生成所有可能的配對組合
        all_combos = list(combinations(price_matrix.columns, 2))

        # 確保持倉中的配對即便不在 Top List 也會被加入掃描
        for ap in active_pairs:
            try:
                s1, s2 = ap.split('-')
                if s1 in price_matrix.columns and s2 in price_matrix.columns:
                    if (s1, s2) not in all_combos and (s2, s1) not in all_combos:
                        all_combos.append((s1, s2))
            except Exception:
                continue

        results = []
        scan_time = datetime.now(timezone.utc).isoformat()

        for s1, s2 in all_combos:
            try:
                y = price_matrix[s1]
                x = price_matrix[s2]
                pair_name = f"{s1}-{s2}"

                # --- [A. 共整合測試 (Engle-Granger)] ---
                score, p_value, _ = coint(y, x)

                # --- [B. 回歸參數計算 (Log-Scale)] ---
                x_with_const = sm.add_constant(x)
                model = sm.OLS(y, x_with_const).fit()
                beta = model.params.iloc[1]
                alpha = model.params.iloc[0]

                # --- [C. 殘差與 Z-Score 計算] ---
                spread = y - (beta * x + alpha)
                spread_std = spread.std()
                last_z = spread.iloc[-1] / spread_std

                # --- [D. 附加指標恢復 (Dashboard 所需)] ---
                correlation = y.corr(x)
                last_p1 = np.exp(y.iloc[-1])  # 從對數還原成真實價格
                last_p2 = np.exp(x.iloc[-1])

                # --- [E. 半衰期] ---
                half_life = self.calculate_half_life(spread)

                # 標註是否為當前持倉
                is_active = pair_name in active_pairs

                results.append({
                    'timestamp': scan_time,
                    'pair': pair_name,
                    's1': s1, 's2': s2,
                    'p_value': round(float(p_value), 5),
                    'correlation': round(float(correlation), 4),
                    'beta': round(float(beta), 4),
                    'alpha': round(float(alpha), 4),
                    'spread_std': round(float(spread_std), 6),
                    'last_z_score': round(float(last_z), 4),  # current_z
                    'half_life': round(float(half_life), 2),
                    'last_p1': round(float(last_p1), 6),
                    'last_p2': round(float(last_p2), 6),
                    'data_points': len(y),
                    'is_active': is_active
                })

            except Exception:
                continue

        if not results:
            logger.warning("📡 No valid pairs found in this scan.")
            return

        # 3. 排序並增加排名與 Top 10 標註
        df_results = pd.DataFrame(results).sort_values(by='p_value').reset_index(drop=True)
        df_results['rank'] = df_results.index + 1
        df_results['is_top_10'] = df_results['rank'] <= 10

        # 採用 Append 模式紀錄，保留歷史軌跡
        file_exists = self.log_filepath.exists()
        df_results.to_csv(self.log_filepath, mode='a', index=False, header=not file_exists)

        logger.success(f"✅ Co-integration scan completed. Logged {len(df_results)} pairs.")