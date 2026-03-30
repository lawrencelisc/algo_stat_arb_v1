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
    [STAGE 1] Pair Screener Module
    Location: /core/pair_screen.py
    Responsibility: Log-price transformation, Cointegration testing (EG Test),
                    calculating Beta, Half-life, and restoring dashboard fields.
    """
    VERSION = "v3.2.1-Safety"

    def __init__(self):
        # Path definitions
        self.root_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.root_dir / 'data' / 'rawdata'
        self.result_folder = self.root_dir / 'result'
        self.log_filepath = self.result_folder / 'master_research_log.csv'

        # Ensure result directory exists
        self.result_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f'🛰️ PairCombine {self.VERSION} initialized.')

    def calculate_half_life(self, spread):
        """
        Calculates the Half-Life of mean reversion for the spread.
        """
        spread = spread.dropna()
        if len(spread) <= 1:
            return 9999.0

        spread_lag = spread.shift(1)
        spread_ret = spread - spread_lag

        # Drop NaNs for regression
        valid_idx = spread_ret.index[1:]
        y = spread_ret.loc[valid_idx]
        x = sm.add_constant(spread_lag.loc[valid_idx])

        try:
            model = sm.OLS(y, x)
            res = model.fit()
            theta = res.params.iloc[1]
            if theta >= 0:
                return 9999.0  # Non-stationary or trending
            half_life = -np.log(2) / theta
            return half_life
        except Exception:
            return 9999.0

    def load_log_prices(self, symbol_list, timeframe='1h'):
        """
        Loads Parquet data and transforms to log-prices.
        """
        data_dict = {}
        for sym in symbol_list:
            # Search for corresponding Parquet files
            files = list(self.data_dir.glob(f"{sym}_{timeframe}_*.parquet"))
            if not files:
                continue

            # Read the latest data file
            latest_file = max(files, key=lambda x: x.stat().st_mtime)
            try:
                df = pd.read_parquet(latest_file)
                if df.empty: continue

                # Apply Log-Price Transformation
                data_dict[sym] = np.log(df['c'])
            except Exception as e:
                logger.error(f"❌ Failed to load {sym}: {e}")

        return pd.DataFrame(data_dict).dropna()

    def pair_screener(self, symbol_list, timeframe='1h', active_pairs=None):
        """
        Core logic: Iterates through combinations and forces calculation for active pairs.
        Restores dashboard fields: correlation, last_p1, last_p2, rank, is_top_10.
        """
        logger.info(f"🔍 Starting co-integration scan for {len(symbol_list)} symbols...")
        active_pairs = active_pairs or []

        # 1. Load log-price matrix
        price_matrix = self.load_log_prices(symbol_list, timeframe)
        if price_matrix.empty:
            logger.warning("⚠️ Price matrix is empty. No data to process.")
            return

        # 2. Generate all possible pair combinations
        all_combos = list(combinations(price_matrix.columns, 2))

        # Ensure active pairs are included even if not in the top list
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

                # --- [A. Cointegration Test (Engle-Granger)] ---
                score, p_value, _ = coint(y, x)

                # --- [B. Regression Parameters (Log-Scale)] ---
                x_with_const = sm.add_constant(x)
                model = sm.OLS(y, x_with_const).fit()
                beta = model.params.iloc[1]
                alpha = model.params.iloc[0]

                # --- [C. Spread & Z-Score Calculation] ---
                spread = y - (beta * x + alpha)
                spread_std = spread.std()
                last_z = spread.iloc[-1] / spread_std

                # --- [D. Dashboard Metric Restoration] ---
                correlation = y.corr(x)
                last_p1 = np.exp(y.iloc[-1])  # Restore from Log to Real Price
                last_p2 = np.exp(x.iloc[-1])

                # --- [E. Half-Life] ---
                half_life = self.calculate_half_life(spread)

                # Active Status Flag
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
                    'last_z_score': round(float(last_z), 4),  # This is 'current_z'
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

        # 3. Sort by P-Value and add Ranking logic
        df_results = pd.DataFrame(results).sort_values(by='p_value').reset_index(drop=True)
        df_results['rank'] = df_results.index + 1
        df_results['is_top_10'] = df_results['rank'] <= 10

        # Save to Research Log (Append mode)
        file_exists = self.log_filepath.exists()
        df_results.to_csv(self.log_filepath, mode='a', index=False, header=not file_exists)

        logger.success(f"✅ Co-integration scan completed. Logged {len(df_results)} pairs (with full dashboard fields).")