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
    [STAGE 2] Pair Screener Module v2.4.0-Stable
    Responsible for calculating Cointegration, Correlation, Beta, Alpha, and Half-Life
    for all possible combinations of the provided coin list.
    """
    VERSION = "v2.4.0-Stable"

    def __init__(self):
        # Robust path resolution
        self.root_dir = Path(__file__).resolve().parent.parent
        self.data_dir = self.root_dir / 'data' / 'rawdata'
        self.result_folder = self.root_dir / 'result'

        logger.info(f'🛰️ PairCombine {self.VERSION} module initialized.')

    def calculate_half_life(self, spread):
        """Calculates the Half-Life of mean reversion using the Ornstein-Uhlenbeck process."""
        spread = spread.dropna()
        if len(spread) <= 1:
            return np.nan

        spread_lag = spread.shift(1)
        spread_ret = spread - spread_lag

        spread_ret = spread_ret.dropna()
        spread_lag = spread_lag.dropna()

        spread_lag_with_const = sm.add_constant(spread_lag)

        try:
            model = sm.OLS(spread_ret, spread_lag_with_const)
            res = model.fit()
            theta = res.params.iloc[1]

            if theta >= 0:
                return np.nan

            half_life = -np.log(2) / theta
            return half_life
        except Exception:
            return np.nan

    def pair_screener(self, coin_list, timeframe='1h'):
        """
        Fully automated cointegration scan and database logging system.
        :param coin_list: List of coins to scan (e.g. ['BTCUSDT', 'ETHUSDT'...])
        :param timeframe: Resampling timeframe (e.g. '1h')
        """
        logger.info(f"🚀 PairCombine {self.VERSION} radar activating...")

        # --- 1. Directory & Environment Check ---
        self.result_folder.mkdir(parents=True, exist_ok=True)
        log_filepath = self.result_folder / 'master_research_log.csv'

        logger.info(f"📂 Locked data directory: {self.data_dir}")

        if not self.data_dir.exists():
            logger.error(f"❌ Directory not found: {self.data_dir}. Please run MarketScan first!")
            return None

        files = [f for f in self.data_dir.iterdir() if f.name.endswith('.parquet')]
        if not files:
            logger.error(f"❌ No Parquet files found in {self.data_dir}!")
            return None

        # --- 2. Read and Filter Target Coins ---
        price_data = {}

        # Clean symbol names to match file prefixes
        clean_coin_list = [c.split('/')[0] + "USDT" if '/' in c else c.upper() for c in coin_list]
        logger.info(f"🔍 Preparing to read data for following symbols: {clean_coin_list}")

        # Format timeframe for Pandas
        pd_timeframe = f"{timeframe}min" if str(timeframe).isdigit() else timeframe
        pd_timeframe = pd_timeframe.lower().replace('m', 'min') if pd_timeframe.endswith('m') else pd_timeframe

        for file_path in files:
            symbol = file_path.name.split('_')[0]

            if symbol not in clean_coin_list:
                continue

            try:
                df = pd.read_parquet(file_path)

                # Fix datetime index issues
                if not isinstance(df.index, pd.DatetimeIndex):
                    time_col = next((col for col in ['timestamp', 'time', 'date', 'ts'] if col in df.columns), None)
                    if time_col:
                        if pd.api.types.is_numeric_dtype(df[time_col]):
                            if df[time_col].max() > 1e11:
                                df[time_col] = pd.to_datetime(df[time_col], unit='ms')
                            else:
                                df[time_col] = pd.to_datetime(df[time_col], unit='s')
                        else:
                            df[time_col] = pd.to_datetime(df[time_col])
                        df.set_index(time_col, inplace=True)
                    else:
                        try:
                            df.index = pd.to_datetime(df.index)
                        except Exception:
                            logger.warning(f"⚠️ No suitable time index found for {symbol}, skipping!")
                            continue

                df = df.sort_index()
                df.columns = [c.lower() for c in df.columns]

                # Resample to target timeframe
                df_resampled = df.resample(pd_timeframe).agg({
                    'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'
                }).dropna()

                price_data[symbol] = df_resampled['c']
                logger.debug(f"📦 Loaded: {symbol} (Total {len(df_resampled)} k-lines)")

            except Exception as e:
                logger.error(f"⚠️ Error processing {symbol}: {e}")
                continue

        df_prices = pd.DataFrame(price_data).dropna()
        symbols = df_prices.columns.tolist()
        data_points = len(df_prices)

        logger.info(f"✅ Data alignment complete! {len(symbols)} target coins, aligned data points: {data_points}.")

        if len(symbols) < 2:
            logger.error("❌ Successfully loaded less than 2 coins, cannot perform pairing!")
            return None

        total_pairs = int(len(symbols) * (len(symbols) - 1) / 2)
        logger.info(f"🔄 Starting calculation for {total_pairs} pairs...")

        # --- 3. Core Cointegration Calculation ---
        results = []
        scan_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        for sym1, sym2 in combinations(symbols, 2):
            y = df_prices[sym1]
            x = df_prices[sym2]

            correlation = y.corr(x)
            # Skip low correlation pairs to save CPU resources
            if correlation < 0.4:
                continue

            # Cointegration and OLS Regression
            try:
                score, p_value, _ = coint(y, x)
                x_with_const = sm.add_constant(x)
                ols_result = sm.OLS(y, x_with_const).fit()

                alpha = float(ols_result.params.iloc[0])
                beta = float(ols_result.params.iloc[1])

                # Calculate Spread = Y - (Beta * X + Alpha)
                spread = y - (beta * x + alpha)
                half_life = self.calculate_half_life(spread)

                spread_mean = spread.mean()
                spread_std = spread.std()
                last_spread = spread.iloc[-1]
                last_z_score = (last_spread - spread_mean) / spread_std if spread_std != 0 else 0

                results.append({
                    'timestamp': scan_time,
                    'pair': f"{sym1}-{sym2}",
                    's1': sym1,
                    's2': sym2,
                    'p_value': float(p_value),
                    'correlation': float(correlation),
                    'beta': beta,
                    'alpha': alpha,
                    'half_life': float(half_life) if not np.isnan(half_life) else 9999.0,
                    'last_z_score': float(last_z_score),
                    'spread_std': float(spread_std),
                    'last_p1': float(y.iloc[-1]),
                    'last_p2': float(x.iloc[-1]),
                    'data_points': data_points
                })
            except Exception as e:
                logger.debug(f"⚠️ Math error during pair {sym1}-{sym2} calculation: {e}")
                continue

        if not results:
            logger.warning("⚠️ No pairs found meeting the initial statistical criteria.")
            return None

        # --- 4. Data Formatting & CSV Export ---
        df_results = pd.DataFrame(results)

        # Ranking by P-Value
        df_results = df_results.sort_values(by=['p_value']).reset_index(drop=True)
        df_results['rank'] = df_results.index + 1
        df_results['is_top_10'] = df_results['rank'] <= 10

        cols_order = [
            'timestamp', 'pair', 's1', 's2',
            'p_value', 'correlation', 'beta', 'alpha', 'half_life',
            'last_z_score', 'spread_std', 'last_p1', 'last_p2',
            'is_top_10', 'rank', 'data_points'
        ]
        df_results = df_results[cols_order]

        # Append to master_research_log.csv safely
        file_exists = log_filepath.exists()
        df_results.to_csv(log_filepath, mode='a', header=not file_exists, index=False)
        logger.success(f"💾 Scan results successfully written to database: {log_filepath.name}")

        return df_results