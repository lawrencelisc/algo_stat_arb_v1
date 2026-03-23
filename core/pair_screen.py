import os
import pandas as pd
import numpy as np
import statsmodels.api as sm

from pathlib import Path
from statsmodels.tsa.stattools import coint
from itertools import combinations
from datetime import datetime, timezone
from loguru import logger


class PairCombine:


    result_folder = Path(__file__).resolve().parent.parent / 'result'


    def __init__(self):
        logger.info('🛰️ PairCombine module initialized')

    def calculate_half_life(self, spread):
        """科學官修正版：計算均值回歸的半衰期 (Half-Life)"""
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
        except:
            return np.nan


    def pair_screener(self, coin_list, timeframe):
        """
        全自動共整合掃描與 CSV 數據庫記錄系統
        :param coin_list: 需要進行配對掃描的幣種清單 (例如 ['BTCUSDT', 'ETHUSDT'...])
        :param timeframe: 重新採樣的時間框架 (例如 '1h')
        """
        logger.info("🚀 Pair screener radar activating...")

        # --- 1. 定位目錄 ---
        current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
        root_dir = os.path.dirname(current_dir) if os.path.basename(current_dir) == 'core' else current_dir

        data_dir = os.path.join(root_dir, 'data/rawdata')

        # 🛡️ [SCO FIX] 善用 self.result_folder，自動建立目錄並設定 Log 路徑
        self.result_folder.mkdir(parents=True, exist_ok=True)
        log_filepath = self.result_folder / 'master_research_log.csv'

        logger.info(f"📂 Locked data directory: {data_dir}")

        if not os.path.exists(data_dir):
            logger.error(f"❌ Directory not found: {data_dir}. Please run MarketScan first!")
            return None

        files = [f for f in os.listdir(data_dir) if f.endswith('.parquet')]
        if not files:
            logger.error(f"❌ No Parquet files found in {data_dir}!")
            return None

        # --- 2. 讀取並過濾指定幣種 ---
        price_data = {}

        # 🛡️ 科學官防彈邏輯：將 Bybit 原生格式 'BTC/USDT:USDT' 轉換為檔名格式 'BTCUSDT'
        clean_coin_list = [c.split('/')[0] + "USDT" if '/' in c else c.upper() for c in coin_list]
        logger.info(f"🔍 Preparing to read data for following symbols: {clean_coin_list}")

        # 處理 timeframe 轉換 (確保 Pandas 看得懂 '1h', '60min' 等)
        pd_timeframe = f"{timeframe}min" if str(timeframe).isdigit() else timeframe
        pd_timeframe = pd_timeframe.lower().replace('m', 'min') if pd_timeframe.endswith('m') else pd_timeframe

        for file in files:
            symbol = file.split('_')[0]  # 取得檔名開頭，例如 BTCUSDT

            # 【關鍵】只處理 clean_coin_list 裡面有的幣種
            if symbol not in clean_coin_list:
                continue

            filepath = os.path.join(data_dir, file)
            df = pd.read_parquet(filepath)

            # 修復時間索引
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
                    except:
                        logger.warning(f"⚠️ No suitable time index found for {symbol}, skipping!")
                        continue

            df = df.sort_index()
            df.columns = [c.lower() for c in df.columns]

            # 聚合為目標 timeframe
            try:
                df_resampled = df.resample(pd_timeframe).agg({
                    'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'
                }).dropna()
                price_data[symbol] = df_resampled['c']  # Bybit parquet 的收盤價是 'c'
                logger.debug(f"📦 Loaded: {symbol} (Total {len(df_resampled)} k-lines)")
            except Exception as e:
                logger.error(f"⚠️ Error resampling {symbol} ({e}), possible time format mismatch.")
                continue

        df_prices = pd.DataFrame(price_data).dropna()
        symbols = df_prices.columns.tolist()
        data_points = len(df_prices)

        logger.info(f"✅ Data alignment complete! {len(symbols)} target coins, aligned data points: {data_points}.")

        if len(symbols) < 2:
            logger.error("❌ Successfully loaded less than 2 coins, cannot perform pairing!")
            return None

        logger.info(f"🔄 Starting calculation for {int(len(symbols) * (len(symbols) - 1) / 2)} pairs...")

        # --- 3. 核心配對計算 ---
        results = []
        scan_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        for sym1, sym2 in combinations(symbols, 2):
            y = df_prices[sym1]
            x = df_prices[sym2]

            correlation = y.corr(x)
            if correlation < 0.4:
                continue  # 相關性太低，放棄計算以節省資源

            # 共整合與迴歸計算
            score, p_value, _ = coint(y, x)
            x_with_const = sm.add_constant(x)
            ols_result = sm.OLS(y, x_with_const).fit()

            alpha = ols_result.params.iloc[0]  # 截距
            beta = ols_result.params.iloc[1]  # 斜率

            # 計算價差 Spread = Y - (Beta * X + Alpha)
            spread = y - (beta * x + alpha)

            half_life = self.calculate_half_life(spread)

            # 計算執行參考指標
            spread_mean = spread.mean()
            spread_std = spread.std()
            last_spread = spread.iloc[-1]
            last_z_score = (last_spread - spread_mean) / spread_std if spread_std != 0 else 0

            last_p1 = y.iloc[-1]
            last_p2 = x.iloc[-1]

            results.append({
                'timestamp': scan_time,
                'pair': f"{sym1}-{sym2}",
                's1': sym1,
                's2': sym2,
                'p_value': p_value,
                'correlation': correlation,
                'beta': beta,
                'alpha': alpha,
                'half_life': half_life if not np.isnan(half_life) else 9999,
                'last_z_score': last_z_score,
                'spread_std': spread_std,
                'last_p1': last_p1,
                'last_p2': last_p2,
                'data_points': data_points
            })

        if not results:
            logger.warning("⚠️ No pairs found meeting the initial criteria.")
            return None

        # --- 4. 數據整理與 CSV 存檔 ---
        df_results = pd.DataFrame(results)

        # 進行排名
        df_results = df_results.sort_values(by=['p_value']).reset_index(drop=True)
        df_results['rank'] = df_results.index + 1
        df_results['is_top_10'] = df_results['rank'] <= 10

        # 重排欄位順序以符合 Checklist 要求
        cols_order = [
            'timestamp', 'pair', 's1', 's2',
            'p_value', 'correlation', 'beta', 'alpha', 'half_life',
            'last_z_score', 'spread_std', 'last_p1', 'last_p2',
            'is_top_10', 'rank', 'data_points'
        ]
        df_results = df_results[cols_order]

        # 儲存到 master_research_log.csv (追加模式)
        file_exists = os.path.isfile(log_filepath)
        df_results.to_csv(log_filepath, mode='a', header=not file_exists, index=False)
        logger.success(f"💾 Scan results successfully written to database: {log_filepath}")

        # # --- 5. 輸出戰報 ---
        # print("\n🏆 ========== True Battlefield Overview Top 10 (Ranked by Cointegration Potential) ========== 🏆")
        # display_df = df_results.head(10)[['pair', 'correlation', 'p_value', 'half_life', 'beta', 'last_z_score']]
        # # 為了美觀，將數值四捨五入
        # display_df = display_df.round({'correlation': 4, 'p_value': 5, 'half_life': 2, 'beta': 4, 'last_z_score': 2})
        # print(display_df.to_string(index=False))
        #
        # best_p = df_results.iloc[0]['p_value']
        # if best_p > 0.1:
        #     print("\n⚠️ Warning: The P-Value of the top-ranked pair is > 0.1. The market shows strong divergence recently. Recommend observing without holding positions!")
        # else:
        #     print(f"\n💡 Top-ranked P-Value is {best_p:.5f}. Data saved and can be used as a reference for opening positions!")

        return df_results