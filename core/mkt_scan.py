import os
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
import ccxt
from loguru import logger


class MarketScanner:
    def __init__(self):
        # 初始化 Bybit 交易所
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        logger.info('🛰️ MarketScanner system is deployed and ready')

    def get_top_volume_coins(self, num_coins=24, days_back=41, timeframe='1h'):
        """
        自動抓取流動性前 N 名幣種，並同步歷史數據。
        num_coins: 抓取幣種數量
        days_back: 回溯天數 (建議 41 天)
        timeframe: 時間單位 (固定為 '1h')
        """
        try:
            # --- 1. 動態篩選流動性 Top N ---
            logger.info(f"🔍 Scanning top {num_coins} perpetual contracts by volume on Bybit...")
            tickers = self.exchange.fetch_tickers()

            usdt_perp_list = []
            for symbol, data in tickers.items():
                if '/USDT:USDT' in symbol:
                    usdt_perp_list.append({
                        'symbol': symbol,
                        'volume_24h': data.get('quoteVolume', 0)
                    })

            df_liquidity = pd.DataFrame(usdt_perp_list)
            top_symbols = df_liquidity.sort_values(by='volume_24h', ascending=False).head(num_coins)['symbol'].tolist()

            # --- 2. 準備目錄與時間 ---
            current_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in locals() else os.getcwd()
            data_dir = os.path.join(current_dir, 'data/rawdata')
            os.makedirs(data_dir, exist_ok=True)
            date_str = datetime.now().strftime('%y%m%d')

            # 計算 start_time (毫秒格式)
            start_timestamp = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp() * 1000)

            # 🛡️ [SCO FIX] 極簡轉換：確定是 '1h' 就給 Bybit '60'
            bybit_interval = '60' if timeframe == '1h' else timeframe

            # --- 3. 開始抓取 ---
            for symbol in top_symbols:
                clean_symbol = symbol.split('/')[0] + "USDT"

                params = {
                    'category': 'linear',
                    'symbol': clean_symbol,
                    'interval': bybit_interval,  # 傳入 60
                    'start': start_timestamp,
                    'limit': 1000,
                }

                logger.info(f"📡 Requesting data for {clean_symbol}: interval={bybit_interval} ...")

                try:
                    response = self.exchange.publicGetV5MarketKline(params)

                    if response and 'list' in response['result']:
                        ohlcv_list = response['result']['list']

                        df = pd.DataFrame(ohlcv_list, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'turnover'])

                        df['ts'] = pd.to_datetime(df['ts'].astype(float), unit='ms')
                        for col in ['o', 'h', 'l', 'c', 'v']:
                            df[col] = df[col].astype(float)

                        # 修正 Pandas index 操作
                        df.set_index('ts', inplace=True)
                        df.sort_index(inplace=True)

                        # 🛡️ [SCO FIX] 存檔拿走 m 字：BTCUSDT_1h_260321.parquet
                        file_name = f"{clean_symbol}_{timeframe}_{date_str}.parquet"
                        file_path = os.path.join(data_dir, file_name)
                        df.to_parquet(file_path, engine='pyarrow')

                        logger.success(f"💾 Saved: {file_name} (Total {len(df)} records)")

                    else:
                        logger.warning(f"⚠️ No data returned for {clean_symbol}")

                    time.sleep(0.1)  # 避開 Rate Limit

                except Exception as e:
                    logger.error(f"❌ Failed to fetch {clean_symbol}: {e}")

            logger.info("✅ Task completed: Liquidity list and historical data are aligned.")
            return top_symbols

        except Exception as e:
            logger.critical(f"🚨 System crashed: {e}")
            return []