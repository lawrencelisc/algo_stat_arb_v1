import time
import pandas as pd
from datetime import datetime, timedelta, timezone
import ccxt
from loguru import logger
from pathlib import Path

class MarketScanner:
    """
    [STAGE 1] Market Scanner Module
    Responsible for dynamically identifying top liquid pairs and fetching historical OHLCV data.
    """
    VERSION = "v3.0.1-Stable"

    def __init__(self):
        # Initialize Bybit exchange
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        logger.info(f'🛰️ MarketScanner {self.VERSION} deployed and ready')

    def get_top_volume_coins(self, num_coins=24, days_back=41, timeframe='1h'):
        """
        Automatically fetch the top N coins by liquidity and sync historical data.
        """
        try:
            # --- 1. Dynamic Liquidity Screening Top N ---
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
            # 獲取帶有斜線的原始 CCXT symbol
            raw_top_symbols = df_liquidity.sort_values(by='volume_24h', ascending=False).head(num_coins)['symbol'].tolist()

            # --- 2. Directory and Time Preparation ---
            root_dir = Path(__file__).resolve().parent.parent
            data_dir = root_dir / 'data' / 'rawdata'
            data_dir.mkdir(parents=True, exist_ok=True)

            date_str = datetime.now().strftime('%y%m%d')
            start_timestamp = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp() * 1000)
            bybit_interval = '60' if timeframe == '1h' else timeframe

            # --- 3. Start Fetching Data ---
            clean_top_symbols = [] # [修復] 準備一個乾淨的列表給後續模組使用

            for symbol in raw_top_symbols:
                # 將 BTC/USDT:USDT 轉換為 BTCUSDT
                clean_symbol = symbol.split('/')[0] + "USDT"
                clean_top_symbols.append(clean_symbol) # 加入乾淨列表

                params = {
                    'category': 'linear',
                    'symbol': clean_symbol,
                    'interval': bybit_interval,
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

                        df.set_index('ts', inplace=True)
                        df.sort_index(inplace=True)

                        file_name = f"{clean_symbol}_{timeframe}_{date_str}.parquet"
                        file_path = data_dir / file_name
                        df.to_parquet(str(file_path), engine='pyarrow')

                        logger.success(f"💾 Saved: {file_name} (Total {len(df)} records)")
                    else:
                        logger.warning(f"⚠️ No data returned for {clean_symbol}")

                    time.sleep(0.1)

                except Exception as e:
                    logger.error(f"❌ Failed to fetch {clean_symbol}: {e}")

            logger.info("✅ Task completed: Liquidity list and historical data are aligned.")

            # [修復] 回傳沒有斜線的乾淨名稱，這樣 PairCombine 才能正確找到 Parquet 檔案！
            return clean_top_symbols

        except Exception as e:
            logger.critical(f"🚨 System crashed: {e}")
            return []