import time
import pandas as pd
from datetime import datetime, timedelta, timezone
import ccxt
from loguru import logger
from pathlib import Path


class MarketScanner:
    """
    [STAGE 1] Market Scanner Module v2.4.0-Stable
    Responsible for dynamically identifying top liquid pairs and fetching historical OHLCV data.
    """
    VERSION = "v3.0.0-Stable"

    def __init__(self):
        # Initialize Bybit exchange
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        logger.info(f'🛰️ MarketScanner {self.VERSION} deployed and ready')

    def get_top_volume_coins(self, num_coins=24, days_back=41, timeframe='1h'):
        """
        Automatically fetch the top N coins by liquidity and sync historical data.
        num_coins: Number of coins to fetch
        days_back: Lookback period (41 days recommended for 1h coint test)
        timeframe: Timeframe (fixed at '1h')
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
            # Sort by 24h volume and extract top symbols
            top_symbols = df_liquidity.sort_values(by='volume_24h', ascending=False).head(num_coins)['symbol'].tolist()

            # --- 2. Directory and Time Preparation ---
            # Use Pathlib to ensure robust directory targeting across all modules
            root_dir = Path(__file__).resolve().parent.parent
            data_dir = root_dir / 'data' / 'rawdata'
            data_dir.mkdir(parents=True, exist_ok=True)

            date_str = datetime.now().strftime('%y%m%d')

            # Calculate start_time (ms format for Bybit V5)
            start_timestamp = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp() * 1000)

            # Ensure interval is passed correctly to Bybit ('60' instead of '1h')
            bybit_interval = '60' if timeframe == '1h' else timeframe

            # --- 3. Start Fetching Data ---
            for symbol in top_symbols:
                clean_symbol = symbol.split('/')[0] + "USDT"

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

                        # Convert to DataFrame
                        df = pd.DataFrame(ohlcv_list, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'turnover'])

                        # Format Timestamp
                        df['ts'] = pd.to_datetime(df['ts'].astype(float), unit='ms')
                        for col in ['o', 'h', 'l', 'c', 'v']:
                            df[col] = df[col].astype(float)

                        # Fix Pandas index operations
                        df.set_index('ts', inplace=True)
                        df.sort_index(inplace=True)

                        # Save to Parquet format (efficient and fast)
                        file_name = f"{clean_symbol}_{timeframe}_{date_str}.parquet"
                        file_path = data_dir / file_name
                        df.to_parquet(str(file_path), engine='pyarrow')

                        logger.success(f"💾 Saved: {file_name} (Total {len(df)} records)")

                    else:
                        logger.warning(f"⚠️ No data returned for {clean_symbol}")

                    time.sleep(0.1)  # Avoid hitting Rate Limit

                except Exception as e:
                    logger.error(f"❌ Failed to fetch {clean_symbol}: {e}")

            logger.info("✅ Task completed: Liquidity list and historical data are aligned.")

            # CRITICAL: Return the list of top coins to be used by the Screener module
            return top_symbols

        except Exception as e:
            logger.critical(f"🚨 System crashed: {e}")
            return []