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
    VERSION = "v3.1.0-FixedScreening"

    # Bybit V5 kline API interval 代碼映射（與 CCXT 標準不同，必須轉換）
    BYBIT_INTERVAL_MAP = {
        '1m': '1',  '3m': '3',  '5m': '5',  '15m': '15', '30m': '30',
        '1h': '60', '2h': '120','4h': '240', '6h': '360', '12h': '720',
        '1d': 'D',  '1w': 'W',  '1M': 'M',
    }

    def __init__(self):
        # Initialize Bybit exchange
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        logger.info(f'🛰️ MarketScanner {self.VERSION} deployed and ready')

    def _fetch_and_save(self, clean_symbol, timeframe, bybit_interval, start_timestamp, data_dir, date_str):
        """下載單一 symbol 的 OHLCV 並存為 parquet，失敗時記錄 error 但不中斷。"""
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
                df.to_parquet(str(data_dir / file_name), engine='pyarrow')
                logger.success(f"💾 Saved: {file_name} (Total {len(df)} records)")
            else:
                logger.warning(f"⚠️ No data returned for {clean_symbol}")
        except Exception as e:
            logger.error(f"❌ Failed to fetch {clean_symbol}: {e}")
        finally:
            # 無論成功或 exception，都等待，避免連環失敗打爆 rate limit
            time.sleep(0.1)

    def get_top_volume_coins(self, num_coins=24, days_back=41, timeframe='1h', force_include=None):
        """
        Fetch top N coins by liquidity and sync historical data.
        force_include: 額外強制下載的 symbol 列表（如持倉幣種），即使不在 Top N 也確保數據是最新的。
        """
        force_include = force_include or []
        try:
            # --- 1. Dynamic Liquidity Screening Top N ---
            logger.info(f"🔍 Scanning top {num_coins} perpetual contracts by volume on Bybit...")
            # 指定 category=linear，只拉 USDT 永續合約，避免傳輸現貨/反向合約的冗餘資料
            tickers = self.exchange.fetch_tickers(params={'category': 'linear'})

            usdt_perp_list = []
            for symbol, data in tickers.items():
                if '/USDT:USDT' in symbol:
                    usdt_perp_list.append({
                        'symbol': symbol,
                        # 用 `or 0` 同時處理 key 不存在和值為 None 的情況
                        'volume_24h': data.get('quoteVolume') or 0
                    })

            if not usdt_perp_list:
                logger.critical("🚨 No USDT perpetual tickers found. Check exchange connection.")
                return []

            df_liquidity = pd.DataFrame(usdt_perp_list)
            raw_top_symbols = df_liquidity.sort_values(by='volume_24h', ascending=False).head(num_coins)['symbol'].tolist()

            # --- 2. Directory and Time Preparation ---
            root_dir = Path(__file__).resolve().parent.parent
            data_dir = root_dir / 'data' / 'rawdata'
            data_dir.mkdir(parents=True, exist_ok=True)

            now_utc = datetime.now(timezone.utc)
            # 統一使用 UTC，避免本地時區在午夜造成 date_str 與 start_timestamp 跨日不一致
            date_str = now_utc.strftime('%y%m%d')
            start_timestamp = int((now_utc - timedelta(days=days_back)).timestamp() * 1000)
            # 使用完整映射表，支援所有常見 timeframe（Bybit V5 interval 代碼與 CCXT 不同）
            bybit_interval = self.BYBIT_INTERVAL_MAP.get(timeframe)
            if bybit_interval is None:
                logger.critical(f"🚨 Unsupported timeframe '{timeframe}'. Supported: {list(self.BYBIT_INTERVAL_MAP.keys())}")
                return []

            # --- 3. Fetch Top N symbols ---
            clean_top_symbols = []
            for symbol in raw_top_symbols:
                clean_symbol = symbol.split('/')[0] + "USDT"
                clean_top_symbols.append(clean_symbol)
                self._fetch_and_save(clean_symbol, timeframe, bybit_interval, start_timestamp, data_dir, date_str)

            # --- 4. Guardian: 強制刷新持倉幣種，防止數據過期 ---
            guardian_symbols = [s for s in force_include if s not in clean_top_symbols]
            if guardian_symbols:
                logger.info(f"🛡️ Guardian refresh: downloading {len(guardian_symbols)} active-position symbols not in Top {num_coins}.")
                for sym in guardian_symbols:
                    self._fetch_and_save(sym, timeframe, bybit_interval, start_timestamp, data_dir, date_str)

            logger.info("✅ Task completed: Liquidity list and historical data are aligned.")
            return clean_top_symbols

        except Exception as e:
            logger.critical(f"🚨 System crashed: {e}")
            return []