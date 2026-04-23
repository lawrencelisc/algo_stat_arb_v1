import pandas as pd
import numpy as np
import ccxt
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

STOP_Z = 8.0   # z-score 超過此值視為共整合結構破裂，觸發止損（backtest 最優值）


class PairMonitor:
    """
    [v3.2.2-Safety] Pair Monitor Module
    Location: /core/pair_monitor.py
    Responsibility: Real-time Z-Score calculation and Cointegration health guarding.
    """
    VERSION = "v3.5.0-OptimizedThresholds"

    def __init__(self):
        # Path definitions
        self.root_dir = Path(__file__).resolve().parent.parent
        self.result_folder = self.root_dir / 'result'
        self.log_filepath = self.result_folder / 'master_research_log.csv'

        # Trade records and Signal paths
        self.trade_record_path = self.root_dir / 'data' / 'trade' / 'trade_record.csv'
        self.signal_folder = self.root_dir / 'data' / 'signal'
        self.signal_table_path = self.signal_folder / 'signal_table.csv'

        # Initialize Exchange (Bybit)
        self.exchange = ccxt.bybit({'enableRateLimit': True})
        self.signal_folder.mkdir(parents=True, exist_ok=True)

        logger.info(f"🛰️ PairMonitor {self.VERSION} Guardian mode online.")

    def get_active_trade_pairs(self):
        if not self.trade_record_path.exists():
            return []
        try:
            df = pd.read_csv(self.trade_record_path)
            if df.empty:
                return []
            active_pairs = df[df['status'] == 'OPEN']['pair'].unique().tolist()
            return active_pairs
        except Exception as e:
            logger.error(f"❌ Failed to read trade records: {e}")
            return []

    def fetch_latest_prices(self, symbols):
        try:
            # 只移除尾部 'USDT'，避免 replace() 誤刪 base 名稱中含 'USDT' 的幣種
            mapping = {f"{(s[:-4] if s.endswith('USDT') else s)}/USDT:USDT": s for s in symbols}
            ccxt_symbols = list(mapping.keys())

            tickers = self.exchange.fetch_tickers(ccxt_symbols, params={'category': 'linear'})

            prices = {}
            for ccxt_id, data in tickers.items():
                if ccxt_id in mapping:
                    csv_key = mapping[ccxt_id]
                    # last 可能為 None（新上市/停牌），float(None) 會拋 TypeError 導致整批失敗
                    last = data.get('last')
                    if last is not None:
                        prices[csv_key] = float(last)
                    else:
                        logger.warning(f"⚠️ No last price for {ccxt_id}, skipping.")
            return prices
        except Exception as e:
            logger.error(f"❌ Failed to fetch real-time prices: {e}")
            return {}

    def check_all_pairs(self):
        if not self.log_filepath.exists():
            logger.warning("⚠️ Master research log not found. Monitoring aborted.")
            return

        try:
            # master_research_log.csv 現在是覆寫模式，每次只含最新一批結果，直接讀取即可
            df_latest = pd.read_csv(self.log_filepath)
            if df_latest.empty: return

            active_pairs = self.get_active_trade_pairs()

            watchlist = df_latest[
                (df_latest['p_value'] < 0.03) |
                (df_latest['pair'].isin(active_pairs))
                ].copy()

            if watchlist.empty:
                logger.info("📡 Market is stable. No pairs to monitor.")
                return

            all_needed_symbols = list(set(watchlist['s1'].tolist() + watchlist['s2'].tolist()))
            current_prices = self.fetch_latest_prices(all_needed_symbols)

            signal_data = []

            for _, row in watchlist.iterrows():
                pair_name = row['pair']
                s1, s2 = row['s1'], row['s2']
                p_value = float(row['p_value'])
                beta = float(row['beta'])  # 提取 Beta 值

                # --- [SAFETY GUARD: SIGNAL_EXPIRED] ---
                if pair_name in active_pairs and p_value >= 0.03:
                    logger.critical(f"🚨 {pair_name} relationship broken (P={p_value:.3f})! Forcing exit signal.")
                    signal_data.append({
                        'pair': pair_name,
                        # nan 作哨兵值，明確區分 FORCE_EXIT 與正常 z_score=0 的均值狀態
                        'z_score': float('nan'),
                        'p_value': p_value,
                        'beta': beta,
                        'action': 'FORCE_EXIT_EXPIRED',
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                    continue

                # --- [STANDARD MONITORING] ---
                if s1 in current_prices and s2 in current_prices:
                    p1, p2 = current_prices[s1], current_prices[s2]
                    alpha, std = float(row['alpha']), float(row['spread_std'])

                    p1_log = np.log(p1)
                    p2_log = np.log(p2)
                    z_score = (p1_log - (beta * p2_log + alpha)) / std

                    # --- [SAFETY GUARD: STOP_LOSS] ---
                    # z-score 持續擴大超過 STOP_Z，代表 spread 方向性偏離，共整合結構可能已破裂
                    if pair_name in active_pairs and abs(z_score) >= STOP_Z:
                        logger.critical(
                            f"🛑 STOP_LOSS triggered for {pair_name}: z={z_score:.3f} ≥ {STOP_Z}. "
                            f"Cointegration structure may be broken!"
                        )
                        signal_data.append({
                            'pair': pair_name,
                            'z_score': round(z_score, 4),
                            'p_value': round(p_value, 4),
                            'beta': round(beta, 4),
                            'action': 'FORCE_EXIT_STOPLOSS',
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                        continue

                    signal_data.append({
                        'pair': pair_name,
                        'z_score': round(z_score, 4),
                        'p_value': round(p_value, 4),
                        'beta': round(beta, 4),
                        'action': 'MONITORING',
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })

            # 無論有無信號都覆寫：若 signal_data 為空，清空訊號表
            # 避免舊週期的 z_score / FORCE_EXIT 殘留，被 ExecutionManager 誤判為有效信號
            pd.DataFrame(signal_data).to_csv(self.signal_table_path, index=False)

        except Exception as e:
            logger.error(f"❌ Monitoring loop failed: {e}")

    def update_signal_table(self, pair, z_score, p_value, beta, action='MONITORING'):
        """手動更新單一配對的訊號，覆寫模式與 check_all_pairs 保持一致，避免舊訊號殘留。"""
        try:
            new_data = {
                'pair': [pair],
                'z_score': [z_score],
                'p_value': [p_value],
                'beta': [beta],
                'action': [action],
                'timestamp': [datetime.now(timezone.utc).isoformat()]
            }
            df = pd.DataFrame(new_data)
            # 改為覆寫，與 check_all_pairs 模式一致，防止追加模式堆積舊訊號
            df.to_csv(self.signal_table_path, mode='w', index=False)
        except Exception as e:
            logger.error(f"❌ Manual signal update failed: {e}")