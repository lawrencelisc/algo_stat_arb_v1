import pandas as pd
import ccxt
import yaml
import os
import numpy as np
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone


class ExecutionManager:
    """
    [STAGE 3] Execution Manager Module
    Location: /utils/execution.py
    Responsibility: Read signals from signal_table.csv and execute dual-leg trades on Bybit.
    """
    VERSION = "v3.1.6-Standalone-Guardian"

    def __init__(self, budget_per_pair=1500.0):
        self.root_dir = Path(__file__).resolve().parent.parent
        self.signal_table_path = self.root_dir / 'data' / 'signal' / 'signal_table.csv'
        self.trade_record_path = self.root_dir / 'data' / 'trade' / 'trade_record.csv'
        self.budget = budget_per_pair

        try:
            config_path = self.root_dir / 'config' / 'config.yaml'
            if not config_path.exists():
                config_path = self.root_dir / 'config.yaml'

            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            api_key = None
            api_secret = None

            if 'algo_pair_trade' in config:
                api_key = config['algo_pair_trade'].get('PT_API_KEY')
                api_secret = config['algo_pair_trade'].get('PT_SECRET_KEY')

            if not api_key and 'ACCOUNTS' in config:
                for acc_name, acc_data in config['ACCOUNTS'].items():
                    api_key = acc_data.get('key')
                    api_secret = acc_data.get('secret')
                    if api_key: break

            if not api_key or not api_secret:
                raise ValueError("API Keys not found in config.yaml")

            self.exchange = ccxt.bybit({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': {'defaultType': 'linear'}
            })
            logger.info(f"✅ ExecutionManager {self.VERSION} successfully connected to Bybit.")
        except Exception as e:
            logger.error(f"❌ ExecutionManager init failed: {e}")
            raise

        self.trade_record_path.parent.mkdir(parents=True, exist_ok=True)

    def _to_ccxt_symbol(self, symbol):
        """翻譯機：將 BTCUSDT 轉換為 CCXT 標準的 BTC/USDT:USDT"""
        base = symbol.replace('USDT', '')
        return f"{base}/USDT:USDT"

    def get_open_positions(self):
        if not self.trade_record_path.exists():
            return pd.DataFrame()
        try:
            df = pd.read_csv(self.trade_record_path)
            if df.empty: return pd.DataFrame()
            return df[df['status'] == 'OPEN']
        except:
            return pd.DataFrame()

    def execute_trades(self):
        """Called every 5 minutes by main_entry.py"""
        if not self.signal_table_path.exists():
            return

        try:
            signals = pd.read_csv(self.signal_table_path)
            open_positions = self.get_open_positions()
            active_pairs = open_positions['pair'].tolist() if not open_positions.empty else []

            for _, sig in signals.iterrows():
                pair = sig['pair']
                z = float(sig['z_score'])
                action = sig.get('action', 'MONITORING')

                if action == 'FORCE_EXIT_EXPIRED' and pair in active_pairs:
                    self._close_pair_position(pair, "SIGNAL_EXPIRED")
                    continue

                if pair in active_pairs:
                    if abs(z) < 0.2:
                        self._close_pair_position(pair, "Z_REVERSION")
                    continue

                if pair not in active_pairs and action == 'MONITORING':
                    if z > 2.0:
                        self._open_pair_position(pair, sig, side='SHORT_SPREAD')
                    elif z < -2.0:
                        self._open_pair_position(pair, sig, side='LONG_SPREAD')

        except Exception as e:
            logger.error(f"❌ Execution loop error: {e}")

    def _open_pair_position(self, pair, sig, side):
        s1, s2 = sig['pair'].split('-')
        beta = float(sig['beta'])

        s1_ccxt = self._to_ccxt_symbol(s1)
        s2_ccxt = self._to_ccxt_symbol(s2)

        try:
            prices = self.exchange.fetch_tickers([s1_ccxt, s2_ccxt])
            p1, p2 = prices[s1_ccxt]['last'], prices[s2_ccxt]['last']

            # [修復] 強制使用絕對值，保證數量永遠為正
            qty1 = abs(self.budget / p1)
            qty2 = abs((self.budget * beta) / p2)

            qty1 = float(self.exchange.amount_to_precision(s1_ccxt, qty1))
            qty2 = float(self.exchange.amount_to_precision(s2_ccxt, qty2))

            # [修復] 根據 Beta 的正負號，動態決定做多還是做空
            if side == 'LONG_SPREAD':
                s1_side = 'buy'
                s2_side = 'sell' if beta > 0 else 'buy'
            else:  # SHORT_SPREAD
                s1_side = 'sell'
                s2_side = 'buy' if beta > 0 else 'sell'

            logger.info(f"🚀 [EXEC] {side} {pair} | Beta:{beta:.2f} | S1:{s1_side} {qty1} | S2:{s2_side} {qty2}")

            self.exchange.create_order(s1_ccxt, 'market', s1_side, qty1)
            self.exchange.create_order(s2_ccxt, 'market', s2_side, qty2)

            new_trade = {
                'pair': pair, 's1': s1, 's2': s2, 'status': 'OPEN',
                'side': side, 'entry_z': sig['z_score'], 'entry_p1': p1, 'entry_p2': p2,
                'qty1': qty1, 'qty2': qty2, 'beta': beta,
                'entry_time': datetime.now(timezone.utc).isoformat()
            }
            pd.DataFrame([new_trade]).to_csv(self.trade_record_path, mode='a',
                                             header=not self.trade_record_path.exists(), index=False)
            logger.success(f"✅ Position successfully opened for {pair}")
        except Exception as e:
            logger.error(f"❌ Open failed {pair}: {e}")

    def _close_pair_position(self, pair, reason):
        try:
            df = pd.read_csv(self.trade_record_path)
            idx = df[(df['pair'] == pair) & (df['status'] == 'OPEN')].index
            if idx.empty: return
            trade = df.loc[idx[0]]

            s1, s2 = trade['s1'], trade['s2']
            qty1, qty2 = float(trade['qty1']), float(trade['qty2'])
            beta = float(trade['beta'])

            # [修復] 根據進場方向與 Beta 正負號，反向平倉
            if trade['side'] == 'LONG_SPREAD':
                s1_close = 'sell'
                s2_close = 'buy' if beta > 0 else 'sell'
            else:  # SHORT_SPREAD
                s1_close = 'buy'
                s2_close = 'sell' if beta > 0 else 'buy'

            s1_ccxt = self._to_ccxt_symbol(s1)
            s2_ccxt = self._to_ccxt_symbol(s2)

            logger.warning(f"⚡ [EXEC] Closing {pair} | Reason: {reason}")

            self.exchange.create_order(s1_ccxt, 'market', s1_close, qty1, params={'reduceOnly': True})
            self.exchange.create_order(s2_ccxt, 'market', s2_close, qty2, params={'reduceOnly': True})

            df.loc[idx, 'status'] = 'CLOSED'
            df.loc[idx, 'exit_time'] = datetime.now(timezone.utc).isoformat()
            df.loc[idx, 'exit_reason'] = reason
            df.to_csv(self.trade_record_path, index=False)
            logger.success(f"✅ Position successfully closed for {pair}")
        except Exception as e:
            logger.error(f"❌ Close failed {pair}: {e}")