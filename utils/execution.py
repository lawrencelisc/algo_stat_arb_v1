import pandas as pd
import ccxt
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
    VERSION = "v3.1.2-Guardian"

    def __init__(self, budget_per_pair=1500.0):
        self.root_dir = Path(__file__).resolve().parent.parent
        self.signal_table_path = self.root_dir / 'data' / 'signal' / 'signal_table.csv'
        self.trade_record_path = self.root_dir / 'data' / 'trade' / 'trade_record.csv'
        self.budget = budget_per_pair

        # --- [修復：模組路徑容錯引進] ---
        try:
            # 優先嘗試引進 api_connect，若失敗則嘗試 connect
            try:
                from core.api_connect import DataBridge
            except ImportError:
                from core.connect import DataBridge

            self.bridge = DataBridge()
            api_config = self.bridge.load_bybit_api_config('algo_pair_trade')

            self.exchange = ccxt.bybit({
                'apiKey': api_config['PT_API_KEY'],
                'secret': api_config['PT_SECRET_KEY'],
                'enableRateLimit': True,
                'options': {'defaultType': 'linear'}
            })
            logger.info(f"✅ ExecutionManager {self.VERSION} connected to Bybit.")
        except Exception as e:
            # 此處報錯會被 main_entry.py 捕捉
            logger.error(f"❌ Failed to initialize Exchange: {e}")
            raise

        # Ensure directories exist
        self.trade_record_path.parent.mkdir(parents=True, exist_ok=True)

    def get_open_positions(self):
        """Fetch current open positions from local CSV records."""
        if not self.trade_record_path.exists():
            return pd.DataFrame()
        try:
            df = pd.read_csv(self.trade_record_path)
            if df.empty: return pd.DataFrame()
            return df[df['status'] == 'OPEN']
        except:
            return pd.DataFrame()

    def execute_trades(self):
        """
        Processes signals and manages Bybit positions.
        Called every 5 minutes by main_entry.py.
        """
        if not self.signal_table_path.exists():
            return

        try:
            signals = pd.read_csv(self.signal_table_path)
            open_positions = self.get_open_positions()
            active_pairs = open_positions['pair'].tolist() if not open_positions.empty else []

            for _, sig in signals.iterrows():
                pair = sig['pair']
                z = sig['z_score']
                action = sig.get('action', 'MONITORING')

                # --- 邏輯 A: 緊急撤退 (P-Value 失效) ---
                if action == 'FORCE_EXIT_EXPIRED' and pair in active_pairs:
                    self._close_pair_position(pair, "SIGNAL_EXPIRED")
                    continue

                # --- 邏輯 B: 常規止盈平倉 (Z-Score 回歸) ---
                if pair in active_pairs:
                    if abs(z) < 0.2:
                        self._close_pair_position(pair, "Z_REVERSION")
                    continue

                # --- 邏輯 C: 開倉 (Z-Score 偏離) ---
                if pair not in active_pairs and action == 'MONITORING':
                    if z > 2.0:
                        self._open_pair_position(pair, sig, side='SHORT_SPREAD')
                    elif z < -2.0:
                        self._open_pair_position(pair, sig, side='LONG_SPREAD')

        except Exception as e:
            logger.error(f"❌ Execution loop error: {e}")

    def _open_pair_position(self, pair, sig, side):
        """執行雙腿開倉下單邏輯 (市價單)"""
        s1, s2 = sig['pair'].split('-')
        beta = float(sig['beta'])
        try:
            prices = self.exchange.fetch_tickers([s1, s2])
            p1, p2 = prices[s1]['last'], prices[s2]['last']

            qty1 = self.budget / p1
            qty2 = (self.budget * beta) / p2

            qty1 = float(self.exchange.amount_to_precision(s1, qty1))
            qty2 = float(self.exchange.amount_to_precision(s2, qty2))

            s1_side = 'buy' if side == 'LONG_SPREAD' else 'sell'
            s2_side = 'sell' if side == 'LONG_SPREAD' else 'buy'

            logger.info(f"🚀 [EXEC] {side} {pair} | S1:{s1_side} {qty1} | S2:{s2_side} {qty2}")
            self.exchange.create_order(s1, 'market', s1_side, qty1)
            self.exchange.create_order(s2, 'market', s2_side, qty2)

            new_trade = {
                'pair': pair, 's1': s1, 's2': s2, 'status': 'OPEN',
                'side': side, 'entry_z': sig['z_score'], 'entry_p1': p1, 'entry_p2': p2,
                'qty1': qty1, 'qty2': qty2, 'beta': beta,
                'entry_time': datetime.now(timezone.utc).isoformat()
            }
            pd.DataFrame([new_trade]).to_csv(self.trade_record_path, mode='a',
                                             header=not self.trade_record_path.exists(), index=False)
        except Exception as e:
            logger.error(f"❌ Open failed {pair}: {e}")

    def _close_pair_position(self, pair, reason):
        """執行雙腿平倉離場邏輯 (Reduce Only)"""
        try:
            df = pd.read_csv(self.trade_record_path)
            idx = df[(df['pair'] == pair) & (df['status'] == 'OPEN')].index
            if idx.empty: return
            trade = df.loc[idx[0]]

            s1, s2 = trade['s1'], trade['s2']
            qty1, qty2 = trade['qty1'], trade['qty2']
            s1_close = 'sell' if trade['side'] == 'LONG_SPREAD' else 'buy'
            s2_close = 'buy' if trade['side'] == 'LONG_SPREAD' else 'sell'

            logger.warning(f"⚡ [EXEC] Closing {pair} | Reason: {reason}")
            self.exchange.create_order(s1, 'market', s1_close, qty1, params={'reduceOnly': True})
            self.exchange.create_order(s2, 'market', s2_close, qty2, params={'reduceOnly': True})

            df.loc[idx, 'status'] = 'CLOSED'
            df.loc[idx, 'exit_time'] = datetime.now(timezone.utc).isoformat()
            df.loc[idx, 'exit_reason'] = reason
            df.to_csv(self.trade_record_path, index=False)
        except Exception as e:
            logger.error(f"❌ Close failed {pair}: {e}")