import pandas as pd
import ccxt
import os
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone


class ExecutionManager:
    """
    [STAGE 3] Execution Manager Module
    Location: /utils/execution.py
    Responsibility: Read signals from signal_table.csv and execute trades on Bybit.
    """
    VERSION = "v3.0.1-Stable"

    def __init__(self, budget_per_pair=1500.0):
        self.root_dir = Path(__file__).resolve().parent.parent
        self.signal_table_path = self.root_dir / 'data' / 'signal' / 'signal_table.csv'
        self.trade_record_path = self.root_dir / 'data' / 'trade' / 'trade_record.csv'
        self.budget = budget_per_pair

        # Initialize Exchange
        from core.connect import DataBridge
        bridge = DataBridge()
        # Use the correct account name from your config
        api_config = bridge.load_bybit_api_config('algo_pair_trade')

        self.exchange = ccxt.bybit({
            'apiKey': api_config['PT_API_KEY'],
            'secret': api_config['PT_SECRET_KEY'],
            'enableRateLimit': True,
            'options': {'defaultType': 'linear'}
        })

        # Ensure directories exist
        self.trade_record_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"🚀 Initializing ExecutionManager {self.VERSION}")

    def get_open_positions(self):
        """Fetch current open positions from local CSV records."""
        if not self.trade_record_path.exists():
            return pd.DataFrame()
        try:
            df = pd.read_csv(self.trade_record_path)
            return df[df['status'] == 'OPEN']
        except:
            return pd.DataFrame()

    def execute_trades(self):
        """
        [FIX] The core method called by main_entry.py
        Processes signals and manages Bybit positions.
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

                # --- 邏輯 A: 強制平倉 (SIGNAL_EXPIRED) ---
                if action == 'FORCE_EXIT_EXPIRED' and pair in active_pairs:
                    self._close_pair_position(pair, "SIGNAL_EXPIRED")
                    continue

                # --- 邏輯 B: 常規平倉 (Z 回歸) ---
                if pair in active_pairs:
                    # 假設持倉是 Long Spread (S1買/S2賣)，Z 回到 0 附近則平倉
                    # 這裡根據您的具體策略閾值設定
                    if abs(z) < 0.2:
                        self._close_pair_position(pair, "Z_REVERSION")
                    continue

                # --- 邏輯 C: 開倉 (Z 偏離) ---
                if pair not in active_pairs and action == 'MONITORING':
                    if z > 2.0:
                        self._open_pair_position(pair, sig, side='SHORT_SPREAD')  # S1賣 S2買
                    elif z < -2.0:
                        self._open_pair_position(pair, sig, side='LONG_SPREAD')  # S1買 S2賣

        except Exception as e:
            logger.error(f"❌ Execution error: {e}")

    def _open_pair_position(self, pair, sig, side):
        """實作開倉下單邏輯 (略)"""
        logger.info(f"⚡ [EXEC] Opening {side} for {pair} at Z={sig['z_score']}")
        # 實作 Bybit create_order...
        pass

    def _close_pair_position(self, pair, reason):
        """實作平倉下單邏輯 (略)"""
        logger.warning(f"⚡ [EXEC] Closing {pair} due to {reason}")
        # 實作 Bybit create_order (Reduce Only)...
        # 並更新 trade_record.csv 為 CLOSED
        pass