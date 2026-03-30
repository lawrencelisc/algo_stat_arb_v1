import pandas as pd
import ccxt
import yaml
import numpy as np
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone


class ExecutionManager:
    """
    [雙手] Execution Manager: 嚴格執行一多一空與原子撤單
    """
    VERSION = "v4.0.0-FirstPrinciples"

    def __init__(self, budget_per_pair=1500.0):
        self.root_dir = Path(__file__).resolve().parent.parent
        self.signal_table_path = self.root_dir / 'data' / 'signal' / 'signal_table.csv'
        self.trade_record_path = self.root_dir / 'data' / 'trade' / 'trade_record.csv'
        self.budget = budget_per_pair

        # 獨立讀取 Config，安全無依賴
        try:
            config_path = self.root_dir / 'config' / 'config.yaml'
            if not config_path.exists(): config_path = self.root_dir / 'config.yaml'
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)

            api_key, api_secret = None, None
            if 'algo_pair_trade' in config:
                api_key = config['algo_pair_trade'].get('PT_API_KEY')
                api_secret = config['algo_pair_trade'].get('PT_SECRET_KEY')
            if not api_key and 'ACCOUNTS' in config:
                for acc_name, acc_data in config['ACCOUNTS'].items():
                    api_key, api_secret = acc_data.get('key'), acc_data.get('secret')
                    if api_key: break

            self.exchange = ccxt.bybit({'apiKey': api_key, 'secret': api_secret, 'enableRateLimit': True,
                                        'options': {'defaultType': 'linear'}})
            logger.info(f"✅ ExecutionManager {self.VERSION} connected to Bybit.")
        except Exception as e:
            logger.error(f"❌ Init failed: {e}");
            raise
        self.trade_record_path.parent.mkdir(parents=True, exist_ok=True)

    def _to_ccxt(self, symbol):
        return f"{symbol.replace('USDT', '')}/USDT:USDT"

    def get_open_positions(self):
        if not self.trade_record_path.exists(): return pd.DataFrame()
        try:
            df = pd.read_csv(self.trade_record_path)
            return df[df['status'] == 'OPEN'] if not df.empty else pd.DataFrame()
        except:
            return pd.DataFrame()

    def execute_trades(self):
        if not self.signal_table_path.exists(): return
        try:
            signals = pd.read_csv(self.signal_table_path)
            active_df = self.get_open_positions()
            active_pairs = active_df['pair'].tolist() if not active_df.empty else []

            for _, sig in signals.iterrows():
                pair, z, action = sig['pair'], float(sig['z_score']), sig.get('action', 'MONITORING')

                if action == 'FORCE_EXIT_EXPIRED' and pair in active_pairs:
                    self._close_pair_position(pair, "SIGNAL_EXPIRED")
                elif pair in active_pairs and abs(z) < 0.2:
                    self._close_pair_position(pair, "Z_REVERSION")
                elif pair not in active_pairs and action == 'MONITORING':
                    if z > 2.0:
                        self._open_pair_position(pair, sig, 'SHORT_SPREAD')
                    elif z < -2.0:
                        self._open_pair_position(pair, sig, 'LONG_SPREAD')
        except Exception as e:
            logger.error(f"❌ Execution loop error: {e}")

    def _open_pair_position(self, pair, sig, side):
        s1, s2 = sig['pair'].split('-')
        beta = abs(float(sig['beta']))  # 保證 Beta 是正數用於計算數量
        s1_ccxt, s2_ccxt = self._to_ccxt(s1), self._to_ccxt(s2)

        try:
            prices = self.exchange.fetch_tickers([s1_ccxt, s2_ccxt])
            p1, p2 = prices[s1_ccxt]['last'], prices[s2_ccxt]['last']

            qty1 = float(self.exchange.amount_to_precision(s1_ccxt, abs(self.budget / p1)))
            qty2 = float(self.exchange.amount_to_precision(s2_ccxt, abs((self.budget * beta) / p2)))

            # 🛡️ 絕對對沖邏輯 (一多一空)
            s1_side = 'buy' if side == 'LONG_SPREAD' else 'sell'
            s2_side = 'sell' if side == 'LONG_SPREAD' else 'buy'

            logger.info(f"🚀 [EXEC] {side} {pair} | S1:{s1_side} {qty1} | S2:{s2_side} {qty2}")

            # 原子交易執行 (Atomic Execution)
            try:
                self.exchange.create_order(s1_ccxt, 'market', s1_side, qty1)
            except Exception as e1:
                logger.error(f"❌ S1 Open Failed: {e1}"); return

            try:
                self.exchange.create_order(s2_ccxt, 'market', s2_side, qty2)
            except Exception as e2:
                logger.critical(f"🚨 S2 Open Failed! Rolling back S1. Error: {e2}")
                s1_rollback = 'sell' if s1_side == 'buy' else 'buy'
                try:
                    self.exchange.create_order(s1_ccxt, 'market', s1_rollback, qty1, params={'reduceOnly': True})
                except Exception as e3:
                    logger.critical(f"💀 Rollback Failed: {e3}")
                return

            # 寫入記錄
            new_trade = {
                'pair': pair, 's1': s1, 's2': s2, 'status': 'OPEN', 'side': side,
                'entry_z': sig['z_score'], 'entry_p1': p1, 'entry_p2': p2,
                'qty1': qty1, 'qty2': qty2, 'beta': beta,
                'entry_time': datetime.now(timezone.utc).isoformat()
            }
            pd.DataFrame([new_trade]).to_csv(self.trade_record_path, mode='a',
                                             header=not self.trade_record_path.exists(), index=False)
            logger.success(f"✅ Market Neutral Position opened for {pair}")
        except Exception as e:
            logger.error(f"❌ Pair open error {pair}: {e}")

    def _close_pair_position(self, pair, reason):
        try:
            df = pd.read_csv(self.trade_record_path)
            idx = df[(df['pair'] == pair) & (df['status'] == 'OPEN')].index
            if idx.empty: return
            trade = df.loc[idx[0]]

            s1_ccxt, s2_ccxt = self._to_ccxt(trade['s1']), self._to_ccxt(trade['s2'])
            qty1, qty2 = float(trade['qty1']), float(trade['qty2'])

            s1_close = 'sell' if trade['side'] == 'LONG_SPREAD' else 'buy'
            s2_close = 'buy' if trade['side'] == 'LONG_SPREAD' else 'sell'

            logger.warning(f"⚡ [EXEC] Closing {pair} | Reason: {reason}")
            self.exchange.create_order(s1_ccxt, 'market', s1_close, qty1, params={'reduceOnly': True})
            self.exchange.create_order(s2_ccxt, 'market', s2_close, qty2, params={'reduceOnly': True})

            df.loc[idx, 'status'] = 'CLOSED'
            df.loc[idx, 'exit_time'] = datetime.now(timezone.utc).isoformat()
            df.loc[idx, 'exit_reason'] = reason
            df.to_csv(self.trade_record_path, index=False)
            logger.success(f"✅ Position closed for {pair}")
        except Exception as e:
            logger.error(f"❌ Close failed {pair}: {e}")