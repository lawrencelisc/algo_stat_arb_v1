import time
import pandas as pd
import ccxt
import yaml
import numpy as np
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

# ── Limit Order 參數 ─────────────────────────────────────
LIMIT_TIMEOUT_SEC    = 30   # 掛單最長等待秒數
LIMIT_CHECK_INTERVAL = 2    # 每隔幾秒查詢一次訂單狀態
# 限價掛單相對 BBO 的偏移量：買單掛在 ask 內側 1 tick，賣單掛在 bid 內側 1 tick
LIMIT_OFFSET_PCT     = 0.0001  # 0.01%


class ExecutionManager:
    """
    [雙手] Execution Manager: 嚴格執行一多一空與原子撤單
    """
    VERSION = "v4.1.0-SafeExecution"

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
        # 只移除尾部 USDT，避免 replace() 誤截 base 名稱中含 USDT 的幣種
        clean = symbol[:-4] if symbol.endswith('USDT') else symbol
        return f"{clean}/USDT:USDT"

    def _try_limit_then_market(self, symbol: str, direction: str, qty: float) -> tuple[bool, str]:
        """
        先嘗試限價掛單（Maker 0.01%），超時後取消並改市價單（Taker 0.06%）。

        策略：
          - 買單：掛在 best ask 內側 LIMIT_OFFSET_PCT，成為 maker
          - 賣單：掛在 best bid 內側 LIMIT_OFFSET_PCT，成為 maker
          - 若 LIMIT_TIMEOUT_SEC 秒內未完全成交，取消並改市價

        回傳：(success: bool, fee_type: 'maker' | 'taker' | None)
        """
        order_id = None
        try:
            # 取 top-3 order book 確認流動性，並用 Level-1 定價
            ob = self.exchange.fetch_order_book(symbol, limit=3, params={'category': 'linear'})
            if not ob['bids'] or not ob['asks']:
                raise ValueError(f"Empty order book for {symbol}")

            if direction == 'buy':
                # 掛在 best ask 內側，略低於 ask → 成為 maker
                raw_price = ob['asks'][0][0] * (1 - LIMIT_OFFSET_PCT)
            else:
                # 掛在 best bid 內側，略高於 bid → 成為 maker
                raw_price = ob['bids'][0][0] * (1 + LIMIT_OFFSET_PCT)

            price = float(self.exchange.price_to_precision(symbol, raw_price))

            order = self.exchange.create_order(
                symbol, 'limit', direction, qty, price,
                params={'category': 'linear', 'timeInForce': 'GTC'}
            )
            order_id = order['id']
            logger.info(f"📋 Limit order placed: {symbol} {direction} {qty} @ {price} (id={order_id})")

            # 輪詢等待成交
            deadline = time.time() + LIMIT_TIMEOUT_SEC
            while time.time() < deadline:
                time.sleep(LIMIT_CHECK_INTERVAL)
                status = self.exchange.fetch_order(order_id, symbol, params={'category': 'linear'})
                if status['status'] == 'closed':
                    logger.success(f"✅ Limit filled (Maker): {symbol} {direction} {qty} @ {price}")
                    return True, 'maker'
                if status['status'] in ('canceled', 'rejected', 'expired'):
                    logger.warning(f"⚠️ Limit order {order_id} cancelled externally. Falling back to market.")
                    order_id = None
                    break

            # 超時：取消限價單，改市價單
            if order_id:
                logger.warning(f"⏰ Limit timeout ({LIMIT_TIMEOUT_SEC}s) for {symbol}. Cancelling → market order.")
                try:
                    self.exchange.cancel_order(order_id, symbol, params={'category': 'linear'})
                except Exception as e_cancel:
                    logger.warning(f"⚠️ Cancel failed (may already be filled): {e_cancel}")

            # 市價兜底
            self.exchange.create_order(symbol, 'market', direction, qty,
                                       params={'category': 'linear'})
            logger.info(f"✅ Market fallback filled (Taker): {symbol} {direction} {qty}")
            return True, 'taker'

        except Exception as e:
            logger.error(f"❌ _try_limit_then_market failed for {symbol}: {e}")
            # 確保掛單已取消，避免孤兒訂單
            if order_id:
                try:
                    self.exchange.cancel_order(order_id, symbol, params={'category': 'linear'})
                except Exception:
                    pass
            return False, None

    def get_open_positions(self):
        if not self.trade_record_path.exists(): return pd.DataFrame()
        try:
            df = pd.read_csv(self.trade_record_path)
            return df[df['status'] == 'OPEN'] if not df.empty else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    def execute_trades(self):
        if not self.signal_table_path.exists(): return
        try:
            signals = pd.read_csv(self.signal_table_path)
            active_df = self.get_open_positions()
            active_pairs = active_df['pair'].tolist() if not active_df.empty else []

            for _, sig in signals.iterrows():
                pair   = sig['pair']
                action = sig.get('action', 'MONITORING')
                # NaN z_score 是 FORCE_EXIT 的哨兵值，安全轉換避免後續 abs() 誤判
                z_raw  = sig['z_score']
                z      = float(z_raw) if pd.notna(z_raw) else float('nan')

                if action == 'FORCE_EXIT_EXPIRED' and pair in active_pairs:
                    self._close_pair_position(pair, "SIGNAL_EXPIRED")
                elif action == 'FORCE_EXIT_STOPLOSS' and pair in active_pairs:
                    logger.critical(f"🛑 STOP_LOSS executing for {pair}: z={z:.3f}. Closing position immediately.")
                    self._close_pair_position(pair, "STOP_LOSS")
                elif pair in active_pairs and not np.isnan(z) and abs(z) < 0.2:
                    self._close_pair_position(pair, "Z_REVERSION")
                elif pair not in active_pairs and action == 'MONITORING' and not np.isnan(z):
                    if z > 2.5:
                        # 開倉成功後立即加入 active_pairs，防止同週期重複開倉
                        if self._open_pair_position(pair, sig, 'SHORT_SPREAD'):
                            active_pairs.append(pair)
                    elif z < -2.5:
                        if self._open_pair_position(pair, sig, 'LONG_SPREAD'):
                            active_pairs.append(pair)
        except Exception as e:
            logger.error(f"❌ Execution loop error: {e}")

    def _open_pair_position(self, pair, sig, side):
        """
        開倉：S1 和 S2 均先嘗試限價掛單（Maker 0.01%），
        超時才改市價（Taker 0.06%）。
        S1 成功後 S2 若完全失敗，立即市價回滾 S1，確保原子性。
        成功回傳 True，失敗回傳 False。
        """
        s1, s2 = sig['pair'].split('-')
        beta = abs(float(sig['beta']))
        s1_ccxt, s2_ccxt = self._to_ccxt(s1), self._to_ccxt(s2)

        try:
            prices = self.exchange.fetch_tickers([s1_ccxt, s2_ccxt],
                                                 params={'category': 'linear'})
            p1 = prices.get(s1_ccxt, {}).get('last')
            p2 = prices.get(s2_ccxt, {}).get('last')
            if p1 is None or p2 is None:
                logger.error(f"❌ Cannot fetch prices for {pair}: p1={p1}, p2={p2}. Aborting open.")
                return False

            qty1 = float(self.exchange.amount_to_precision(s1_ccxt, self.budget / p1))
            qty2 = float(self.exchange.amount_to_precision(s2_ccxt, (self.budget * beta) / p2))

            s1_side = 'buy' if side == 'LONG_SPREAD' else 'sell'
            s2_side = 'sell' if side == 'LONG_SPREAD' else 'buy'

            logger.info(f"🚀 [EXEC] {side} {pair} | S1:{s1_side} {qty1} @ limit | S2:{s2_side} {qty2} @ limit")

            # ── S1：限價優先，超時改市價 ──────────────────────
            s1_ok, s1_fee_type = self._try_limit_then_market(s1_ccxt, s1_side, qty1)
            if not s1_ok:
                logger.error(f"❌ S1 open failed for {pair}. Aborting.")
                return False

            # ── S2：限價優先，超時改市價 ──────────────────────
            s2_ok, s2_fee_type = self._try_limit_then_market(s2_ccxt, s2_side, qty2)
            if not s2_ok:
                # S1 已開倉，S2 失敗 → 立即市價回滾 S1
                logger.critical(f"🚨 S2 open failed for {pair}! Rolling back S1 with market order.")
                s1_rollback = 'sell' if s1_side == 'buy' else 'buy'
                try:
                    self.exchange.create_order(s1_ccxt, 'market', s1_rollback, qty1,
                                               params={'category': 'linear', 'reduceOnly': True})
                    logger.info(f"↩️ S1 rollback succeeded for {pair}.")
                except Exception as e_rb:
                    logger.critical(f"💀 S1 rollback failed for {pair}: {e_rb}. Manual intervention required!")
                return False

            fee_note = f"S1={s1_fee_type} S2={s2_fee_type}"
            new_trade = {
                'pair': pair, 's1': s1, 's2': s2, 'status': 'OPEN', 'side': side,
                'entry_z': sig['z_score'], 'entry_p1': p1, 'entry_p2': p2,
                'qty1': qty1, 'qty2': qty2, 'beta': beta,
                'open_fee_type': fee_note,
                'entry_time': datetime.now(timezone.utc).isoformat()
            }
            pd.DataFrame([new_trade]).to_csv(self.trade_record_path, mode='a',
                                             header=not self.trade_record_path.exists(), index=False)
            logger.success(f"✅ Market Neutral Position opened for {pair} ({fee_note})")
            return True
        except Exception as e:
            logger.error(f"❌ Pair open error {pair}: {e}")
            return False

    def _close_pair_position(self, pair, reason):
        try:
            df = pd.read_csv(self.trade_record_path)
            idx = df[(df['pair'] == pair) & (df['status'] == 'OPEN')].index
            if idx.empty: return
            trade = df.loc[idx[0]]

            s1_ccxt, s2_ccxt = self._to_ccxt(trade['s1']), self._to_ccxt(trade['s2'])
            qty1, qty2 = float(trade['qty1']), float(trade['qty2'])

            # 問題六修正：從交易所讀取實際倉位大小，防止因強平/手動調整導致 qty 與記錄不符
            try:
                open_positions = self.exchange.fetch_positions(
                    symbols=[s1_ccxt, s2_ccxt], params={'category': 'linear'}
                )
                pos_map = {
                    p['symbol']: abs(float(p.get('contracts', 0) or 0))
                    for p in open_positions
                }
                actual_q1 = pos_map.get(s1_ccxt, qty1)
                actual_q2 = pos_map.get(s2_ccxt, qty2)
                if actual_q1 != qty1:
                    logger.warning(f"⚠️ {trade['s1']} qty mismatch: CSV={qty1}, exchange={actual_q1}. Using exchange qty.")
                    qty1 = actual_q1
                if actual_q2 != qty2:
                    logger.warning(f"⚠️ {trade['s2']} qty mismatch: CSV={qty2}, exchange={actual_q2}. Using exchange qty.")
                    qty2 = actual_q2
            except Exception as e_pos:
                logger.warning(f"⚠️ Cannot verify actual position size for {pair}, using CSV qty: {e_pos}")

            s1_close = 'sell' if trade['side'] == 'LONG_SPREAD' else 'buy'
            s2_close = 'buy' if trade['side'] == 'LONG_SPREAD' else 'sell'

            logger.warning(f"⚡ [EXEC] Closing {pair} | Reason: {reason}")

            # 問題一修正：兩腿各自獨立 try-except，確保一腿失敗不阻斷另一腿執行
            s1_closed, s2_closed = False, False

            try:
                if qty1 > 0:
                    self.exchange.create_order(s1_ccxt, 'market', s1_close, qty1, params={'reduceOnly': True})
                s1_closed = True
            except Exception as e1:
                logger.error(f"❌ S1 close failed for {pair} ({trade['s1']}): {e1}")

            try:
                if qty2 > 0:
                    self.exchange.create_order(s2_ccxt, 'market', s2_close, qty2, params={'reduceOnly': True})
                s2_closed = True
            except Exception as e2:
                logger.error(f"❌ S2 close failed for {pair} ({trade['s2']}): {e2}")

            # 依實際結果寫入狀態，PARTIAL_CLOSE 需人工介入
            if s1_closed and s2_closed:
                df.loc[idx, 'status'] = 'CLOSED'
                logger.success(f"✅ Position closed for {pair}")
            elif s1_closed or s2_closed:
                df.loc[idx, 'status'] = 'PARTIAL_CLOSE'
                logger.critical(f"🚨 {pair} PARTIAL CLOSE: S1={s1_closed}, S2={s2_closed}. Manual intervention required!")
            else:
                logger.critical(f"🚨 {pair} both legs failed to close. Position unchanged in records.")

            df.loc[idx, 'exit_time'] = datetime.now(timezone.utc).isoformat()
            df.loc[idx, 'exit_reason'] = reason
            df.to_csv(self.trade_record_path, index=False)
        except Exception as e:
            logger.error(f"❌ Close failed {pair}: {e}")