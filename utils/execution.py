import time
import pandas as pd
import ccxt
import yaml
import numpy as np
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

# ── 開倉 / 平倉 Z-Score 門檻 ─────────────────────────────
ENTRY_Z_L1 = 2.0    # Level 1 開倉門檻（首次入場，1.0× 倉位）
ENTRY_Z_L2 = 2.5    # Level 2 加倉門檻（加碼同方向，0.8× 倉位）
ENTRY_Z_L3 = 3.0    # Level 3 加倉門檻（加碼同方向，0.6× 倉位）
ENTRY_Z_L4 = 4.0    # Level 4 加倉門檻（加碼同方向，0.4× 倉位）
SIZE_L1    = 1.0    # L1 倉位乘數（×budget）
SIZE_L2    = 0.8    # L2 倉位乘數（×budget）
SIZE_L3    = 0.6    # L3 倉位乘數（×budget）
SIZE_L4    = 0.4    # L4 倉位乘數（×budget）
EXIT_Z     = 0.2    # z 絕對值低於此值觸發均值回歸平倉

# 加倉計劃表：(觸發門檻, 倉位乘數, 升級後 entry_level)
_ADD_ON_LEVELS = [
    (ENTRY_Z_L2, SIZE_L2, 2),
    (ENTRY_Z_L3, SIZE_L3, 3),
    (ENTRY_Z_L4, SIZE_L4, 4),
]

# ── Limit Order 參數 ─────────────────────────────────────
LIMIT_TIMEOUT_SEC    = 30      # 掛單最長等待秒數
LIMIT_CHECK_INTERVAL = 2       # 每隔幾秒查詢一次訂單狀態
LIMIT_OFFSET_PCT     = 0.0001  # 掛單相對 BBO 偏移量（0.01%），買在 ask 內側，賣在 bid 內側

# ── Leverage 設定 ─────────────────────────────────────────
PAIR_LEVERAGE = 10             # 雙腿統一 leverage（必須在 Bybit 允許範圍內）


class ExecutionManager:
    """
    [雙手] Execution Manager: 嚴格執行一多一空與原子撤單
    """
    VERSION = "v4.4.0-ParallelLegs"

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

    @staticmethod
    def _limit_price_from_ob5(ob: dict, direction: str) -> float:
        """
        用 top-5 order book levels 計算 VWAP，作為限價單參考基準。

        Buy  → 取 asks 前 5 層 VWAP，再向內偏移 LIMIT_OFFSET_PCT（略低於 VWAP）
        Sell → 取 bids 前 5 層 VWAP，再向內偏移 LIMIT_OFFSET_PCT（略高於 VWAP）

        相比只用 Level-1 BBO：
          - 參考價格更穩定，不會被單一薄層帶偏
          - 掛單位置落在市場實際流動性重心，填單率更高
          - 5 層 VWAP 通常介於 L1 價格與 L5 價格之間，掛單比純 L1 更積極
        """
        levels = ob['asks'][:5] if direction == 'buy' else ob['bids'][:5]
        if not levels:
            raise ValueError(f"Order book has no {'ask' if direction == 'buy' else 'bid'} levels")

        total_qty   = sum(qty for _, qty in levels)
        vwap        = sum(px * qty for px, qty in levels) / total_qty if total_qty > 0 else levels[0][0]

        if direction == 'buy':
            return vwap * (1 - LIMIT_OFFSET_PCT)
        else:
            return vwap * (1 + LIMIT_OFFSET_PCT)

    def _set_pair_leverage(self, s1_ccxt: str, s2_ccxt: str) -> bool:
        """
        開倉前為兩腿設定相同 leverage，確保保證金比率對稱。
        任一腿失敗即回傳 False，阻止開倉。
        """
        for sym in (s1_ccxt, s2_ccxt):
            try:
                self.exchange.set_leverage(PAIR_LEVERAGE, sym, params={'category': 'linear'})
                logger.info(f"⚙️ Leverage set: {sym} → {PAIR_LEVERAGE}x")
            except Exception as e:
                logger.error(f"❌ Failed to set leverage for {sym}: {e}")
                return False
        return True

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
            ob = self.exchange.fetch_order_book(symbol, limit=5, params={'category': 'linear'})
            if not ob['bids'] or not ob['asks']:
                raise ValueError(f"Empty order book for {symbol}")

            price = float(self.exchange.price_to_precision(
                symbol, self._limit_price_from_ob5(ob, direction)))

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
                status = self.exchange.fetch_order(order_id, symbol, params={'category': 'linear', 'acknowledged': True})
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

    def _execute_pair(
        self,
        s1_ccxt: str, s1_side: str, s1_qty: float,
        s2_ccxt: str, s2_side: str, s2_qty: float,
        label: str = "",
    ) -> tuple[bool, str | None, bool, str | None]:
        """
        並行雙腿執行：兩腿共用同一個 deadline，大幅壓縮腿風險窗口。

        流程：
          Phase 1 — 順序掛兩個限價單（< 1s，避免 CCXT 單實例多線程競爭）
          Phase 2 — 統一輪詢：兩腿共用同一個 LIMIT_TIMEOUT_SEC deadline
          Phase 3 — 超時後同時取消剩餘掛單，再順序發市價兜底

        最壞情況腿風險窗口：~30s（共享等待）+ ~2s（市價），vs 舊版 ~60s。
        回傳：(s1_ok, s1_fee_type, s2_ok, s2_fee_type)
        """
        s1_id = s2_id = None
        s1_price = s2_price = None
        s1_fee: str | None = None
        s2_fee: str | None = None
        s1_done = s2_done = False

        try:
            # ── Phase 1: 掛兩個限價單（top-5 VWAP 定價）──────────
            ob1 = self.exchange.fetch_order_book(s1_ccxt, limit=5, params={'category': 'linear'})
            if not ob1['bids'] or not ob1['asks']:
                raise ValueError(f"Empty order book for {s1_ccxt}")
            s1_price = float(self.exchange.price_to_precision(
                s1_ccxt, self._limit_price_from_ob5(ob1, s1_side)))
            o1 = self.exchange.create_order(
                s1_ccxt, 'limit', s1_side, s1_qty, s1_price,
                params={'category': 'linear', 'timeInForce': 'GTC'})
            s1_id = o1['id']
            logger.info(f"📋 [{label}] Limit placed: {s1_ccxt} {s1_side} {s1_qty} @ {s1_price} (id={s1_id})")

            ob2 = self.exchange.fetch_order_book(s2_ccxt, limit=5, params={'category': 'linear'})
            if not ob2['bids'] or not ob2['asks']:
                raise ValueError(f"Empty order book for {s2_ccxt}")
            s2_price = float(self.exchange.price_to_precision(
                s2_ccxt, self._limit_price_from_ob5(ob2, s2_side)))
            o2 = self.exchange.create_order(
                s2_ccxt, 'limit', s2_side, s2_qty, s2_price,
                params={'category': 'linear', 'timeInForce': 'GTC'})
            s2_id = o2['id']
            logger.info(f"📋 [{label}] Limit placed: {s2_ccxt} {s2_side} {s2_qty} @ {s2_price} (id={s2_id})")

            # ── Phase 2: 統一輪詢（共享 deadline）──────────────────
            deadline = time.time() + LIMIT_TIMEOUT_SEC
            while time.time() < deadline:
                time.sleep(LIMIT_CHECK_INTERVAL)

                if not s1_done and s1_id:
                    st1 = self.exchange.fetch_order(s1_id, s1_ccxt,
                                                    params={'category': 'linear', 'acknowledged': True})
                    if st1['status'] == 'closed':
                        s1_fee, s1_done, s1_id = 'maker', True, None
                        logger.success(f"✅ [{label}] Maker filled: {s1_ccxt} {s1_side} {s1_qty} @ {s1_price}")
                    elif st1['status'] in ('canceled', 'rejected', 'expired'):
                        s1_id = None  # 外部取消，Phase 3 改市價

                if not s2_done and s2_id:
                    st2 = self.exchange.fetch_order(s2_id, s2_ccxt,
                                                    params={'category': 'linear', 'acknowledged': True})
                    if st2['status'] == 'closed':
                        s2_fee, s2_done, s2_id = 'maker', True, None
                        logger.success(f"✅ [{label}] Maker filled: {s2_ccxt} {s2_side} {s2_qty} @ {s2_price}")
                    elif st2['status'] in ('canceled', 'rejected', 'expired'):
                        s2_id = None

                if s1_done and s2_done:
                    break

            # ── Phase 3: 取消剩餘掛單，市價兜底 ────────────────────
            for done, oid, sym, side, qty, attr in [
                (s1_done, s1_id, s1_ccxt, s1_side, s1_qty, 's1'),
                (s2_done, s2_id, s2_ccxt, s2_side, s2_qty, 's2'),
            ]:
                if done:
                    continue
                if oid:
                    logger.warning(f"⏰ [{label}] Limit timeout {sym}. Cancelling → market.")
                    try:
                        self.exchange.cancel_order(oid, sym, params={'category': 'linear'})
                    except Exception:
                        pass
                try:
                    self.exchange.create_order(sym, 'market', side, qty,
                                               params={'category': 'linear'})
                    logger.info(f"✅ [{label}] Market fallback filled: {sym} {side} {qty}")
                    if attr == 's1':
                        s1_fee, s1_done = 'taker', True
                    else:
                        s2_fee, s2_done = 'taker', True
                except Exception as e_mkt:
                    logger.error(f"❌ [{label}] Market fallback failed {sym}: {e_mkt}")

            return s1_done, s1_fee, s2_done, s2_fee

        except Exception as e:
            logger.error(f"❌ [{label}] _execute_pair failed: {e}")
            for oid, sym in [(s1_id, s1_ccxt), (s2_id, s2_ccxt)]:
                if oid:
                    try:
                        self.exchange.cancel_order(oid, sym, params={'category': 'linear'})
                    except Exception:
                        pass
            return s1_done, None, s2_done, None

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
                elif pair in active_pairs and not np.isnan(z) and abs(z) < EXIT_Z:
                    self._close_pair_position(pair, "Z_REVERSION")
                elif pair in active_pairs and action == 'MONITORING' and not np.isnan(z):
                    # L2 加倉：持倉方向一致 且 z 繼續擴大超過 L2 門檻
                    self._try_add_to_position(pair, sig, z)
                elif pair not in active_pairs and action == 'MONITORING' and not np.isnan(z):
                    if z > ENTRY_Z_L1:
                        if self._open_pair_position(pair, sig, 'SHORT_SPREAD', SIZE_L1):
                            active_pairs.append(pair)
                    elif z < -ENTRY_Z_L1:
                        if self._open_pair_position(pair, sig, 'LONG_SPREAD', SIZE_L1):
                            active_pairs.append(pair)
        except Exception as e:
            logger.error(f"❌ Execution loop error: {e}")

    def _open_pair_position(self, pair, sig, side, size_multiplier: float = 1.0):
        """
        開倉：S1 和 S2 均先嘗試限價掛單（Maker 0.01%），
        超時才改市價（Taker 0.06%）。
        S1 成功後 S2 若完全失敗，立即市價回滾 S1，確保原子性。
        size_multiplier: 實際投入 = budget × size_multiplier。
        成功回傳 True，失敗回傳 False。
        """
        s1, s2 = sig['pair'].split('-')
        beta = abs(float(sig['beta']))
        s1_ccxt, s2_ccxt = self._to_ccxt(s1), self._to_ccxt(s2)
        alloc = self.budget * size_multiplier

        try:
            # ── 開倉前先確保兩腿 leverage 一致 ───────────────────
            if not self._set_pair_leverage(s1_ccxt, s2_ccxt):
                logger.error(f"❌ Leverage setup failed for {pair}. Aborting open.")
                return False

            prices = self.exchange.fetch_tickers([s1_ccxt, s2_ccxt],
                                                 params={'category': 'linear'})
            p1 = prices.get(s1_ccxt, {}).get('last')
            p2 = prices.get(s2_ccxt, {}).get('last')
            if p1 is None or p2 is None:
                logger.error(f"❌ Cannot fetch prices for {pair}: p1={p1}, p2={p2}. Aborting open.")
                return False

            qty1 = float(self.exchange.amount_to_precision(s1_ccxt, alloc / p1))
            qty2 = float(self.exchange.amount_to_precision(s2_ccxt, (alloc * beta) / p2))

            s1_side = 'buy' if side == 'LONG_SPREAD' else 'sell'
            s2_side = 'sell' if side == 'LONG_SPREAD' else 'buy'

            s1_notional = qty1 * p1
            s2_notional = qty2 * p2
            logger.info(
                f"🚀 [EXEC] {side} {pair} | "
                f"S1:{s1_side} {qty1} (${s1_notional:.1f}) | "
                f"S2:{s2_side} {qty2} (${s2_notional:.1f}) | "
                f"beta={beta:.3f} net=${abs(s2_notional - s1_notional):.1f}"
            )

            # ── 並行雙腿：同時掛限價，共用 deadline ────────────
            s1_ok, s1_fee_type, s2_ok, s2_fee_type = self._execute_pair(
                s1_ccxt, s1_side, qty1,
                s2_ccxt, s2_side, qty2,
                label=f"OPEN {pair}",
            )

            if not s1_ok and not s2_ok:
                logger.error(f"❌ Both legs failed for {pair}. Aborting.")
                return False

            if s1_ok and not s2_ok:
                logger.critical(f"🚨 S2 open failed for {pair}! Rolling back S1 with market order.")
                s1_rollback = 'sell' if s1_side == 'buy' else 'buy'
                try:
                    self.exchange.create_order(s1_ccxt, 'market', s1_rollback, qty1,
                                               params={'category': 'linear', 'reduceOnly': True})
                    logger.info(f"↩️ S1 rollback succeeded for {pair}.")
                except Exception as e_rb:
                    logger.critical(f"💀 S1 rollback failed for {pair}: {e_rb}. Manual intervention required!")
                return False

            if not s1_ok and s2_ok:
                logger.critical(f"🚨 S1 open failed for {pair}! Rolling back S2 with market order.")
                s2_rollback = 'buy' if s2_side == 'sell' else 'sell'
                try:
                    self.exchange.create_order(s2_ccxt, 'market', s2_rollback, qty2,
                                               params={'category': 'linear', 'reduceOnly': True})
                    logger.info(f"↩️ S2 rollback succeeded for {pair}.")
                except Exception as e_rb:
                    logger.critical(f"💀 S2 rollback failed for {pair}: {e_rb}. Manual intervention required!")
                return False

            fee_note = f"S1={s1_fee_type} S2={s2_fee_type}"
            new_trade = {
                'pair': pair, 's1': s1, 's2': s2, 'status': 'OPEN', 'side': side,
                'entry_z': sig['z_score'], 'entry_p1': p1, 'entry_p2': p2,
                'qty1': qty1, 'qty2': qty2, 'beta': beta,
                'entry_level': 1, 'l2_entry_z': None, 'l2_entry_time': None,
                'open_fee_type': fee_note,
                'entry_time': datetime.now(timezone.utc).isoformat()
            }
            need_header = not (self.trade_record_path.exists() and self.trade_record_path.stat().st_size > 0)
            pd.DataFrame([new_trade]).to_csv(self.trade_record_path, mode='a',
                                             header=need_header, index=False)
            logger.success(f"✅ Market Neutral Position opened for {pair} ({fee_note})")
            return True
        except Exception as e:
            logger.error(f"❌ Pair open error {pair}: {e}")
            return False

    def _try_add_to_position(self, pair: str, sig, z: float):
        """
        金字塔加倉：L1 開倉後，z 繼續擴大可依序觸發 L2 / L3 / L4。
        每次只升一級，確保每個門檻最多加倉一次。
        加倉計劃由模組級常數 _ADD_ON_LEVELS 驅動，新增/調整級別只需改常數。
        """
        try:
            if not self.trade_record_path.exists():
                return
            df = pd.read_csv(self.trade_record_path)
            idx = df[(df['pair'] == pair) & (df['status'] == 'OPEN')].index
            if idx.empty:
                return
            trade = df.loc[idx[0]]

            current_level = int(trade.get('entry_level', 1))
            # 已達最高級，無需再加倉
            if current_level >= len(_ADD_ON_LEVELS) + 1:
                return

            side = trade['side']

            # 找出下一個應觸發的加倉級別
            next_threshold, next_size, next_level = _ADD_ON_LEVELS[current_level - 1]

            # 加倉方向必須與原始持倉一致，且 z 須超過該級門檻
            if side == 'SHORT_SPREAD' and z < next_threshold:
                return
            if side == 'LONG_SPREAD' and z > -next_threshold:
                return

            s1, s2 = trade['s1'], trade['s2']
            s1_ccxt, s2_ccxt = self._to_ccxt(s1), self._to_ccxt(s2)
            beta = abs(float(trade['beta']))
            alloc = self.budget * next_size

            prices = self.exchange.fetch_tickers([s1_ccxt, s2_ccxt],
                                                 params={'category': 'linear'})
            p1 = prices.get(s1_ccxt, {}).get('last')
            p2 = prices.get(s2_ccxt, {}).get('last')
            if p1 is None or p2 is None:
                logger.warning(f"⚠️ L{next_level} add-on skipped for {pair}: cannot fetch prices.")
                return

            add_qty1 = float(self.exchange.amount_to_precision(s1_ccxt, alloc / p1))
            add_qty2 = float(self.exchange.amount_to_precision(s2_ccxt, (alloc * beta) / p2))

            s1_side = 'buy' if side == 'LONG_SPREAD' else 'sell'
            s2_side = 'sell' if side == 'LONG_SPREAD' else 'buy'

            logger.info(f"📈 [L{next_level} ADD] {pair} z={z:.3f} | {s1_side} {add_qty1} S1 / {s2_side} {add_qty2} S2")

            # ── 並行雙腿加倉：共用 deadline ────────────────────
            s1_ok, _, s2_ok, _ = self._execute_pair(
                s1_ccxt, s1_side, add_qty1,
                s2_ccxt, s2_side, add_qty2,
                label=f"L{next_level} ADD {pair}",
            )

            if not s1_ok and not s2_ok:
                logger.error(f"❌ L{next_level} both legs failed for {pair}. Skipping.")
                return

            if s1_ok and not s2_ok:
                logger.critical(f"🚨 L{next_level} S2 add-on failed for {pair}! Rolling back S1.")
                s1_rollback = 'sell' if s1_side == 'buy' else 'buy'
                try:
                    self.exchange.create_order(s1_ccxt, 'market', s1_rollback, add_qty1,
                                               params={'category': 'linear', 'reduceOnly': True})
                except Exception as e_rb:
                    logger.critical(f"💀 L{next_level} S1 rollback failed for {pair}: {e_rb}. Manual intervention required!")
                return

            if not s1_ok and s2_ok:
                logger.critical(f"🚨 L{next_level} S1 add-on failed for {pair}! Rolling back S2.")
                s2_rollback = 'buy' if s2_side == 'sell' else 'sell'
                try:
                    self.exchange.create_order(s2_ccxt, 'market', s2_rollback, add_qty2,
                                               params={'category': 'linear', 'reduceOnly': True})
                except Exception as e_rb:
                    logger.critical(f"💀 L{next_level} S2 rollback failed for {pair}: {e_rb}. Manual intervention required!")
                return

            # 累加持倉數量，升級至下一 level
            df['l2_entry_time'] = df['l2_entry_time'].astype(object)
            df.loc[idx[0], 'qty1']          = float(trade['qty1']) + add_qty1
            df.loc[idx[0], 'qty2']          = float(trade['qty2']) + add_qty2
            df.loc[idx[0], 'entry_level']   = next_level
            df.loc[idx[0], 'l2_entry_z']    = round(z, 4)
            df.loc[idx[0], 'l2_entry_time'] = datetime.now(timezone.utc).isoformat()
            df.to_csv(self.trade_record_path, index=False)
            logger.success(f"✅ L{next_level} add-on done for {pair} | z={z:.3f} | +qty1={add_qty1} +qty2={add_qty2}")

        except Exception as e:
            logger.error(f"❌ _try_add_to_position failed for {pair}: {e}")

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