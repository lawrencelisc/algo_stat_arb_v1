import schedule
import time
import pandas as pd
from pathlib import Path
from loguru import logger
from datetime import datetime

# 引入核心模組
from core.mkt_scan import MarketScanner
from core.pair_screen import PairCombine
from core.pair_monitor import PairMonitor
from utils.execution import ExecutionManager
from utils.tg_wrapper import TelegramReporter

# ==========================================
# 🛰️ 戰術配置中心
# ==========================================
BUDGET_PER_PAIR = 100.0  # 每對組合預算
DRAWDOWN_LIMIT = 100.0  # 利潤回撤容忍額度 (Strategy 2)
TRADE_LOG = Path("data/trade/trade_record.csv")

# 初始化組件
scanner = MarketScanner()
screener = PairCombine()
monitor = PairMonitor()
executor = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)
tg = TelegramReporter()


def hourly_zscore_check():
    """
    每小時巡邏任務：監控 Z-Score 並執行風險管理優化。
    整合：Strategy 1 (Time Exit) 與 Strategy 2 (Profit Guard)。
    """
    logger.info("🛰️ 定時巡邏啟動：正在掃描獵場與執行風險審計...")

    try:
        # ---------------------------------------------------------
        # 🛡️ [OPTIMIZATION 2] Profit Guard (利潤斷路器)
        # ---------------------------------------------------------
        # 先獲取帳戶總權益
        bal = executor.exchange.fetch_balance()
        # Bybit UTA 帳戶權益讀取
        current_equity = float(bal['info']['result']['list'][0]['totalEquity'])

        # 檢查是否觸發高水位線回撤 (100 USDT)
        if executor.check_profit_guard(current_equity, drawdown_limit=DRAWDOWN_LIMIT):
            tg.send_error_alert("PROFIT GUARD TRIGGERED", "MainTower",
                                f"Equity {current_equity}U dropped from peak. Panic Close All executed.")
            return  # 若觸發全場平倉，本次巡邏結束

        # ---------------------------------------------------------
        # ⏳ [OPTIMIZATION 1] Time-based Exit (超時平倉)
        # ---------------------------------------------------------
        if TRADE_LOG.exists():
            df_trades = pd.read_csv(TRADE_LOG)
            # 找出目前標記為 OPEN 的倉位
            open_trades = df_trades[df_trades['status'] == 'OPEN']

            for idx, row in open_trades.iterrows():
                pair = row['pair']

                # 獲取該部位的實時未實現盈虧 (用於判斷是否獲利中)
                # 這裡調用 Bybit API 獲取部位資訊
                positions = executor.exchange.fetch_positions(params={'category': 'linear'})
                # 找到對應的標的 (以 S1 作為代表檢查)
                pos_info = next((p for p in positions if p['symbol'].replace(':USDT', '') == row['s1']), None)
                unrealized_pnl = float(pos_info['unrealizedPnl']) if pos_info else 0.0

                # 執行超時檢查：持有時間 > 3 * Half-life 且 PnL > 0
                if executor.check_time_exit(pair, row['entry_time'], row['opening_half_life'], unrealized_pnl):
                    # 執行該部位平倉
                    executor._emergency_market_close()  # 這裡建議使用針對單一對組合的平倉函數，目前先用 emergency
                    tg.send_heartbeat(0.0, 0, f"⏳ Time-Exit executed for {pair}. Profit secured.")

        # ---------------------------------------------------------
        # 🎯 標規動作：Z-Score 監控與落單執行
        # ---------------------------------------------------------
        # 1. 雷達掃描現價，並產出信號到 signal_table.csv
        monitor.check_all_pairs()

        # 2. 執行引擎讀取信號並執行下單
        executor.process_signals()

        # 3. 定期對帳，確保與交易所同步
        executor.reconcile_positions()

    except Exception as e:
        logger.error(f"❌ 巡邏任務發生錯誤: {e}")
        tg.send_error_alert(str(e), "hourly_zscore_check")


def week_schedule():
    """每週大掃描：重新篩選共整合組合"""
    logger.info("📅 每週戰略研究啟動：更新獵物清單...")
    try:
        scanner.scan_market()
        screener.screen_pairs()
        logger.success("✅ 獵物清單 (master_research_log.csv) 已更新。")
    except Exception as e:
        logger.error(f"❌ 每週掃描失敗: {e}")


# ==========================================
# 🚀 啟動調度引擎
# ==========================================
if __name__ == "__main__":
    logger.info("🚢 Stat-Arb v2.1 潛艇離港，正在啟動自動化指揮系統...")

    # [SCO FIX] 啟動時立即執行一次，確保不漏掉當前機會
    week_schedule()
    hourly_zscore_check()

    # 每週一凌晨 04:00 執行大掃描
    schedule.every().monday.at("04:00").do(week_schedule)

    # 每小時執行一次 Z-Score 巡邏與風險優化
    schedule.every().hour.at(":01").do(hourly_zscore_check)

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.warning("🛑 艦長手動中止程式。")
            break