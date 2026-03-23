import schedule
import time
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from loguru import logger
from pathlib import Path

# ==========================================
# 🛰️ 路徑自癒邏輯 (Path Auto-Heal)
# 確保根目錄被加入到系統路徑，防止 Import 子模組失敗
# ==========================================
current_dir = Path(__file__).resolve().parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

# --- 引入核心與工具模組 (Modular Imports) ---
from core.mkt_scan import MarketScanner
from core.pair_screen import PairCombine
from core.pair_monitor import PairMonitor
from utils.tg_wrapper import TelegramReporter
from utils.execution import ExecutionManager

# ==========================================
# Configuration Area (指揮塔配置中心)
# ==========================================
# 策略週期與排程配置
WEEKLY_RUN_DAY = 'monday'
WEEKLY_RUN_TIME = '04:00'
DAILY_REPORT_TIME = '08:05'  # 每日戰報時間 (Bybit 資金費結算後)

# 市場掃描與研究參數
num_coins = 20  # [STAGE 6] 監控交易量前 20 名幣種 (約 190 對組合)
days_back = 41  # 回溯數據量 (約一個半月)
timeframe = '1h'  # 策略週期為 1 小時

# 執行與風險參數
budget_per_pair = 100  # 每對組合開倉預算 (USDT)
MAX_DRAWDOWN = 0.05  # 緊急斷路門檻 (帳戶權益回撤 5% 觸發)


# ==========================================
# Task 1: Hourly Z-Score Monitoring & Execution
# ==========================================
def hourly_zscore_check():
    """
    每小時巡邏任務：斷路檢查 -> 自動對帳 -> 監控雷達 -> 執行開倉
    """
    try:
        current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"[HOURLY] Launching patrol cycle at (UTC) {current_time}")

        # 🚀 初始化執行引擎
        executor = ExecutionManager(budget_per_pair)

        # 🛑 第一步：緊急斷路器檢查 (Kill Switch)
        # 實時監控 UTA 帳戶總權益，若低於安全線則強制平倉並停機
        if executor.check_kill_switch(max_drawdown=MAX_DRAWDOWN):
            logger.critical("🚨 Kill Switch activated! Patrol suspended for safety.")
            return

        # 🔍 第二步：自動對帳 (Reconciliation)
        # 比對 Bybit 真實持倉與本地紀錄，修復漏單或外部手動操作
        executor.reconcile_positions()

        # 🛡️ 第三步：環境檢查
        research_file = current_dir / 'result' / 'master_research_log.csv'
        if not research_file.exists():
            logger.warning("⚠️ Research log not found. Triggering immediate research...")
            week_schedule()

        # 🎯 第四步：監控雷達 (Monitoring)
        # 抓取現價計算 Z-Score 並進行 Rolling Beta 漂移檢測
        monitor = PairMonitor()
        monitor.check_all_pairs()

        # 🔥 第五步：處理執行 (Execution)
        # 讀取信號表，經過 Funding Guard 過濾利息成本後落單
        executor.process_signals()

        logger.info("[HOURLY] Patrol cycle completed successfully.")

    except Exception as e:
        logger.error(f"❌ Error in hourly_zscore_check: {e}")


# ==========================================
# Task 2: Weekly Market Research (The Scouter)
# ==========================================
def week_schedule():
    """
    每週大掃描任務：重新評估全市場流動性與共整合組合
    """
    try:
        logger.info("📅 [WEEKLY] Starting global research sequence...")

        # 1. 市場掃描：抓取 Top 20 交易量標的
        ms = MarketScanner()
        coin_list = ms.get_top_volume_coins(num_coins, days_back, timeframe)

        if not coin_list:
            logger.error("❌ [WEEKLY] Failed to retrieve coin list. Research aborted.")
            return

        # 2. 統計分析：篩選共整合組合並更新 master_research_log
        pa = PairCombine()
        pa.pair_screener(coin_list, timeframe)

        logger.success("✅ [WEEKLY] Research finished. Market hunters list updated.")

    except Exception as e:
        logger.error(f"❌ Critical error in week_schedule: {e}")


# ==========================================
# Task 3: Daily Performance Accounting (The Accountant)
# ==========================================
def run_daily_accounting():
    """
    每日精密結算：抓取真實帳目流水 (包含手續費與資金費)
    """
    try:
        logger.info("📊 [ACCOUNTING] Generating precise daily performance report...")

        # 呼叫執行引擎，透過 fetch_ledger 獲取真實財務數據
        executor = ExecutionManager(budget_per_pair)
        stats = executor.get_daily_stats()

        if stats:
            tg = TelegramReporter()
            # 將毛利、手續費、資金費與勝率發送到 Telegram
            tg.send_daily_report(
                total_pnl=stats['gross_pnl'],
                fees=stats['fees'],
                funding=stats['funding'],
                win_rate=stats['win_rate'],
                active_count=stats['count']
            )
            logger.success(f"📊 Precise Daily Report sent. Net: {stats['net_pnl']} USDT")
        else:
            logger.warning("⚠️ No accounting data available for the last 24h.")

    except Exception as e:
        logger.error(f"❌ Daily accounting task failed: {e}")


# ==========================================
# Main Execution Entry (總指揮塔啟動區)
# ==========================================
if __name__ == '__main__':
    logger.info('🛰️ Statistical Arbitrage System v2.1 Starting...')

    # --- [BOOT] 啟動即開戰邏輯 (Startup Execution) ---
    # 無論當前排程，程式開啟瞬間立刻執行一次全系統巡航
    INITIAL_BOOT = True

    if INITIAL_BOOT:
        logger.warning('🚀 [BOOT] Executing INITIAL STARTUP SEQUENCE...')

        # 1. 立即通報重啟成功
        try:
            tg = TelegramReporter()
            tg.send_heartbeat(pnl=0, active_pairs=0, uptime="System Rebooted & Initializing...")
        except:
            pass

        # 2. 立即進行市場研究 (確保數據最新)
        week_schedule()

        # 3. 立即進行第一次巡邏 (檢測是否有開倉機會)
        hourly_zscore_check()

        # 4. 關閉啟動旗標，交接給排程器
        INITIAL_BOOT = False
        logger.info('✅ [BOOT] Initial sequence complete. Transitioning to Scheduled Mode.')

    # ==========================================
    # 📅 設定排程排班 (Scheduler Configuration)
    # ==========================================

    # A. 每週大掃描排程 (預設週一凌晨 04:00)
    try:
        day_func = getattr(schedule.every(), WEEKLY_RUN_DAY.lower())
        day_func.at(WEEKLY_RUN_TIME).do(week_schedule)
        logger.info(f"📅 Schedule: Weekly Scan on {WEEKLY_RUN_DAY} at {WEEKLY_RUN_TIME}")
    except AttributeError:
        logger.error(f"❌ Invalid WEEKLY_RUN_DAY: {WEEKLY_RUN_DAY}")

    # B. 每日精密戰報 (預設 08:05)
    schedule.every().day.at(DAILY_REPORT_TIME).do(run_daily_accounting)
    logger.info(f"📊 Schedule: Daily Accounting at {DAILY_REPORT_TIME}")

    # C. 每小時巡邏 (於每小時第 01 分鐘)
    schedule.every().hour.at(":01").do(hourly_zscore_check)
    logger.info("📡 Schedule: Hourly Patrol active.")

    # --- 主心跳循環 ---
    logger.info('🚀 System heartbeat engaged. All defenses active.')
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning('🛑 System manually terminated by Captain.')