import schedule
import time
import pandas as pd
import os
import gc
from pathlib import Path
from loguru import logger
from datetime import datetime

# 導入核心模組 (確保 core 目錄下有 __init__.py)
from core.mkt_scan import MarketScanner
from core.pair_screen import PairCombine
from core.pair_monitor import PairMonitor
from utils.execution import ExecutionManager

# ==========================================
# 🛰️ 戰術配置中心
# ==========================================
NUM_COINS         = 24     # 掃描前 24 名流動性幣種
BUDGET_PER_PAIR   = 110.0  # Paper trade 資金（正式上線後改回 150.0）
RUN_INTERVAL_MINS = 60     # 每 60 分鐘運行一次（配合 1h K 線）

ROOT = Path(__file__).resolve().parent
TRADE_RECORD_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'


def get_active_info():
    """
    [v3.2.1 強化] 提取持倉 Pair 名單與所涉及的單幣名單
    確保這些幣種在 MarketScanner 中不會被遺漏
    """
    if not TRADE_RECORD_PATH.exists():
        return [], []
    try:
        df = pd.read_csv(TRADE_RECORD_PATH)
        if df.empty: return [], []
        # 僅獲取狀態為 OPEN 的配對
        active_rows = df[df['status'] == 'OPEN']
        active_pairs = active_rows['pair'].unique().tolist()
        # 提取所有涉及的幣種 (例如 DOGEUSDT, BTCUSDT)
        active_coins = list(set(active_rows['s1'].tolist() + active_rows['s2'].tolist()))
        return active_pairs, active_coins
    except Exception as e:
        logger.error(f"❌ Failed to read active trade records: {e}")
        return [], []


def frequent_tactical_check():
    """
    🛰️ 核心任務：每 5 分鐘執行的戰術掃描
    包含：市場探測 -> 數據更新 -> 共整合測試 -> 倉位監控與強制平倉
    """
    start_time = time.time()
    logger.info(f"🚀 [{datetime.now().strftime('%H:%M:%S')}] Starting {RUN_INTERVAL_MINS}-minute tactical check...")

    try:
        # 1. 獲取當前持倉信息 (確保 Expired 檢查有名單)
        active_pairs, active_coins = get_active_info()

        # 2. 市場探測 + OHLCV 下載：Top N 流動性幣種，並強制刷新持倉幣種數據
        ms = MarketScanner()
        top_coins = ms.get_top_volume_coins(num_coins=NUM_COINS, force_include=active_coins)

        # 合併清單傳給篩選器（數據已在上一步全部更新）
        full_scan_list = list(set(top_coins + active_coins))
        logger.info(f"🛡️ Guardian Mode: Scanning {len(full_scan_list)} symbols in total.")

        # 3. 共整合篩選 (對數空間轉換與 P-Value 測試)
        pc = PairCombine()
        df_screened = pc.pair_screener(full_scan_list, timeframe='1h', active_pairs=active_pairs)
        if df_screened.empty:
            logger.warning("⚠️ pair_screener 無結果，本輪跳過監控與執行。")
            return

        # 4. 配對監控 (PairMonitor)：計算即時 Z-Score，檢查 P-Value 是否失效
        pm = PairMonitor()
        pm.check_all_pairs()

        # 5. 執行管理 (ExecutionManager)：根據最新 Z-Score 進行開平倉
        em = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)
        em.execute_trades()

        duration = time.time() - start_time
        logger.success(f"✅ Tactical check completed in {duration:.1f}s | Next scan in {RUN_INTERVAL_MINS} min.")

        # 記憶體清理，防止長時間運行洩漏
        gc.collect()

    except Exception as e:
        logger.critical(f"🚨 Tactical check crashed: {e}")


# ==========================================
# 🛰️ 主程序啟動器
# ==========================================
if __name__ == "__main__":
    logger.info(f"🛰️ Stat-Arb Guardian System Online | Frequency: Every {RUN_INTERVAL_MINS} Minutes")

    # 啟動時立即執行第一次檢查
    frequent_tactical_check()

    # 設定排程
    schedule.every(RUN_INTERVAL_MINS).minutes.do(frequent_tactical_check)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning("🛑 System manual shutdown requested. Closing fleet operations...")