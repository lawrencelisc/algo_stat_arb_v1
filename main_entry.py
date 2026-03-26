import schedule
import time
import pandas as pd
from pathlib import Path
from loguru import logger
from datetime import datetime

# 導入核心模組
from core.mkt_scan import MarketScanner
from core.pair_screen import PairCombine
from core.pair_monitor import PairMonitor
from utils.execution import ExecutionManager

# ==========================================
# 🛰️ 戰術配置中心
# ==========================================
NUM_COINS = 20
BUDGET_PER_PAIR = 1500.0
BYPASS_WEEKLY = True

ROOT = Path(__file__).resolve().parent
TRADE_RECORD_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'


def get_active_info():

    """
    [v3.1.3 強化] 同時提取持倉 Pair 名單與所涉及的單幣名單
    """

    if not TRADE_RECORD_PATH.exists():
        return [], []
    try:
        df = pd.read_csv(TRADE_RECORD_PATH)
        if df.empty: return [], []
        active_rows = df[df['status'] == 'OPEN']
        active_pairs = active_rows['pair'].unique().tolist()
        # 提取所有涉及的幣種 (例如 DOGEUSDT, BNBUSDT...)
        active_coins = list(set(active_rows['s1'].tolist() + active_rows['s2'].tolist()))
        return active_pairs, active_coins
    except Exception as e:
        logger.error(f"❌ 讀取持倉失敗: {e}")
        return [], []


# ==========================================
# 🕒 任務 A：大搜獵 (Strategic Scan)
# ==========================================
def week_schedule():
    logger.info("📅 啟動全市場掃描與數據同步...")

    # 1. 獲取持倉信息
    active_pairs, active_coins = get_active_info()

    # 2. 市場探測：Top 20 + 持倉中的幣
    ms = MarketScanner()
    # 這裡先抓 Top 20
    top_coins = ms.get_top_volume_coins(num_coins=NUM_COINS)

    # [核心修正]：將持倉幣種合併進掃描名單，確保它們一定有數據下載
    full_scan_list = list(set(top_coins + active_coins))
    logger.info(f"🛡️ 最終掃描清單共 {len(full_scan_list)} 個幣種 (含持倉守護)。")

    # 3. 下載數據 (MarketScanner 內部會處理數據下載)
    # 如果 MarketScanner 沒有自動下載，請確保 full_scan_list 被正確傳遞

    # 4. 配對篩選 (傳入 active_pairs 確保強制計算 Z)
    pc = PairCombine()
    pc.pair_screener(full_scan_list, timeframe='1h', active_pairs=active_pairs)
    logger.success("✅ 數據與 Z-Score 已同步更新。")


# ==========================================
# 🚀 其餘邏輯保持不變 (hourly_zscore_check, position_maintenance)
# ==========================================
def hourly_zscore_check():
    pm = PairMonitor()
    pm.check_all_pairs()
    em = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)
    em.process_signals()


def position_maintenance():
    em = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)
    em.reconcile_positions()


if __name__ == "__main__":
    logger.info(f"🚢 Stat-Arb v3.1.3 指揮塔啟動")
    if BYPASS_WEEKLY:
        week_schedule()
        hourly_zscore_check()

    schedule.every().monday.at("04:00").do(week_schedule)
    schedule.every().hour.at(":01").do(hourly_zscore_check)
    schedule.every(1).minutes.do(position_maintenance)

    while True:
        schedule.run_pending()
        time.sleep(1)