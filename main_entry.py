import schedule
import time
import pandas as pd
from pathlib import Path
from loguru import logger
from datetime import datetime

# 導入核心戰鬥模組
from core.mkt_scan import MarketScanner
from core.pair_screen import PairCombine
from core.pair_monitor import PairMonitor
from utils.execution import ExecutionManager

# ==========================================
# 🛰️ 戰術配置中心
# ==========================================
NUM_COINS = 20  # 監控成交量前 20 的幣種
BUDGET_PER_PAIR = 1500.0  # 艦長指示：每對注碼約 1500U
BYPASS_WEEKLY = True  # 啟動時是否立即執行大掃描

ROOT = Path(__file__).resolve().parent
TRADE_RECORD_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'


def get_active_pairs_list():
    """
    從帳本中提取當前『持倉中』的組合，交給掃描器強制監控，確保有始有終。
    """

    VERSION = "v3.1.3-Stable"

    if not TRADE_RECORD_PATH.exists():
        return []
    try:
        df = pd.read_csv(TRADE_RECORD_PATH)
        if df.empty: return []
        # 提取所有處於 OPEN 狀態的 pair 名稱
        active_list = df[df['status'] == 'OPEN']['pair'].unique().tolist()
        return active_list
    except Exception as e:
        logger.error(f"❌ 讀取持倉紀錄失敗: {e}")
        return []


# ==========================================
# 🕒 任務 A：每週/啟動大搜獵 (Strategic Scan)
# ==========================================
def week_schedule():
    logger.info("📅 [WEEKLY] 啟動全市場共整合深度篩選...")

    # 1. 市場探測
    ms = MarketScanner()
    coin_list = ms.get_top_volume_coins(num_coins=NUM_COINS)

    # 2. 配對篩選 (傳入 active_pairs 以防 Wait Scan)
    pc = PairCombine()
    active_pairs = get_active_pairs_list()

    if active_pairs:
        logger.info(f"🛡️ 偵測到 {len(active_pairs)} 組持倉單位，已下令掃描器強制監控。")

    pc.pair_screener(coin_list, timeframe='1h', active_pairs=active_pairs)
    logger.success("✅ 獵物清單 (master_research_log.csv) 已更新。")


# ==========================================
# 🕒 任務 B：每小時雷達監控 (Tactical Monitor)
# ==========================================
def hourly_zscore_check():
    logger.info("📡 [HOURLY] 雷達啟動：巡邏 Z-Score 偏離情況...")

    # 1. 計算實時 Z-Score 並產生訊號
    pm = PairMonitor()
    pm.check_all_pairs()

    # 2. 執行引擎處理 PENDING 指令 (開倉)
    em = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)
    em.process_signals()

    logger.info("💓 巡邏結束，系統心跳正常。")


# ==========================================
# 🕒 任務 C：每分鐘持倉守護 (Maintenance - 有始有終的核心)
# ==========================================
def position_maintenance():
    """
    [v3.1.3 強化] 極速監控：執行自動化撤退協議。
    這是真正防止「短炒變長揸」的實戰邏輯。
    """
    active_pairs = get_active_pairs_list()
    if not active_pairs:
        return

    logger.info(f"🛡️ [MAINTENANCE] 正在檢查 {len(active_pairs)} 組活躍頭寸...")

    # 執行引擎獲取當前帳戶權益與持倉
    em = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)

    # 1. 自動對帳 (Reconciliation)
    # 確保本地 CSV 與 Bybit 實時持倉 100% 同步
    em.reconcile_positions()

    # 2. 獲利與安全巡檢 (Profit Guard & Exit Checks)
    # 這裡會遍歷持倉，自動執行 check_time_exit 等邏輯
    # (註：em 內部已封裝自動平倉機制)
    try:
        # 獲取總權益以檢查 Profit Guard
        bal_data = em.exchange.fetch_balance()
        current_equity = float(bal_data['info']['result']['list'][0]['totalEquity'])
        em.check_profit_guard(current_equity)
    except Exception as e:
        logger.error(f"❌ 每分鐘安全巡檢異常: {e}")


# ==========================================
# 🚀 系統啟動引擎
# ==========================================
if __name__ == "__main__":
    logger.info(f"🚢 Stat-Arb v3.1.3 指揮塔正式啟動 | 注碼: {BUDGET_PER_PAIR}U")

    # 啟動自覺性掃描：一進場先做一次全體檢閱
    if BYPASS_WEEKLY:
        week_schedule()
        hourly_zscore_check()

    # 排程設定
    schedule.every().monday.at("04:00").do(week_schedule)  # 每週一凌晨更新獵場
    schedule.every().hour.at(":01").do(hourly_zscore_check)  # 每小時第 1 分鐘巡邏訊號
    schedule.every(1).minutes.do(position_maintenance)  # 每分鐘監控「有始有終」

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.warning("🛑 艦長手動終止指揮塔。")
            break
        except Exception as e:
            logger.error(f"🚨 指揮塔運行異常: {e}")
            time.sleep(10)