import schedule
import time
import pandas as pd
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
NUM_COINS            = 24     # 掃描前 24 名流動性幣種
BUDGET_PER_PAIR      = 110.0  # Paper trade 資金（正式上線後改回 150.0）
RESEARCH_INTERVAL    = 60     # 慢循環：重新篩選配對（分鐘，配合 1h K 線）
EXECUTION_INTERVAL   = 5      # 快循環：重算 Z-Score + 執行交易（分鐘）

ROOT = Path(__file__).resolve().parent
TRADE_RECORD_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'
LOG_FILEPATH      = ROOT / 'result' / 'master_research_log.csv'


def get_active_info():
    """
    提取持倉 Pair 名單與所涉及的單幣名單
    確保這些幣種在 MarketScanner 中不會被遺漏
    """
    if not TRADE_RECORD_PATH.exists():
        return [], []
    try:
        df = pd.read_csv(TRADE_RECORD_PATH)
        if df.empty: return [], []
        active_rows = df[df['status'] == 'OPEN']
        active_pairs = active_rows['pair'].unique().tolist()
        active_coins = list(set(active_rows['s1'].tolist() + active_rows['s2'].tolist()))
        return active_pairs, active_coins
    except Exception as e:
        logger.error(f"❌ Failed to read active trade records: {e}")
        return [], []


# ==========================================
# 🔬 慢循環：每 60 分鐘
# 職責：下載 OHLCV、跑共整合篩選、更新 master_research_log
# ==========================================
def research_cycle():
    start_time = time.time()
    logger.info(f"🔬 [{datetime.now().strftime('%H:%M:%S')}] Research cycle starting...")
    try:
        active_pairs, active_coins = get_active_info()

        ms = MarketScanner()
        top_coins = ms.get_top_volume_coins(num_coins=NUM_COINS, force_include=active_coins)

        full_scan_list = list(set(top_coins + active_coins))
        logger.info(f"🛡️ Guardian Mode: Scanning {len(full_scan_list)} symbols in total.")

        pc = PairCombine()
        df_screened = pc.pair_screener(full_scan_list, timeframe='1h', active_pairs=active_pairs)

        if df_screened.empty:
            logger.warning("⚠️ pair_screener 無結果，master_research_log 未更新。")
        else:
            logger.success(f"✅ Research cycle done in {time.time() - start_time:.1f}s | "
                           f"{len(df_screened)} pairs logged.")
    except Exception as e:
        logger.critical(f"🚨 Research cycle crashed: {e}")
    finally:
        gc.collect()


# ==========================================
# ⚡ 快循環：每 5 分鐘
# 職責：用即時報價重算 Z-Score，執行開平倉
# 前提：master_research_log 存在（由慢循環維護）
# ==========================================
def execution_cycle():
    start_time = time.time()
    logger.info(f"⚡ [{datetime.now().strftime('%H:%M:%S')}] Execution cycle starting...")
    try:
        # master_research_log 未就緒時，快循環無法運行，靜默跳過
        if not LOG_FILEPATH.exists():
            logger.warning("⚠️ master_research_log not found. Execution cycle skipped.")
            return

        pm = PairMonitor()
        pm.check_all_pairs()

        em = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)
        em.execute_trades()

        logger.success(f"⚡ Execution cycle done in {time.time() - start_time:.1f}s.")
    except Exception as e:
        logger.critical(f"🚨 Execution cycle crashed: {e}")
    finally:
        gc.collect()


# ==========================================
# 🛰️ 主程序啟動器
# ==========================================
if __name__ == "__main__":
    logger.info(
        f"🛰️ Stat-Arb Guardian System Online | "
        f"Research: {RESEARCH_INTERVAL}min | Execution: {EXECUTION_INTERVAL}min"
    )

    # 啟動時先跑完整慢循環，確保 master_research_log 就緒，才讓快循環有數據可讀
    research_cycle()
    execution_cycle()

    schedule.every(RESEARCH_INTERVAL).minutes.do(research_cycle)
    schedule.every(EXECUTION_INTERVAL).minutes.do(execution_cycle)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning("🛑 System manual shutdown requested. Closing fleet operations...")
