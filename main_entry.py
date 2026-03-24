import schedule
import time
import pandas as pd
import sys
from pathlib import Path
from loguru import logger
from datetime import datetime

# --- 路徑自愈邏輯 ---
root_path = Path(__file__).resolve().parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

# 引入核心模組
try:
    from core.mkt_scan import MarketScanner
    from core.pair_screen import PairCombine
    from core.pair_monitor import PairMonitor
    from utils.execution import ExecutionManager
    from utils.tg_wrapper import TelegramReporter
except ImportError as e:
    logger.error(f"❌ 模組載入失敗，請檢查目錄結構: {e}")
    sys.exit(1)

# ==========================================
# 🛰️ 戰術配置中心 v2.3-Stable
# ==========================================
VERSION = "v2.3.0-Stable"
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
    """每小時巡邏任務：監控 Z-Score 並執行風險管理優化"""
    logger.info(f"🛰️ 定時巡邏啟動 (Main {VERSION})：正在掃描獵場與執行風險審計...")

    try:
        # ---------------------------------------------------------
        # 🛡️ [OPTIMIZATION 2] Profit Guard (利潤斷路器)
        # ---------------------------------------------------------
        bal = executor.exchange.fetch_balance()
        current_equity = float(bal['info']['result']['list'][0]['totalEquity'])

        if executor.check_profit_guard(current_equity, drawdown_limit=DRAWDOWN_LIMIT):
            tg.send_error_alert("PROFIT GUARD TRIGGERED", "MainTower",
                                f"Equity {current_equity}U dropped. Panic Close executed.")
            return  # 若觸發全場平倉，本次巡邏結束

        # ---------------------------------------------------------
        # ⏳ [OPTIMIZATION 1] Time-based Exit (超時平倉)
        # ---------------------------------------------------------
        if TRADE_LOG.exists():
            try:
                df_trades = pd.read_csv(TRADE_LOG)
                open_trades = df_trades[df_trades['status'] == 'OPEN']

                if not open_trades.empty:
                    logger.info(f"🔎 正在檢查 {len(open_trades)} 個活動持倉的持有時間...")
                    positions = executor.exchange.fetch_positions(params={'category': 'linear'})

                    for idx, row in open_trades.iterrows():
                        pair = row['pair']

                        # ⚠️ 防呆機制：相容舊帳本，如果沒有 opening_half_life 就預設為 8.0
                        half_life = row.get('opening_half_life', 8.0)
                        if pd.isna(half_life):
                            half_life = 8.0

                        pos_info = next((p for p in positions if p['symbol'].replace(':USDT', '') == row['s1']), None)
                        unrealized_pnl = float(pos_info['unrealizedPnl']) if pos_info else 0.0

                        # 檢查是否超時
                        if executor.check_time_exit(pair, row['entry_time'], float(half_life), unrealized_pnl):
                            # ✅ v2.3 精準平倉：只平嗰一對，唔會誤傷其他！
                            success = executor.close_specific_pair(pair, reason="TIME_EXIT")
                            if success:
                                tg.send_heartbeat(0.0, 0, f"⏳ Time-Exit executed for {pair}. Profit secured.")
            except pd.errors.ParserError:
                logger.error("❌ trade_record.csv 格式錯誤，可能包含新舊混雜欄位。請刪除或備份該檔案。")
            except Exception as e:
                logger.error(f"❌ Time Exit 執行異常: {e}")

        # ---------------------------------------------------------
        # 🎯 標規動作：Z-Score 監控與落單執行
        # ---------------------------------------------------------
        monitor.check_all_pairs()
        executor.process_signals()
        executor.reconcile_positions()

    except Exception as e:
        logger.error(f"❌ 巡邏任務發生錯誤: {e}")
        tg.send_error_alert(str(e), "hourly_zscore_check")


def week_schedule():
    """每週大掃描：重新篩選共整合組合"""
    logger.info("📅 每週戰略研究啟動：更新獵物清單...")
    try:
        # 1. 執行市場掃描，並獲取 top_coins 列表 (預設抓前 24 名，回溯 41 天)
        top_coins = scanner.get_top_volume_coins(num_coins=24, days_back=41, timeframe='1h')

        # 2. 確認有成功抓到幣種清單後，將清單傳遞給共整合篩選器
        if top_coins and len(top_coins) > 0:
            logger.info(f"🔄 準備將 {len(top_coins)} 隻幣種交給 PairCombine 進行共整合運算...")
            screener.pair_screener(coin_list=top_coins, timeframe='1h')
            logger.success("✅ 獵物清單 (master_research_log.csv) 已更新。")
        else:
            logger.warning("⚠️ MarketScanner 未能回傳幣種清單，跳過共整合運算。")

    except Exception as e:
        logger.error(f"❌ 每週掃描失敗: {e}")


# ==========================================
# 🚀 啟動調度引擎
# ==========================================
if __name__ == "__main__":
    logger.info(f"🚢 Stat-Arb {VERSION} 潛艇離港，正在啟動自動化指揮系統...")

    # 啟動時執行一次
    week_schedule()
    hourly_zscore_check()

    schedule.every().monday.at("04:00").do(week_schedule)
    schedule.every().hour.at(":01").do(hourly_zscore_check)

    logger.info("✅ (BOOT) Initial sequence completed. Transitioning to Scheduled Mode.")

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.warning("🛑 艦長手動中止程式。")
            break
        except Exception as e:
            logger.error(f"🚨 系統心跳異常: {e}")
            time.sleep(10)