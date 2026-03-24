import schedule
import time
import pandas as pd
import sys
from pathlib import Path
from loguru import logger
from datetime import datetime

# --- 路徑自愈邏輯 ---
# 確保系統能正確識別 core/utils 目錄
root_path = Path(__file__).resolve().parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

# 引入核心戰術模組
try:
    from core.mkt_scan import MarketScanner
    from core.pair_screen import PairCombine
    from core.pair_monitor import PairMonitor
    from utils.execution import ExecutionManager
    from utils.tg_wrapper import TelegramReporter
except ImportError as e:
    logger.error(f"❌ Failed to load modules, please check directory structure: {e}")
    sys.exit(1)

# ==========================================
# 🛰️ 戰術配置中心 v2.4.0-Stable
# ==========================================
VERSION = "v2.4.0-Stable"
BUDGET_PER_PAIR = 100.0  # 每對組合預算 (USDT)
DRAWDOWN_LIMIT = 100.0  # 總賬戶回撤保護 (HWM 斷路器)
TRADE_LOG = Path("data/trade/trade_record.csv")

# 初始化自動化組件
scanner = MarketScanner()
screener = PairCombine()
monitor = PairMonitor()
executor = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)
tg = TelegramReporter()


def high_frequency_risk_check():
    """
    [手術 3] 高頻風險巡邏 (每 10 分鐘執行)：
    專門監控 Trailing TP、Time-Exit 與 Profit Guard，確保及時鎖定利潤，防止 PnL 大幅回吐。
    """
    logger.info(f"🛡️ Starting high-frequency risk check (v{VERSION})...")
    try:
        # 1. 檢查 Profit Guard (全場總盈虧斷路器)
        bal = executor.exchange.fetch_balance()
        current_equity = float(bal['info']['result']['list'][0]['totalEquity'])

        if executor.check_profit_guard(current_equity, drawdown_limit=DRAWDOWN_LIMIT):
            tg.send_error_alert("PROFIT GUARD TRIGGERED", "MainTower", f"Panic Exit at {current_equity}U")
            return

            # 2. 持倉微觀監控 (檢查個別組合是否需要撤退)
        if TRADE_LOG.exists():
            df_trades = pd.read_csv(TRADE_LOG)
            open_trades = df_trades[df_trades['status'] == 'OPEN']

            if not open_trades.empty:
                # 獲取最新即時價格
                all_symbols = list(set(open_trades['s1'].tolist() + open_trades['s2'].tolist()))
                current_prices = monitor.fetch_latest_prices(all_symbols)

                for idx, row in open_trades.iterrows():
                    pair = row['pair']
                    s1, s2 = row['s1'], row['s2']

                    if s1 in current_prices and s2 in current_prices:
                        # 即時計算 Z-Score (使用開倉時記錄的統計參數)
                        p1, p2 = current_prices[s1], current_prices[s2]
                        beta = float(row['beta'])
                        alpha = float(row.get('alpha', 0))
                        std = float(row.get('spread_std', 0.01))

                        z_score = (p1 - (beta * p2 + alpha)) / std

                        # A. 檢查追蹤止盈 (Trailing Take Profit)
                        if executor.check_trailing_tp(pair, z_score):
                            executor.close_specific_pair(pair, reason="TRAILING_TP")
                            continue

                        # B. 檢查超時平倉 (Time-based Exit)
                        half_life = row.get('opening_half_life', 8.0)
                        try:
                            # 獲取該組合第一條腿的未實現盈虧作為平倉條件之一
                            pos_data = executor.exchange.fetch_position(f"{s1.replace('USDT', '')}/USDT:USDT")
                            u_pnl = float(pos_data['unrealizedPnl']) if pos_data else 0.0

                            if executor.check_time_exit(pair, row['entry_time'], half_life, u_pnl):
                                executor.close_specific_pair(pair, reason="TIME_EXIT")
                        except Exception as e:
                            logger.error(f"⚠️ Unable to fetch position details for {pair}: {e}")

    except Exception as e:
        logger.error(f"❌ High-frequency risk check exception: {e}")


def hourly_routine():
    """每小時常規任務：監控信號、執行落單、對帳"""
    logger.info(f"🛰️ Starting hourly routine (v{VERSION})...")
    try:
        # 1. 檢查所有監控中的 Pairs
        monitor.check_all_pairs()
        # 2. 處理 PENDING 狀態的信號
        executor.process_signals()
        # 3. 執行本機與交易所的自動對帳
        executor.reconcile_positions()
    except Exception as e:
        logger.error(f"❌ Hourly routine execution failed: {e}")


def week_schedule():
    """每週大掃描：重新計算全市場相關性與共整合關係"""
    logger.info(f"📅 Starting weekly strategic scan (v{VERSION})...")
    try:
        # 1. 抓取流動性前 24 名的幣種
        top_coins = scanner.get_top_volume_coins(num_coins=24)

        # 2. 如果成功獲得名單，交給篩選器進行運算
        if top_coins and len(top_coins) > 0:
            logger.info(f"🔄 Processing cross-cointegration calculation for {len(top_coins)} coins...")
            screener.pair_screener(coin_list=top_coins, timeframe='1h')
            logger.success("✅ Weekly scan and watchlist update completed.")
        else:
            logger.warning("⚠️ Failed to fetch valid coin list, skipping this screening.")

    except Exception as e:
        logger.error(f"❌ Weekly scan error: {e}")


# ==========================================
# 🚀 啟動自動化指揮流程
# ==========================================
if __name__ == "__main__":
    logger.info(f"🚢 Stat-Arb {VERSION} departing, entering [Vigilance Cruise Mode]")

    # 啟動初始化程序
    week_schedule()
    hourly_routine()
    high_frequency_risk_check()

    # --- 排程設定 ---
    # 1. 每週一凌晨 04:00 更新獵物名單
    schedule.every().monday.at("04:00").do(week_schedule)

    # 2. 每小時的第 01 分鐘執行交易檢查
    schedule.every().hour.at(":01").do(hourly_routine)

    # 3. 每 10 分鐘執行一次高頻風險守衛 (解決 PnL 回吐的關鍵)
    schedule.every(10).minutes.do(high_frequency_risk_check)

    logger.info(f"✅ (BOOT) Command system is running. Version: {VERSION}")

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logger.warning("🛑 Captain manually aborted the command system.")
            break
        except Exception as e:
            logger.error(f"🚨 System heartbeat exception: {e}")
            time.sleep(10)