import schedule
import time
import pandas as pd
import sys
from pathlib import Path
from loguru import logger
from datetime import datetime

# --- Path Auto-Heal Logic ---
root_path = Path(__file__).resolve().parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

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
# 🛰️ Tactical Configuration Center v2.4.2-Stable
# ==========================================
VERSION = "v2.4.2-Stable"
BUDGET_PER_PAIR = 100.0
DRAWDOWN_LIMIT = 100.0
TRADE_LOG = Path("data/trade/trade_record.csv")

scanner = MarketScanner()
screener = PairCombine()
monitor = PairMonitor()
executor = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)
tg = TelegramReporter()


def high_frequency_risk_check():
    logger.info(f"🛡️ Starting high-frequency risk check (v{VERSION})...")
    try:
        bal = executor.exchange.fetch_balance()
        current_equity = float(bal['info']['result']['list'][0]['totalEquity'])

        if executor.check_profit_guard(current_equity, drawdown_limit=DRAWDOWN_LIMIT):
            tg.send_error_alert("PROFIT GUARD TRIGGERED", "MainTower", f"Panic Exit at {current_equity}U")
            return

        if TRADE_LOG.exists():
            df_trades = pd.read_csv(TRADE_LOG)
            open_trades = df_trades[df_trades['status'] == 'OPEN']

            if not open_trades.empty:
                all_symbols = list(set(open_trades['s1'].tolist() + open_trades['s2'].tolist()))
                current_prices = monitor.fetch_latest_prices(all_symbols)

                for idx, row in open_trades.iterrows():
                    pair = row['pair']
                    s1, s2 = row['s1'], row['s2']

                    if s1 in current_prices and s2 in current_prices:
                        p1, p2 = current_prices[s1], current_prices[s2]
                        beta = float(row['beta'])
                        alpha = float(row.get('alpha', 0))
                        std = float(row.get('spread_std', 0.01))

                        z_score = (p1 - (beta * p2 + alpha)) / std

                        if executor.check_trailing_tp(pair, z_score):
                            executor.close_specific_pair(pair, reason="TRAILING_TP")
                            continue

                        half_life = row.get('opening_half_life', 8.0)
                        try:
                            # ✅ [SCO FIX] 使用安全且帶有 Retry 機制的封裝函數獲取 PnL
                            u_pnl = executor.get_unrealized_pnl(s1)

                            if executor.check_time_exit(pair, row['entry_time'], half_life, u_pnl):
                                executor.close_specific_pair(pair, reason="TIME_EXIT")
                        except Exception as e:
                            logger.error(f"⚠️ Unable to fetch position details for {pair}: {e}")

    except Exception as e:
        logger.error(f"❌ High-frequency risk check exception: {e}")


def hourly_routine():
    logger.info(f"🛰️ Starting hourly routine (v{VERSION})...")
    try:
        monitor.check_all_pairs()
        executor.process_signals()
        executor.reconcile_positions()
    except Exception as e:
        logger.error(f"❌ Hourly routine execution failed: {e}")


def week_schedule():
    logger.info(f"📅 Starting weekly strategic scan (v{VERSION})...")
    try:
        top_coins = scanner.get_top_volume_coins(num_coins=24)
        if top_coins and len(top_coins) > 0:
            logger.info(f"🔄 Processing cross-cointegration calculation for {len(top_coins)} coins...")
            screener.pair_screener(coin_list=top_coins, timeframe='1h')
            logger.success("✅ Weekly scan and watchlist update completed.")
        else:
            logger.warning("⚠️ Failed to fetch valid coin list, skipping this screening.")
    except Exception as e:
        logger.error(f"❌ Weekly scan error: {e}")


if __name__ == "__main__":
    logger.info(f"🚢 Stat-Arb {VERSION} departing, entering [Vigilance Cruise Mode]")

    week_schedule()
    hourly_routine()
    high_frequency_risk_check()

    schedule.every().monday.at("04:00").do(week_schedule)
    schedule.every().hour.at(":01").do(hourly_routine)
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