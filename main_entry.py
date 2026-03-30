import schedule
import time
import pandas as pd
import os
import gc
from pathlib import Path
from loguru import logger
from datetime import datetime

# Import Core Modules
from core.mkt_scan import MarketScanner
from core.pair_screen import PairCombine
from core.pair_monitor import PairMonitor
from utils.execution import ExecutionManager

# ==========================================
# 🛰️ Tactical Configuration Center
# ==========================================
NUM_COINS = 24  # Scan top 24 liquid coins
BUDGET_PER_PAIR = 1500.0  # Trading budget per pair
RUN_INTERVAL_MINS = 5  # [CRITICAL] Re-scan every 5 minutes to prevent expired signals

ROOT = Path(__file__).resolve().parent
TRADE_RECORD_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'


def get_active_info():
    """
    [v3.2.1-Safety] Extract active pair names and involved symbols
    to ensure the 'Guardian Mode' scans held positions.
    """
    if not TRADE_RECORD_PATH.exists():
        return [], []
    try:
        df = pd.read_csv(TRADE_RECORD_PATH)
        if df.empty: return [], []
        active_rows = df[df['status'] == 'OPEN']
        active_pairs = active_rows['pair'].unique().tolist()
        # Extract all involved symbols (e.g., BTCUSDT, ETHUSDT)
        active_coins = list(set(active_rows['s1'].tolist() + active_rows['s2'].tolist()))
        return active_pairs, active_coins
    except Exception as e:
        logger.error(f"❌ Failed to read active trade records: {e}")
        return [], []


def frequent_tactical_check():
    """
    🛰️ Main Task: Tactical scan and position maintenance.
    Flow: Scan Market -> Update Data -> Coint Test -> Monitor Signal -> Execute Trades
    """
    start_time = time.time()
    logger.info(f"🚀 [T+{datetime.now().strftime('%M:%S')}] Starting 5-minute high-frequency tactical check...")

    try:
        # 1. Retrieve current active positions info
        active_pairs, active_coins = get_active_info()

        # 2. Market Probing: Scan Top Volume + Held Symbols (Guardian Mode)
        ms = MarketScanner()
        top_coins = ms.get_top_volume_coins(num_coins=NUM_COINS)

        # Merge lists: Ensure held coins are ALWAYS included in the scan list
        full_scan_list = list(set(top_coins + active_coins))
        logger.info(f"🛡️ Guardian Mode: Scanning {len(full_scan_list)} symbols in total.")

        # 3. Data Sync & Cointegration calculation (PairCombine)
        # Force recalculation for held pairs to check if P-Value > 0.05
        pc = PairCombine()
        pc.pair_screener(full_scan_list, timeframe='1h', active_pairs=active_pairs)

        # 4. Signal Monitoring (PairMonitor)
        # This module triggers FORCE_EXIT if cointegration relationship is broken
        pm = PairMonitor()
        pm.check_all_pairs()

        # 5. Execution Management (ExecutionManager)
        # Handle new entries or forced exits based on signal_table.csv
        em = ExecutionManager(budget_per_pair=BUDGET_PER_PAIR)
        em.execute_trades()

        duration = time.time() - start_time
        logger.success(f"✅ Tactical check completed in {duration:.1f}s | Next scan in {RUN_INTERVAL_MINS} minutes.")

        # Memory cleanup to prevent leakage during long-term operation
        gc.collect()

    except Exception as e:
        logger.critical(f"🚨 Tactical check crashed: {e}")


# ==========================================
# 🚀 System Bootstrapper
# ==========================================
if __name__ == "__main__":
    logger.info(f"🛰️ Stat-Arb Guardian System Online | Configured Frequency: Every {RUN_INTERVAL_MINS} Minutes")

    # Perform initial check immediately upon startup
    frequent_tactical_check()

    # Setup the scheduled task
    schedule.every(RUN_INTERVAL_MINS).minutes.do(frequent_tactical_check)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.warning("🛑 Interruption signal received. Safely shutting down the fleet...")