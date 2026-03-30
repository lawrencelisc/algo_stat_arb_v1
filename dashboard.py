import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import ccxt
import os
from pathlib import Path
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh

# ==========================================
# 🛰️ UI Configuration & Branding
# ==========================================
VERSION = "v3.2.1-Safety-UI"

st.set_page_config(
    page_title=f"Stat-Arb Guardian Dashboard",
    page_icon="🛰️",
    layout="wide"
)

# Custom CSS for dark mode optimization
st.markdown("""
    <style>
    .block-container { padding-top: 1rem !important; }
    [data-testid="stMetricValue"] { font-size: 1.6rem !important; }
    .stMetric {
        background-color: #161b22;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #30363d;
    }
    .status-active { color: #00ff00; font-weight: bold; }
    .status-expired { color: #ff4b4b; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# 🚀 Auto-refresh (60 seconds)
st_autorefresh(interval=60 * 1000, key="datarefresh")

# --- Path Configuration ---
ROOT = Path(__file__).resolve().parent
LOG_PATH = ROOT / 'result' / 'master_research_log.csv'
TRADE_RECORD_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'
SIGNAL_TABLE_PATH = ROOT / 'data' / 'signal' / 'signal_table.csv'


# ==========================================
# 📊 Data Loading Engines
# ==========================================

def load_research_log():
    if not LOG_PATH.exists(): return pd.DataFrame()
    try:
        df = pd.read_csv(LOG_PATH)
        return df.sort_values('timestamp', ascending=False)
    except:
        return pd.DataFrame()


def load_active_trades():
    if not TRADE_RECORD_PATH.exists(): return pd.DataFrame()
    try:
        df = pd.read_csv(TRADE_RECORD_PATH)
        return df[df['status'] == 'OPEN']
    except:
        return pd.DataFrame()


def load_signals():
    if not SIGNAL_TABLE_PATH.exists(): return pd.DataFrame()
    try:
        return pd.read_csv(SIGNAL_TABLE_PATH)
    except:
        return pd.DataFrame()


# ==========================================
# 🛰️ Main Dashboard UI
# ==========================================

st.title(f"🛰️ Stat-Arb Guardian System {VERSION}")
st.caption(f"Last UI Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (5-min Sync Cycle)")

# --- [Sidebar Controls] ---
st.sidebar.header("Command Center")
p_value_threshold = st.sidebar.slider("P-Value Threshold", 0.01, 0.10, 0.05, step=0.01)
show_active_only = st.sidebar.checkbox("Show Only Active Positions", value=False)

# Load All Data
df_log = load_research_log()
df_trades = load_active_trades()
df_signals = load_signals()

# --- [Top Metrics Row] ---
m1, m2, m3, m4 = st.columns(4)

total_pairs = len(df_log['pair'].unique()) if not df_log.empty else 0
active_count = len(df_trades)
latest_scan = df_log['timestamp'].iloc[0] if not df_log.empty else "N/A"

with m1:
    st.metric("Pairs Monitored", total_pairs)
with m2:
    st.metric("Active Positions", active_count, delta_color="normal")
with m3:
    st.metric("Scan Frequency", "5 Mins")
with m4:
    st.caption("Latest Scan Sync:")
    st.write(f"⏱️ {latest_scan[:19] if isinstance(latest_scan, str) else 'No Data'}")

# --- [Main Tabs] ---
tab1, tab2, tab3 = st.tabs(["🎯 Real-time Scanner", "📈 Active Monitor", "📜 Research History"])

with tab1:
    st.subheader("Radar: Co-integration Opportunities")
    if not df_log.empty:
        # Get latest snapshot
        latest_ts = df_log['timestamp'].max()
        df_radar = df_log[df_log['timestamp'] == latest_ts].copy()

        # Filter by P-Value
        df_radar = df_radar[df_radar['p_value'] <= p_value_threshold]

        # Merge with signals to get "Live Z"
        if not df_signals.empty:
            df_radar = df_radar.merge(df_signals[['pair', 'z_score']], on='pair', how='left', suffixes=('', '_live'))

        # Formatting for Display
        display_cols = {
            'pair': 'Pair Name',
            'last_p1': 'Price S1',
            'last_p2': 'Price S2',
            'p_value': 'P-Value',
            'correlation': 'Corr',
            'last_z_score': 'Scan Z',
            'z_score': 'Live Z',
            'is_active': 'Status'
        }

        # Map Status for visibility
        df_radar['is_active'] = df_radar['is_active'].apply(lambda x: "🟢 ACTIVE" if x else "⚪ WATCH")

        # Display table with formatting
        st.dataframe(
            df_radar[list(display_cols.keys())].rename(columns=display_cols).style.format({
                'P-Value': '{:.4f}',
                'Corr': '{:.2f}',
                'Scan Z': '{:+.2f}',
                'Live Z': '{:+.2f}',
                'Price S1': '{:.4f}',
                'Price S2': '{:.4f}'
            }).background_gradient(cmap='RdYlGn', subset=['Scan Z'], low=0.5, high=0.5),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("Waiting for first scan data...")

with tab2:
    st.subheader("Guardian: Open Positions & Co-int Health")
    if not df_trades.empty:
        # Merge trades with latest research to check P-Value survival
        latest_ts = df_log['timestamp'].max()
        df_current_research = df_log[df_log['timestamp'] == latest_ts]

        monitor_df = df_trades.merge(df_current_research[['pair', 'p_value', 'last_z_score']], on='pair', how='left')


        # Add a warning column
        def check_health(p):
            if pd.isna(p): return "❓ NO DATA"
            return "✅ HEALTHY" if p < 0.05 else "🚨 EXPIRED"


        monitor_df['Health'] = monitor_df['p_value'].apply(check_health)

        st.dataframe(
            monitor_df[
                ['pair', 'entry_time', 'side', 'entry_p1', 'entry_p2', 'p_value', 'last_z_score', 'Health']].style.map(
                lambda x: 'background-color: #4b0000; color: white;' if x == "🚨 EXPIRED" else '',
                subset=['Health']
            ),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.success("No active trades. System is standing by.")

with tab3:
    st.subheader("Deep Archive: Research Log")
    if not df_log.empty:
        st.write("Historical co-integration test results (Last 500 records)")
        st.dataframe(df_log.head(500), use_container_width=True)
    else:
        st.warning("No research logs found.")

# --- [Footer Support] ---
st.divider()
st.markdown("Designed for **United Fleet Command**. Frequency: 5m | Strategy: Log-Scale Coint-Arb.")