import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime
from streamlit_autorefresh import st_autorefresh

# ==========================================
# 🛰️ 網頁配置與縮小版面 CSS
# ==========================================
st.set_page_config(
    page_title="Stat-Arb v2.5 UI",
    page_icon="🛰️",
    layout="wide"
)

# 自定義 CSS：進一步壓縮頂部空間，讓內容更緊湊
st.markdown("""
    <style>
    /* 移除 Streamlit 預設的頂部大空白 */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 0rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    /* 縮小 Metric 卡片的大小 */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
    }
    .stMetric {
        background-color: #1e2130;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #3e4259;
    }
    /* 移除底部多餘空白 */
    footer {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)  # ✅ [FIX] 修正參數名稱為 unsafe_allow_html

# 🚀 [核心功能] 自動更新設定
# interval=60 * 1000 代表每 60,000 毫秒 (1分鐘) 自動重新整理一次
st_autorefresh(interval=60 * 1000, key="datarefresh")

# --- 項目路徑 ---
ROOT = Path(__file__).resolve().parent
LOG_PATH = ROOT / 'result' / 'master_research_log.csv'
TRADE_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'


# ==========================================
# 📥 數據載入 (Read Only)
# ==========================================
def load_data():
    """Reads CSV files and handles missing files gracefully"""
    df_log = pd.read_csv(LOG_PATH) if LOG_PATH.exists() else pd.DataFrame()
    df_trade = pd.read_csv(TRADE_PATH) if TRADE_PATH.exists() else pd.DataFrame()
    return df_log, df_trade


# ==========================================
# 📱 標題區 (頂部)
# ==========================================
col_title, col_time = st.columns([3, 1])
with col_title:
    st.subheader("🛰️ Stat-Arb v2.5 Command Center")
with col_time:
    # 顯示目前 UI 的重新整理時間
    st.write(f"⏱️ `Last Sync: {datetime.now().strftime('%H:%M:%S')}`")

df_log, df_trade = load_data()

# ==========================================
# 📊 第一層：數據指標 (Metrics)
# ==========================================
m1, m2, m3, m4 = st.columns(4)

active_df = pd.DataFrame()
if not df_trade.empty:
    active_df = df_trade[df_trade['status'] == 'OPEN']

with m1:
    st.metric("Active Pairs", f"{len(active_df)} Pairs")
with m2:
    total_scanned = len(df_log) if not df_log.empty else 0
    st.metric("Total Scanned", f"{total_scanned}")
with m3:
    pnl = 0.0
    if not df_trade.empty and 'pnl' in df_trade.columns:
        pnl = df_trade['pnl'].sum()
    st.metric("Total PnL", f"{pnl:+.2f} USDT")
with m4:
    health = "Good"
    if not df_log.empty:
        avg_p = df_log['p_value'].head(10).mean()
        health = "Excellent" if avg_p < 0.01 else "Normal"
    st.metric("Strategy Health", health)

# ==========================================
# 📑 第二層：分頁視圖 (Tabs)
# ==========================================
tab1, tab2, tab3 = st.tabs(["🎯 Real-time Radar", "🔥 Active Positions", "📜 Historical Logs"])

with tab1:
    if not df_log.empty:
        latest_ts = df_log['timestamp'].max()
        df_plot = df_log[df_log['timestamp'] == latest_ts].copy()
        df_plot['abs_z'] = df_plot['last_z_score'].abs()
        df_plot = df_plot.sort_values(by='abs_z', ascending=False).head(15)

        fig = px.bar(
            df_plot, x='pair', y='last_z_score',
            color='last_z_score',
            color_continuous_scale='RdYlGn_r',
            range_y=[-4, 4],
            height=350  # 再次縮小圖表高度以節省空間
        )
        fig.add_hline(y=2.0, line_dash="dash", line_color="#ff4b4b")
        fig.add_hline(y=-2.0, line_dash="dash", line_color="#00ff00")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No scan data available.")

with tab2:
    if not active_df.empty:
        display_df = active_df.copy()
        if 'entry_time' in display_df.columns:
            display_df['entry_time'] = pd.to_datetime(display_df['entry_time']).dt.strftime('%m-%d %H:%M')

        # 精簡欄位，讓表格橫向空間更小
        cols = ['entry_time', 'pair', 'price1', 'price2', 'beta', 'peak_z_score']
        st.dataframe(display_df[cols], use_container_width=True)
    else:
        st.success("Scanning for new opportunities...")

with tab3:
    if not df_trade.empty:
        # 顯示最近 20 筆歷史紀錄
        history_df = df_trade[df_trade['status'] != 'OPEN'].tail(20)
        st.dataframe(history_df, use_container_width=True)

st.caption("v2.5.0-Stable | Command Tower Live Feed")