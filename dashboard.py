import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh

# ==========================================
# 🛰️ 網頁配置與自定義 CSS
# ==========================================
VERSION = "v2.5.1-Stable"

st.set_page_config(
    page_title=f"Stat-Arb {VERSION} UI",
    page_icon="🛰️",
    layout="wide"
)

st.markdown("""
    <style>
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 0rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
    }
    .stMetric {
        background-color: #1e2130;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #3e4259;
    }
    footer {visibility: hidden;}
    </style>
    """, unsafe_allow_html=True)

# 🚀 自動更新 (每 60 秒)
st_autorefresh(interval=60 * 1000, key="datarefresh")

# --- 項目路徑 ---
ROOT = Path(__file__).resolve().parent
LOG_PATH = ROOT / 'result' / 'master_research_log.csv'
TRADE_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'


# ==========================================
# 📥 數據載入與預處理 (Read Only)
# ==========================================
@st.cache_data(ttl=50)  # 快取 50 秒，避免頻繁讀檔
def load_data():
    df_log = pd.read_csv(LOG_PATH) if LOG_PATH.exists() else pd.DataFrame()
    df_trade = pd.read_csv(TRADE_PATH) if TRADE_PATH.exists() else pd.DataFrame()

    # 時間格式轉換
    if not df_trade.empty and 'entry_time' in df_trade.columns:
        df_trade['entry_time'] = pd.to_datetime(df_trade['entry_time'])
    if not df_log.empty and 'timestamp' in df_log.columns:
        df_log['timestamp'] = pd.to_datetime(df_log['timestamp'])

    return df_log, df_trade


df_log, df_trade = load_data()

# ==========================================
# 📱 標題區 (頂部)
# ==========================================
col_title, col_time = st.columns([3, 1])
with col_title:
    st.subheader(f"🛰️ Stat-Arb {VERSION} Command Center")
with col_time:
    st.write(f"⏱️ `Last Sync: {datetime.now().strftime('%H:%M:%S')}`")

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
    # 計算健康度 (Avg P-Value of Top 10)
    avg_p = 1.0
    if not df_log.empty:
        latest_ts = df_log['timestamp'].max()
        avg_p = df_log[df_log['timestamp'] == latest_ts]['p_value'].head(10).mean()

    health_status = "Excellent" if avg_p < 0.01 else "Good" if avg_p < 0.05 else "Warning"
    st.metric("Strategy Health", health_status, delta=f"P-Val: {avg_p:.4f}", delta_color="inverse")

# ==========================================
# 📑 第二層：分頁視圖 (Tabs)
# ==========================================
tab1, tab2, tab3 = st.tabs(["🔥 Active Positions", "🎯 Real-time Radar", "📜 Historical Logs"])

# --- Tab 1: 活躍持倉 (強化版) ---
with tab1:
    if not active_df.empty:
        display_df = active_df.copy()

        # 取得最新 Z-Score
        if not df_log.empty:
            latest_ts = df_log['timestamp'].max()
            latest_scan = df_log[df_log['timestamp'] == latest_ts]

            # 將最新 Z-Score 映射到持倉表格
            display_df = display_df.merge(
                latest_scan[['pair', 'last_z_score']],
                on='pair',
                how='left',
                suffixes=('', '_current')
            )
            display_df.rename(columns={'last_z_score_current': 'Current Z'}, inplace=True)
            display_df['Current Z'] = display_df['Current Z'].round(2)
        else:
            display_df['Current Z'] = "Wait Scan..."

        # 整理顯示欄位
        display_df['entry_time'] = display_df['entry_time'].dt.strftime('%m-%d %H:%M')
        display_df['peak_z_score'] = display_df['peak_z_score'].round(2)

        cols = ['entry_time', 'pair', 'peak_z_score', 'Current Z', 'price1', 'price2', 'beta']


        # 使用 Styler 替 Z-Score 上色 (綠色代表賺錢回歸中)
        def color_z_score(val):
            try:
                v = float(val)
                color = 'green' if abs(v) < 2.0 else 'red'
                return f'color: {color}'
            except:
                return ''


        styled_df = display_df[cols].style.map(color_z_score, subset=['Current Z'])
        st.dataframe(styled_df, use_container_width=True, hide_index=True)

    else:
        st.success("✨ All clear! Scanning for new opportunities...")

# --- Tab 2: 實時雷達圖 ---
with tab2:
    col_radar, col_gauge = st.columns([3, 1])

    with col_radar:
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
                title="Top 15 Deviated Pairs"
            )
            fig.add_hline(y=2.0, line_dash="dash", line_color="#ff4b4b")
            fig.add_hline(y=-2.0, line_dash="dash", line_color="#00ff00")
            fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No scan data available.")

    with col_gauge:
        if not df_log.empty:
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=avg_p,
                title={'text': "Market Cointegration (P-Value)"},
                gauge={
                    'axis': {'range': [None, 0.1]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 0.01], 'color': "lightgreen"},
                        {'range': [0.01, 0.05], 'color': "yellow"},
                        {'range': [0.05, 0.1], 'color': "salmon"}
                    ],
                }
            ))
            fig_gauge.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_gauge, use_container_width=True)

# --- Tab 3: 歷史紀錄與盈虧圖 ---
with tab3:
    if not df_trade.empty:
        closed_df = df_trade[df_trade['status'] != 'OPEN'].copy()

        if not closed_df.empty and 'pnl' in closed_df.columns:
            # 繪製累積盈虧圖
            closed_df['Cumulative PnL'] = closed_df['pnl'].cumsum()
            fig_pnl = px.line(closed_df, x='entry_time', y='Cumulative PnL', title="Cumulative PnL Curve", markers=True)
            fig_pnl.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_pnl, use_container_width=True)

        # 顯示歷史表格
        st.subheader("Trade History")
        display_closed = closed_df.tail(20).sort_values(by='entry_time', ascending=False)
        st.dataframe(display_closed, use_container_width=True, hide_index=True)
    else:
        st.write("No historical trades yet.")

st.caption(f"{VERSION} | Command Tower Live Feed")