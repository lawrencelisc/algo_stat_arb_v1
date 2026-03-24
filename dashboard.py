import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
import ccxt

# ==========================================
# 🛰️ 儀表板配置中心
# ==========================================
st.set_page_config(
    page_title="Stat-Arb v2.1 Command Center",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 自定義 CSS 提升質感
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border-left: 5px solid #00ff00; }
    div[data-testid="stMetricValue"] { color: #00ff00; }
    </style>
""", unsafe_allow_html=True)

# 讀取路徑
ROOT = Path(__file__).resolve().parent
LOG_PATH = ROOT / 'result' / 'master_research_log.csv'
TRADE_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'
SIGNAL_PATH = ROOT / 'data' / 'signal' / 'signal_table.csv'


# ==========================================
# 📥 數據載入引擎 (含緩存邏輯)
# ==========================================
def load_data():
    df_log = pd.read_csv(LOG_PATH) if LOG_PATH.exists() else pd.DataFrame()
    df_trade = pd.read_csv(TRADE_PATH) if TRADE_PATH.exists() else pd.DataFrame()
    df_sig = pd.read_csv(SIGNAL_PATH) if SIGNAL_PATH.exists() else pd.DataFrame()
    return df_log, df_trade, df_sig


df_log, df_trade, df_sig = load_data()

# ==========================================
# 📱 Sidebar: 實時系統狀態
# ==========================================
st.sidebar.title("🛰️ 指揮塔監控")
st.sidebar.markdown("---")
status = "🟢 運作中" if not df_log.empty else "🔴 停止"
st.sidebar.write(f"系統狀態: **{status}**")
st.sidebar.write(f"帳戶本金: **1,942.43 USDT**")
st.sidebar.write(f"最後更新: `{datetime.now().strftime('%H:%M:%S')}`")

if st.sidebar.button("🔄 手動刷新數據"):
    st.rerun()

# ==========================================
# 📊 第一區塊：核心指標 (Top Metrics)
# ==========================================
col1, col2, col3, col4 = st.columns(4)

active_pairs = len(df_trade[df_trade['status'] == 'OPEN']) if not df_trade.empty else 0
total_pnl = df_trade['pnl'].sum() if 'pnl' in df_trade.columns else 0.0

with col1:
    st.metric("當前持倉 (Pairs)", f"{active_pairs} / 10")
with col2:
    st.metric("累計利潤 (USDT)", f"{total_pnl:+.2f}")
with col3:
    win_rate = 0.0
    if 'pnl' in df_trade.columns and len(df_trade) > 0:
        win_rate = len(df_trade[df_trade['pnl'] > 0]) / len(df_trade)
    st.metric("勝率 (Win Rate)", f"{win_rate:.1%}")
with col4:
    z_extreme = 0
    if not df_log.empty:
        last_ts = df_log['timestamp'].max()
        z_extreme = len(df_log[(df_log['timestamp'] == last_ts) & (abs(df_log['last_z_score']) > 2.0)])
    st.metric("極端偏離機會", f"{z_extreme}")

# ==========================================
# 📈 第二區塊：Performance Chart (Equity Curve)
# ==========================================
st.markdown("---")
st.header("📈 績效表現與回撤 (Equity Curve)")

if not df_trade.empty and 'pnl' in df_trade.columns:
    df_trade['entry_time'] = pd.to_datetime(df_trade['entry_time'])
    df_trade = df_trade.sort_values('entry_time')
    df_trade['cum_pnl'] = df_trade['pnl'].cumsum() + 1945.70

    fig_equity = px.line(df_trade, x='entry_time', y='cum_pnl',
                         title='帳戶總權益增長曲線',
                         color_discrete_sequence=['#00ff00'])
    fig_equity.update_layout(xaxis_title="時間", yaxis_title="USDT", template="plotly_dark")
    st.plotly_chart(fig_equity, use_container_width=True)
else:
    st.info("💡 目前尚無已平倉數據，待首筆交易結算後將顯示累計收益曲線。")

# ==========================================
# 🎯 第三區塊：Z-Score Profile & Radar
# ==========================================
st.markdown("---")
left_col, right_col = st.columns([2, 1])

with left_col:
    st.header("🎯 獵場即時掃描 (Z-Score Radar)")
    if not df_log.empty:
        last_ts = df_log['timestamp'].max()
        df_latest = df_log[df_log['timestamp'] == last_ts].copy()

        # 只取偏離最嚴重的 Top 15
        df_top = df_latest.sort_values(by='last_z_score', key=abs, ascending=False).head(15)

        fig_z = px.bar(df_top, x='pair', y='last_z_score',
                       color='last_z_score',
                       color_continuous_scale='RdYlGn_r',  # 紅綠反轉
                       title="Top 15 偏離組合 (Z > 2.0 為進場訊號)")
        fig_z.add_hline(y=2.0, line_dash="dash", line_color="red", annotation_text="SELL Spread")
        fig_z.add_hline(y=-2.0, line_dash="dash", line_color="green", annotation_text="BUY Spread")
        fig_z.update_layout(template="plotly_dark")
        st.plotly_chart(fig_z, use_container_width=True)

with right_col:
    st.header("🔍 統計分布")
    if not df_log.empty:
        fig_hist = px.histogram(df_latest, x="last_z_score", nbins=30,
                                title="全市場 190 對 Z-Score 分布",
                                color_discrete_sequence=['#636EFA'])
        fig_hist.update_layout(template="plotly_dark")
        st.plotly_chart(fig_hist, use_container_width=True)

# ==========================================
# 📂 第四區塊：交易與持倉 (Transactions)
# ==========================================
st.markdown("---")
st.header("📂 實時持倉與歷史清單")

tabs = st.tabs(["🔥 當前持倉 (Active)", "📜 歷史紀錄 (History)", "⚓ 被攔截訊號 (Skipped)"])

with tabs[0]:
    if not df_trade.empty:
        active_df = df_trade[df_trade['status'] == 'OPEN'].copy()
        if not active_df.empty:
            st.dataframe(active_df.style.background_gradient(subset=['beta'], cmap='Blues'), use_container_width=True)
        else:
            st.write("目前無持倉中部位。")

with tabs[1]:
    if not df_trade.empty:
        st.dataframe(df_trade.sort_values('entry_time', ascending=False), use_container_width=True)

with tabs[2]:
    if not df_sig.empty:
        skipped = df_sig[df_sig['status'].str.contains('SKIP', na=False)]
        st.dataframe(skipped.sort_values('timestamp', ascending=False), use_container_width=True)

# ==========================================
# 🧪 第五區塊：科學官深度分析 (Advanced Insights)
# ==========================================
st.markdown("---")
st.header("🧪 首席科學官：深度監控 (Advanced Analytics)")

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("📡 均值回歸速度 (Half-Life)")
    if not df_log.empty:
        df_hl = df_latest[df_latest['p_value'] < 0.05].sort_values('half_life').head(10)
        fig_hl = px.scatter(df_hl, x="pair", y="half_life", size="correlation", color="p_value",
                            title="Top 10 最快回歸組合 (氣泡越大代表相關性越高)")
        fig_hl.update_layout(template="plotly_dark")
        st.plotly_chart(fig_hl, use_container_width=True)

with col_b:
    st.subheader("⚓ 資金費率地圖 (Funding Guard Preview)")
    st.info("此模組將在下個版本中顯示全市場幣種的資金費率熱圖，幫助預測開倉障礙。")

# 腳註
st.markdown("---")
st.caption(f"🛰️ Stat-Arb v2.1 Dashboard | 數據路徑: {ROOT} | 切記：統計利潤需要時間耐心等待回歸。")