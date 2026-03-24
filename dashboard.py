import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import ccxt
from pathlib import Path
from datetime import datetime, timezone
from streamlit_autorefresh import st_autorefresh

# ==========================================
# 🛰️ 網頁配置與自定義 CSS
# ==========================================
VERSION = "v2.5.5-Stable"

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
@st.cache_data(ttl=50)  # 快取 50 秒，避免頻繁讀檔 CSV
def load_data():
    df_log = pd.read_csv(LOG_PATH) if LOG_PATH.exists() else pd.DataFrame()
    df_trade = pd.read_csv(TRADE_PATH) if TRADE_PATH.exists() else pd.DataFrame()

    if not df_trade.empty and 'entry_time' in df_trade.columns:
        df_trade['entry_time'] = pd.to_datetime(df_trade['entry_time'])
    if not df_log.empty and 'timestamp' in df_log.columns:
        df_log['timestamp'] = pd.to_datetime(df_log['timestamp'])

    return df_log, df_trade


# 📡 實時價格抓取 (Public API，無需 Key，安全免干擾)
@st.cache_data(ttl=15)  # 每 15 秒才准許抓一次 API，防止被 Ban
def fetch_live_prices(symbols):
    if not symbols: return {}
    try:
        exchange = ccxt.bybit({'options': {'defaultType': 'linear'}})
        # 轉換成 CCXT 的 Bybit 永續合約格式 (例: BTCUSDT -> BTC/USDT:USDT)
        ccxt_symbols = [f"{s.replace('USDT', '')}/USDT:USDT" for s in symbols]
        tickers = exchange.fetch_tickers(ccxt_symbols)
        prices = {}
        for s in symbols:
            ccxt_s = f"{s.replace('USDT', '')}/USDT:USDT"
            if ccxt_s in tickers:
                prices[s] = float(tickers[ccxt_s]['last'])
        return prices
    except Exception as e:
        return {}


df_log, df_trade = load_data()

active_df = pd.DataFrame()
if not df_trade.empty:
    active_df = df_trade[df_trade['status'] == 'OPEN']

# 取得最新 Z-Score 用於計算方向與映射
z_map = {}
if not df_log.empty:
    latest_ts = df_log['timestamp'].max()
    latest_scan = df_log[df_log['timestamp'] == latest_ts]
    z_map = latest_scan.set_index('pair')['last_z_score'].to_dict()

# 💰 實時未實現盈虧 (Live PnL) 結算邏輯
total_floating_pnl = 0.0
display_df = active_df.copy() if not active_df.empty else pd.DataFrame()

if not display_df.empty:
    display_df['Current Z'] = display_df['pair'].map(z_map)
    unique_symbols = list(set(display_df['s1'].tolist() + display_df['s2'].tolist()))
    live_prices = fetch_live_prices(unique_symbols)

    live_pnl_list = []
    for idx, row in display_df.iterrows():
        try:
            z_val = row['Current Z']
            if pd.isna(z_val):
                live_pnl_list.append(None)
                continue
            z_val = float(z_val)

            cp1 = live_prices.get(row['s1'])
            cp2 = live_prices.get(row['s2'])

            if cp1 is not None and cp2 is not None:
                ep1, ep2 = float(row['price1']), float(row['price2'])
                q1, q2 = float(row['qty1']), float(row['qty2'])

                # 透過 Z-Score 正負號判斷多空方向 (Z>0: Short s1/Long s2, Z<0: Long s1/Short s2)
                if z_val > 0:
                    pnl = (ep1 - cp1) * q1 + (cp2 - ep2) * q2
                else:
                    pnl = (cp1 - ep1) * q1 + (ep2 - cp2) * q2

                live_pnl_list.append(pnl)
                total_floating_pnl += pnl
            else:
                live_pnl_list.append(None)
        except Exception:
            live_pnl_list.append(None)

    display_df['Live PnL_num'] = live_pnl_list
    display_df['Live PnL'] = display_df['Live PnL_num'].apply(
        lambda x: f"{x:+.2f} USDT" if pd.notna(x) else "Loading...")

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

with m1:
    st.metric("Active Pairs", f"{len(display_df)} Pairs")
with m2:
    total_scanned = len(df_log) if not df_log.empty else 0
    st.metric("Total Scanned", f"{total_scanned}")
with m3:
    realized_pnl = 0.0
    if not df_trade.empty and 'pnl' in df_trade.columns:
        realized_pnl = df_trade['pnl'].sum()
    # ✅ [新功能] 結合已實現盈虧與「實時浮動盈虧」顯示
    st.metric("Total PnL (Realized)", f"{realized_pnl:+.2f} USDT", delta=f"Float: {total_floating_pnl:+.2f} U",
              delta_color="normal")
with m4:
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
    if not display_df.empty:
        # 整理顯示欄位格式
        display_df['entry_time'] = display_df['entry_time'].dt.strftime('%m-%d %H:%M')
        display_df['peak_z_score'] = display_df['peak_z_score'].apply(
            lambda x: f"{float(x):.2f}" if pd.notna(x) else "N/A")
        display_df['Current Z'] = display_df['Current Z'].apply(
            lambda x: f"{float(x):.2f}" if pd.notna(x) else "Wait Scan...")
        display_df['price1'] = display_df['price1'].apply(lambda x: f"{float(x):.4f}" if pd.notna(x) else "N/A")
        display_df['price2'] = display_df['price2'].apply(lambda x: f"{float(x):.4f}" if pd.notna(x) else "N/A")
        display_df['beta'] = display_df['beta'].apply(lambda x: f"{float(x):.6f}" if pd.notna(x) else "N/A")

        cols = ['entry_time', 'pair', 'peak_z_score', 'Current Z', 'Live PnL', 'price1', 'price2', 'beta']


        # 色彩映射函數
        def color_z_score(val):
            try:
                v = float(val)
                color = 'lightgreen' if abs(v) < 2.0 else 'salmon'
                return f'color: {color}; font-weight: bold;'
            except:
                return ''


        def color_pnl(val):
            if isinstance(val, str) and 'USDT' in val:
                try:
                    num = float(val.replace(' USDT', '').replace('+', ''))
                    color = 'lightgreen' if num > 0 else 'salmon' if num < 0 else 'white'
                    return f'color: {color}; font-weight: bold;'
                except:
                    return ''
            return ''


        # 雙重上色：Z-Score 和 Live PnL 都有專屬顏色提示
        styled_df = display_df[cols].style.map(color_z_score, subset=['Current Z']).map(color_pnl, subset=['Live PnL'])
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