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
VERSION = "v2.6.4-Stable"

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
@st.cache_data(ttl=50)
def load_data():
    df_log = pd.read_csv(LOG_PATH) if LOG_PATH.exists() else pd.DataFrame()
    df_trade = pd.read_csv(TRADE_PATH) if TRADE_PATH.exists() else pd.DataFrame()

    if not df_trade.empty and 'entry_time' in df_trade.columns:
        df_trade['entry_time'] = pd.to_datetime(df_trade['entry_time'])
    if not df_log.empty and 'timestamp' in df_log.columns:
        df_log['timestamp'] = pd.to_datetime(df_log['timestamp'])

    return df_log, df_trade


@st.cache_data(ttl=15)
def fetch_live_prices(symbols):
    if not symbols: return {}
    try:
        exchange = ccxt.bybit({'options': {'defaultType': 'linear'}})
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

z_map = {}
if not df_log.empty:
    latest_ts = df_log['timestamp'].max()
    latest_scan = df_log[df_log['timestamp'] == latest_ts]
    z_map = latest_scan.set_index('pair')['last_z_score'].to_dict()

total_floating_pnl = 0.0
display_df = active_df.copy() if not active_df.empty else pd.DataFrame()

if not display_df.empty:
    display_df['Current Z'] = display_df['pair'].map(z_map)
    unique_symbols = list(set(display_df['s1'].tolist() + display_df['s2'].tolist()))
    live_prices = fetch_live_prices(unique_symbols)

    live_pnl_list = []
    time_left_list = []
    current_utc_time = pd.Timestamp.utcnow()

    for idx, row in display_df.iterrows():
        try:
            z_val = row['Current Z']
            if pd.isna(z_val):
                live_pnl_list.append(None)
            else:
                z_val = float(z_val)
                cp1 = live_prices.get(row['s1'])
                cp2 = live_prices.get(row['s2'])

                if cp1 is not None and cp2 is not None:
                    ep1, ep2 = float(row['price1']), float(row['price2'])
                    q1, q2 = float(row['qty1']), float(row['qty2'])

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

        try:
            entry_t = pd.Timestamp(row['entry_time'])
            if entry_t.tz is None:
                entry_t = entry_t.tz_localize('UTC')

            hl = float(row.get('opening_half_life', 8.0))
            if pd.isna(hl): hl = 8.0

            time_limit_hours = hl * 3
            deadline = entry_t + pd.Timedelta(hours=time_limit_hours)
            remaining_time = deadline - current_utc_time

            if remaining_time.total_seconds() > 0:
                hours, rem = divmod(remaining_time.total_seconds(), 3600)
                mins, _ = divmod(rem, 60)
                time_left_list.append(f"{int(hours)}h {int(mins)}m")
            else:
                time_left_list.append("Expired ⚠️")
        except Exception as e:
            time_left_list.append("N/A")

    display_df['Live PnL_num'] = live_pnl_list
    display_df['Live PnL'] = display_df['Live PnL_num'].apply(
        lambda x: f"{x:+.2f} USDT" if pd.notna(x) else "Loading...")
    display_df['Time Left'] = time_left_list

# ==========================================
# 📱 標題區 (頂部)
# ==========================================
col_title, col_time = st.columns([3, 1])
with col_title:
    st.subheader(f"🛰️ Stat-Arb {VERSION} Command Center")
with col_time:
    st.write(f"⏱️ `Last Sync: {datetime.now().strftime('%H:%M:%S')}`")

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

# --- Tab 1: 活躍持倉 ---
with tab1:
    if not display_df.empty:
        display_df['entry_time'] = display_df['entry_time'].dt.strftime('%m-%d %H:%M')
        display_df['peak_z_score'] = display_df['peak_z_score'].apply(
            lambda x: f"{float(x):.2f}" if pd.notna(x) else "N/A")
        display_df['Current Z'] = display_df['Current Z'].apply(
            lambda x: f"{float(x):.2f}" if pd.notna(x) else "Wait Scan...")
        display_df['price1'] = display_df['price1'].apply(lambda x: f"{float(x):.4f}" if pd.notna(x) else "N/A")
        display_df['price2'] = display_df['price2'].apply(lambda x: f"{float(x):.4f}" if pd.notna(x) else "N/A")
        display_df['beta'] = display_df['beta'].apply(lambda x: f"{float(x):.6f}" if pd.notna(x) else "N/A")

        cols = ['entry_time', 'pair', 'Time Left', 'peak_z_score', 'Current Z', 'Live PnL', 'price1', 'price2', 'beta']


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


        def color_time_left(val):
            if 'Expired' in str(val):
                return 'color: #ff4b4b; font-weight: bold;'
            return ''


        styled_df = (display_df[cols].style
                     .map(color_z_score, subset=['Current Z'])
                     .map(color_pnl, subset=['Live PnL'])
                     .map(color_time_left, subset=['Time Left']))

        st.dataframe(styled_df, use_container_width=True, hide_index=True)

    else:
        st.success("✨ All clear! Scanning for new opportunities...")

# --- Tab 2: 實時雷達圖 (改為戰術掃描表格) ---
with tab2:
    col_table, col_gauge = st.columns([3, 1])

    with col_table:
        if not df_log.empty:
            st.markdown("##### 🎯 Top 15 Deviated Pairs (Entry Radar)")

            latest_ts = df_log['timestamp'].max()
            df_plot = df_log[df_log['timestamp'] == latest_ts].copy()

            # 取絕對值偏離最大的前 15 名
            df_plot['abs_z'] = df_plot['last_z_score'].abs()
            df_plot = df_plot.sort_values(by='abs_z', ascending=False).head(15)

            # 準備乾淨的表格數據
            df_plot['Pair'] = df_plot['pair'].str.replace('USDT', '')
            df_plot['Z-Score'] = df_plot['last_z_score'].apply(float)
            df_plot['P-Value'] = df_plot['p_value'].apply(float)
            df_plot['Beta'] = df_plot['beta'].apply(float)


            # 🎯 核心判定邏輯：即時給出這對組合目前的狀態信號
            def get_status(row):
                z = row['Z-Score']
                p = row['P-Value']
                if p >= 0.05:
                    return "⚠️ Weak Cointegration (P>0.05)"
                if z >= 2.0:
                    return "🔴 Short 1 / Long 2"
                if z <= -2.0:
                    return "🟢 Long 1 / Short 2"
                return "⏳ Wait for Divergence"


            df_plot['Signal Status'] = df_plot.apply(get_status, axis=1)

            # 挑選要顯示的欄位
            show_df = df_plot[['Pair', 'Z-Score', 'P-Value', 'Beta', 'Signal Status']].copy()

            # 格式化小數點
            format_dict = {'Z-Score': '{:.2f}', 'P-Value': '{:.4f}', 'Beta': '{:.4f}'}


            # 顏色渲染邏輯
            def style_z(val):
                color = '#ff4b4b' if val >= 2.0 else '#00ff00' if val <= -2.0 else 'white'
                return f'color: {color}; font-weight: bold;'


            def style_p(val):
                color = 'lightgreen' if val < 0.01 else 'yellow' if val < 0.05 else 'salmon'
                return f'color: {color}; font-weight: bold;'


            def style_signal(val):
                if 'Short 1' in val: return 'color: #ff4b4b; font-weight: bold;'
                if 'Long 1' in val: return 'color: #00ff00; font-weight: bold;'
                if 'Weak' in val: return 'color: salmon;'
                return 'color: #a0a0a0;'  # 灰色代表等待


            styled_df = (show_df.style
                         .format(format_dict)
                         .map(style_z, subset=['Z-Score'])
                         .map(style_p, subset=['P-Value'])
                         .map(style_signal, subset=['Signal Status']))

            st.dataframe(styled_df, use_container_width=True, hide_index=True, height=430)

            # ✅ 搬到表格下方，翻譯為英文，縮小字體並匹配背景設計
            st.markdown("""
                <div style="background-color: #1e2130; border: 1px solid #3e4259; padding: 12px 18px; font-size: 0.8rem; color: #94a3b8; border-radius: 8px; margin-top: 10px;">
                    <span style="color: #e2e8f0; font-weight: bold; font-size: 0.85rem;">💡 Entry Criteria:</span><br>
                    <div style="margin-top: 4px;">
                        <span style="margin-left: 5px;">• <b>P-Value (Cointegration Strength):</b> Must be <code>< 0.05</code> (Smaller is better; > 0.05 indicates decoupling, do not trade).</span><br>
                        <span style="margin-left: 5px;">• <b>Z-Score (Deviation):</b> Absolute value must be <code>>= 2.0</code>.</span><br>
                        <span style="margin-left: 20px;">🔴 <code>>= 2.0</code> : Short 1st Coin / Long 2nd Coin</span><br>
                        <span style="margin-left: 20px;">🟢 <code><= -2.0</code>: Long 1st Coin / Short 2nd Coin</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)

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

            # Thermostat (Gauge) 圖例 (Legend)
            st.markdown("""
                <div style="display: flex; justify-content: center; gap: 15px; font-size: 0.85rem; color: #a0a0a0; margin-top: -20px; margin-bottom: 20px;">
                    <div style="display: flex; align-items: center;">
                        <span style="display: inline-block; width: 12px; height: 12px; background-color: lightgreen; border-radius: 50%; margin-right: 5px;"></span>
                        Excellent (<0.01)
                    </div>
                    <div style="display: flex; align-items: center;">
                        <span style="display: inline-block; width: 12px; height: 12px; background-color: yellow; border-radius: 50%; margin-right: 5px;"></span>
                        Good (<0.05)
                    </div>
                    <div style="display: flex; align-items: center;">
                        <span style="display: inline-block; width: 12px; height: 12px; background-color: salmon; border-radius: 50%; margin-right: 5px;"></span>
                        Warning (>0.05)
                    </div>
                </div>
            """, unsafe_allow_html=True)

# --- Tab 3: 歷史紀錄與盈虧圖 ---
with tab3:
    if not df_trade.empty:
        closed_df = df_trade[df_trade['status'] != 'OPEN'].copy()

        if not closed_df.empty and 'pnl' in closed_df.columns:
            closed_df['Cumulative PnL'] = closed_df['pnl'].cumsum()
            fig_pnl = px.line(closed_df, x='entry_time', y='Cumulative PnL', title="Cumulative PnL Curve", markers=True)
            fig_pnl.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_pnl, use_container_width=True)

        st.subheader("Trade History")
        display_closed = closed_df.tail(20).sort_values(by='entry_time', ascending=False)
        st.dataframe(display_closed, use_container_width=True, hide_index=True)
    else:
        st.write("No historical trades yet.")

st.caption(f"{VERSION} | Command Tower Live Feed")