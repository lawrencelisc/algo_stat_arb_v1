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
VERSION = "v3.0.0-Stable"

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
# 📥 數據載入與預處理
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
        prices = {s: float(tickers[f"{s.replace('USDT', '')}/USDT:USDT"]['last']) for s in symbols if
                  f"{s.replace('USDT', '')}/USDT:USDT" in tickers}
        return prices
    except:
        return {}


df_log, df_trade = load_data()
active_df = df_trade[df_trade['status'] == 'OPEN'].copy() if not df_trade.empty else pd.DataFrame()

z_map = {}
if not df_log.empty:
    latest_ts = df_log['timestamp'].max()
    z_map = df_log[df_log['timestamp'] == latest_ts].set_index('pair')['last_z_score'].to_dict()

# ==========================================
# 📊 第一層：數據指標
# ==========================================
total_floating_pnl = 0.0
if not active_df.empty:
    active_df['Current Z'] = active_df['pair'].map(z_map)
    unique_symbols = list(set(active_df['s1'].tolist() + active_df['s2'].tolist()))
    live_prices = fetch_live_prices(unique_symbols)

    # 計算 PnL 與 Progress
    pnls = []
    progress_list = []
    for idx, row in active_df.iterrows():
        try:
            cur_z = row['Current Z']
            peak_z = float(row['peak_z_score'])

            # Progress 計算: (1 - 現在Z/初始Z) * 100
            if not pd.isna(cur_z) and peak_z != 0:
                prog = (1 - (float(cur_z) / peak_z)) * 100
                progress_list.append(prog)
            else:
                progress_list.append(0)

            # PnL 計算
            cp1, cp2 = live_prices.get(row['s1']), live_prices.get(row['s2'])
            if cp1 and cp2:
                ep1, ep2, q1, q2 = float(row['price1']), float(row['price2']), float(row['qty1']), float(row['qty2'])
                pnl = ((ep1 - cp1) * q1 + (cp2 - ep2) * q2) if peak_z > 0 else ((cp1 - ep1) * q1 + (ep2 - cp2) * q2)
                pnls.append(pnl)
                total_floating_pnl += pnl
            else:
                pnls.append(None)
        except:
            pnls.append(None)
            progress_list.append(0)

    active_df['Live PnL_num'] = pnls
    active_df['Progress'] = progress_list

# UI Header
col_title, col_time = st.columns([3, 1])
with col_title: st.subheader(f"🛰️ Stat-Arb {VERSION} Command Center")
with col_time: st.write(f"⏱️ `Last Sync: {datetime.now().strftime('%H:%M:%S')}`")

m1, m2, m3, m4 = st.columns(4)
with m1: st.metric("Active Pairs", f"{len(active_df)} Pairs")
with m2: st.metric("Total Scanned", f"{len(df_log) if not df_log.empty else 0}")
with m3:
    realized_pnl = df_trade['pnl'].sum() if not df_trade.empty and 'pnl' in df_trade.columns else 0.0
    st.metric("Total PnL (Realized)", f"{realized_pnl:+.2f} U", delta=f"Float: {total_floating_pnl:+.2f} U")
with m4:
    avg_p = df_log[df_log['timestamp'] == df_log['timestamp'].max()]['p_value'].head(
        10).mean() if not df_log.empty else 1.0
    st.metric("Strategy Health", "Excellent" if avg_p < 0.01 else "Good", delta=f"P-Val: {avg_p:.4f}",
              delta_color="inverse")

# ==========================================
# 📑 第二層：分頁視圖 (Tabs)
# ==========================================
tab1, tab2, tab3 = st.tabs(["🔥 Active Positions", "🎯 Real-time Radar", "📜 Historical Logs"])


# ✅ 核心 HTML 上色函式：分辨 Z-Score 正負
def get_z_color_html(z_val):
    try:
        z = float(z_val)
        if z > 0:
            return f'<span style="color: #ff4b4b; font-weight: bold;">+{z:.2f} 🔴</span>'
        elif z < 0:
            return f'<span style="color: #00ff00; font-weight: bold;">{z:.2f} 🟢</span>'
        else:
            return f'<span style="color: #ffffff; font-weight: bold;">{z:.2f}</span>'
    except:
        return f'<span style="color: #a0a0a0;">N/A</span>'


# --- Tab 1: 活躍持倉 ---
with tab1:
    if not active_df.empty:
        active_df['Side'] = active_df.apply(lambda r: f"🔴 Short {r['s1'].replace('USDT', '')}" if float(
            r['peak_z_score']) > 0 else f"🟢 Long {r['s1'].replace('USDT', '')}", axis=1)
        active_df['Live PnL'] = active_df['Live PnL_num'].apply(
            lambda x: f"{x:+.2f} USDT" if pd.notna(x) else "Loading...")

        # 使用自定義表格，加入進度條與 Z-Score 顏色視覺效果
        for idx, row in active_df.iterrows():
            with st.expander(f"🚢 {row['pair']} | {row['Side']} | PnL: {row['Live PnL']}", expanded=True):
                c1, c2, c3 = st.columns([1, 2, 1])

                # 取得帶顏色的 Z-Score HTML
                cur_z_html = get_z_color_html(row['Current Z'])
                peak_z_html = get_z_color_html(row['peak_z_score'])

                c1.markdown(f"**Current Z:** {cur_z_html}", unsafe_allow_html=True)
                c1.write(f"**Target:** $0.00$")

                # 進度條顏色: 賺錢綠色，賠錢紅色
                c2.write(f"Reversion Progress: **{row['Progress']:.1f}%**")
                c2.progress(min(max(row['Progress'] / 100, 0.0), 1.0))

                c3.markdown(f"**Peak Z:** {peak_z_html}", unsafe_allow_html=True)
                pnl_color = "lightgreen" if row['Live PnL_num'] and row['Live PnL_num'] > 0 else "salmon" if row[
                                                                                                                 'Live PnL_num'] and \
                                                                                                             row[
                                                                                                                 'Live PnL_num'] < 0 else "white"
                c3.markdown(f"**PnL:** <span style='color:{pnl_color}; font-weight:bold;'>{row['Live PnL']}</span>",
                            unsafe_allow_html=True)
    else:
        st.success("✨ All clear! Scanning for new opportunities...")

# --- Tab 2: 實時雷達圖 ---
with tab2:
    col_table, col_gauge = st.columns([3, 1])
    with col_table:
        if not df_log.empty:
            st.markdown("##### 🎯 Top 15 Deviated Pairs (Entry Radar)")
            latest_ts = df_log['timestamp'].max()
            df_plot = df_log[df_log['timestamp'] == latest_ts].copy()
            df_plot['abs_z'] = df_plot['last_z_score'].abs()
            df_plot = df_plot.sort_values(by='abs_z', ascending=False).head(15)
            df_plot['Pair'] = df_plot['pair'].str.replace('USDT', '')
            df_plot['Z-Score'] = df_plot['last_z_score'].apply(float)
            df_plot['P-Value'] = df_plot['p_value'].apply(float)
            df_plot['Beta'] = df_plot['beta'].apply(float)


            def get_status(row):
                z, p = row['Z-Score'], row['P-Value']
                if p >= 0.05: return "⚠️ Weak Cointegration"
                if z >= 2.0: return "🔴 Short 1 / Long 2"
                if z <= -2.0: return "🟢 Long 1 / Short 2"
                return "⏳ Wait for Divergence"


            df_plot['Signal Status'] = df_plot.apply(get_status, axis=1)
            show_df = df_plot[['Pair', 'Z-Score', 'P-Value', 'Beta', 'Signal Status']]


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
                return 'color: #a0a0a0;'


            styled_df = (show_df.style
                         .format({'Z-Score': '{:.2f}', 'P-Value': '{:.4f}', 'Beta': '{:.4f}'})
                         .map(style_z, subset=['Z-Score'])
                         .map(style_p, subset=['P-Value'])
                         .map(style_signal, subset=['Signal Status']))

            st.dataframe(styled_df, use_container_width=True, hide_index=True, height=430)

            st.markdown("""
                <div style="background-color: #1e2130; border: 1px solid #3e4259; padding: 12px 18px; font-size: 0.8rem; color: #94a3b8; border-radius: 8px; margin-top: 10px;">
                    <span style="color: #e2e8f0; font-weight: bold; font-size: 0.85rem;">💡 Entry Criteria:</span><br>
                    <div style="margin-top: 4px;">
                        • <b>P-Value:</b> Must be <code>< 0.05</code><br>
                        • <b>Z-Score:</b> Absolute value must be <code>>= 2.0</code><br>
                        <span style="margin-left: 15px;">🔴 <code>>= 2.0</code> : Short 1st Coin / Long 2nd Coin</span><br>
                        <span style="margin-left: 15px;">🟢 <code><= -2.0</code>: Long 1st Coin / Short 2nd Coin</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.info("No scan data available.")

    with col_gauge:
        if not df_log.empty:
            fig_gauge = go.Figure(go.Indicator(mode="gauge+number", value=avg_p, title={'text': "Market Cointegration"},
                                               gauge={'axis': {'range': [None, 0.1]}, 'bar': {'color': "darkblue"},
                                                      'steps': [{'range': [0, 0.01], 'color': "lightgreen"},
                                                                {'range': [0.01, 0.05], 'color': "yellow"},
                                                                {'range': [0.05, 0.1], 'color': "salmon"}]}))
            st.plotly_chart(fig_gauge, use_container_width=True)
            st.markdown(
                '<div style="display: flex; justify-content: center; gap: 15px; font-size: 0.85rem; color: #a0a0a0; margin-top: -20px;">'
                '<div><span style="color: lightgreen;">●</span> Excellent</div><div><span style="color: yellow;">●</span> Good</div><div><span style="color: salmon;">●</span> Warning</div></div>',
                unsafe_allow_html=True)

# --- Tab 3: 歷史紀錄 ---
with tab3:
    if not df_trade.empty:
        closed_df = df_trade[df_trade['status'] != 'OPEN'].copy()
        if not closed_df.empty and 'pnl' in closed_df.columns:
            closed_df['Cumulative PnL'] = closed_df['pnl'].cumsum()
            st.plotly_chart(px.line(closed_df, x='entry_time', y='Cumulative PnL', title="Cumulative PnL Curve"),
                            use_container_width=True)
        st.dataframe(closed_df.tail(20).sort_values(by='entry_time', ascending=False), use_container_width=True,
                     hide_index=True)

st.caption(f"{VERSION} | Command Tower Live Feed")