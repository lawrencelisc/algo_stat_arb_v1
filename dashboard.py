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
VERSION = "v3.1.2-Stable"

st.set_page_config(
    page_title=f"Stat-Arb {VERSION} UI",
    page_icon="🛰️",
    layout="wide"
)

# 注入自定義樣式，優化深色模式體驗
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

# 🚀 自動更新 (每 60 秒刷新一次 UI)
st_autorefresh(interval=60 * 1000, key="datarefresh")

# --- 項目路徑配置 ---
ROOT = Path(__file__).resolve().parent
LOG_PATH = ROOT / 'result' / 'master_research_log.csv'
TRADE_PATH = ROOT / 'data' / 'trade' / 'trade_record.csv'


# ==========================================
# 📥 數據載入與預處理
# ==========================================
@st.cache_data(ttl=50)
def load_data():
    """載入研究日誌與交易紀錄，具備結構衝突防護"""
    try:
        # 使用 on_bad_lines='skip' 或是手動清理 CSV 確保讀取成功
        df_log = pd.read_csv(LOG_PATH) if LOG_PATH.exists() else pd.DataFrame()
        df_trade = pd.read_csv(TRADE_PATH) if TRADE_PATH.exists() else pd.DataFrame()
    except Exception as e:
        st.error(f"⚠️ CSV 讀取失敗（可能是版本結構衝突，請執行 rm 清理）：{e}")
        return pd.DataFrame(), pd.DataFrame()

    if not df_trade.empty and 'entry_time' in df_trade.columns:
        df_trade['entry_time'] = pd.to_datetime(df_trade['entry_time'])
    if not df_log.empty and 'timestamp' in df_log.columns:
        df_log['timestamp'] = pd.to_datetime(df_log['timestamp'])

    return df_log, df_trade


@st.cache_data(ttl=15)
def fetch_live_prices(symbols):
    """從 Bybit 抓取實時價格用於計算浮盈"""
    if not symbols: return {}
    try:
        exchange = ccxt.bybit({'options': {'defaultType': 'linear'}})
        # 轉換為 CCXT 格式
        ccxt_symbols = [f"{s.replace('USDT', '')}/USDT:USDT" for s in symbols]
        tickers = exchange.fetch_tickers(ccxt_symbols)
        prices = {s: float(tickers[f"{s.replace('USDT', '')}/USDT:USDT"]['last'])
                  for s in symbols if f"{s.replace('USDT', '')}/USDT:USDT" in tickers}
        return prices
    except:
        return {}


# 1. 初始化數據
df_log, df_trade = load_data()
active_df = df_trade[df_trade['status'] == 'OPEN'].copy() if not df_trade.empty else pd.DataFrame()

# 2. 構建 Z-Score 映射與強制監控映射 (解決 Wait Scan 關鍵)
z_map = {}
active_status_map = {}
if not df_log.empty:
    latest_ts = df_log['timestamp'].max()
    latest_scan = df_log[df_log['timestamp'] == latest_ts]
    z_map = latest_scan.set_index('pair')['last_z_score'].to_dict()
    # 讀取 v3.1.2 新增的 is_active 欄位
    if 'is_active' in latest_scan.columns:
        active_status_map = latest_scan.set_index('pair')['is_active'].to_dict()

# ==========================================
# 📊 核心運算：PnL 與 Time-Exit
# ==========================================
total_floating_pnl = 0.0
if not active_df.empty:
    active_df['Current Z'] = active_df['pair'].map(z_map)
    active_df['Forced'] = active_df['pair'].map(active_status_map).fillna(False)

    # 獲取所有相關幣種的現價
    unique_symbols = list(set(active_df['s1'].tolist() + active_df['s2'].tolist()))
    live_prices = fetch_live_prices(unique_symbols)

    pnls = []
    time_left_list = []
    current_utc_time = pd.Timestamp.utcnow()

    for idx, row in active_df.iterrows():
        # 計算 Float PnL
        try:
            peak_z = float(row['peak_z_score'])
            cp1, cp2 = live_prices.get(row['s1']), live_prices.get(row['s2'])
            if cp1 and cp2:
                ep1, ep2, q1, q2 = float(row['price1']), float(row['price2']), float(row['qty1']), float(row['qty2'])
                # 根據做多或做空對沖方向計算損益
                pnl = ((ep1 - cp1) * q1 + (cp2 - ep2) * q2) if peak_z > 0 else ((cp1 - ep1) * q1 + (ep2 - cp2) * q2)
                pnls.append(pnl)
                total_floating_pnl += pnl
            else:
                pnls.append(None)
        except:
            pnls.append(None)

        # 計算 Time Left (有始有終邏輯)
        try:
            entry_t = pd.Timestamp(row['entry_time'])
            if entry_t.tz is None: entry_t = entry_t.tz_localize('UTC')
            # 設定過期門檻：3 倍半衰期
            deadline = entry_t + pd.Timedelta(hours=float(row.get('opening_half_life', 8.0)) * 3)
            remaining_time = deadline - current_utc_time
            if remaining_time.total_seconds() > 0:
                h, r = divmod(remaining_time.total_seconds(), 3600)
                time_left_list.append(f"{int(h)}h {int(r // 60)}m")
            else:
                time_left_list.append("Expired ⚠️")
        except:
            time_left_list.append("N/A")

    active_df['Live PnL_num'] = pnls
    active_df['Time Left'] = time_left_list

# ==========================================
# 🖥️ UI 佈局：指揮控制台
# ==========================================
col_title, col_time = st.columns([3, 1])
with col_title: st.subheader(f"🛰️ Stat-Arb {VERSION} Command Center")
with col_time: st.write(f"⏱️ `最後同步: {datetime.now().strftime('%H:%M:%S')}`")

# 第一層：快速指標
m1, m2, m3, m4 = st.columns(4)
with m1: st.metric("活躍組合 (Active)", f"{len(active_df)} Pairs")
with m2: st.metric("總掃描路徑", f"{len(df_log) if not df_log.empty else 0}")
with m3:
    realized_pnl = df_trade['pnl'].sum() if not df_trade.empty and 'pnl' in df_trade.columns else 0.0
    st.metric("累計已實現盈虧", f"{realized_pnl:+.2f} U", delta=f"浮動: {total_floating_pnl:+.2f} U")
with m4:
    avg_p = df_log[df_log['timestamp'] == df_log['timestamp'].max()]['p_value'].head(
        10).mean() if not df_log.empty else 1.0
    st.metric("策略健康度 (Top 10)", "Excellent" if avg_p < 0.01 else "Good", delta=f"P-Val: {avg_p:.4f}",
              delta_color="inverse")

# 第二層：功能分頁
tab1, tab2, tab3 = st.tabs(["🔥 活躍頭寸 (Positions)", "🎯 實時雷達 (Radar)", "📜 歷史紀錄 (Logs)"])

with tab1:
    if not active_df.empty:
        display_df = active_df.copy()
        display_df['entry_time'] = display_df['entry_time'].dt.strftime('%m-%d %H:%M')
        display_df['Action'] = display_df.apply(
            lambda r: f"🟢 Short {r['s1'].replace('USDT', '')} / Long {r['s2'].replace('USDT', '')}" if float(r[
                                                                                                                 'peak_z_score']) > 0 else f"🔴 Long {r['s1'].replace('USDT', '')} / Short {r['s2'].replace('USDT', '')}",
            axis=1)


        def format_z_with_emoji(x, is_forced):
            if pd.isna(x): return "Wait Scan..."
            prefix = "🛡️ " if is_forced else ""
            try:
                val = float(x)
                emoji = "🟢" if val > 0 else "🔴"
                return f"{prefix}{emoji} {val:+.2f}"
            except:
                return str(x)


        display_df['Peak Z'] = display_df.apply(lambda r: format_z_with_emoji(r['peak_z_score'], False), axis=1)
        display_df['Current Z'] = display_df.apply(lambda r: format_z_with_emoji(r['Current Z'], r['Forced']), axis=1)
        display_df['Live PnL'] = display_df['Live PnL_num'].apply(
            lambda x: f"{x:+.2f} USDT" if pd.notna(x) else "Loading...")

        # 樣式定義與顯示
        cols = ['entry_time', 'pair', 'Action', 'Peak Z', 'Current Z', 'Live PnL', 'Time Left']
        styled_df = (display_df[cols].style
                     .map(
            lambda v: 'color: #00ff00;' if '🟢' in str(v) else ('color: #ff4b4b;' if '🔴' in str(v) else ''),
            subset=['Action', 'Peak Z', 'Current Z'])
                     .map(lambda v: 'color: #00ff00;' if '+' in str(v) else 'color: #ff4b4b;', subset=['Live PnL']))
        st.dataframe(styled_df, use_container_width=True, hide_index=True)
    else:
        st.success("✨ 市場平靜，雷達正在搜尋新的共整合機會...")

with tab2:
    if not df_log.empty:
        latest_ts = df_log['timestamp'].max()
        df_plot = df_log[df_log['timestamp'] == latest_ts].copy()
        df_plot['abs_z'] = df_plot['last_z_score'].abs()
        # 顯示當前偏離度最高的前 15 對，並標註是否為強制監控
        df_plot = df_plot.sort_values(by='abs_z', ascending=False).head(15)
        st.write("📡 當前市場最具獲利潛力組合 (或強制監控組合)：")
        st.dataframe(df_plot[['pair', 'p_value', 'correlation', 'last_z_score', 'is_active']], use_container_width=True,
                     hide_index=True)
    else:
        st.info("尚無掃描數據。")

with tab3:
    if not df_trade.empty:
        # 顯示最近 20 筆已結束的交易
        st.dataframe(df_trade[df_trade['status'] != 'OPEN'].tail(20), use_container_width=True, hide_index=True)

st.caption(f"{VERSION} | Command Tower Live Feed (Updated for v3.1.2 Sync)")