import requests
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

# 引入 DataBridge 以實現模組化配置讀取
try:
    from core.api_connect import DataBridge
except ImportError:
    # 處理路徑識別問題，確保能找到核心模組
    import sys

    root_path = Path(__file__).resolve().parent.parent
    if str(root_path) not in sys.path:
        sys.path.append(str(root_path))
    from core.api_connect import DataBridge


class TelegramReporter:
    """
    [STAGE 5 & 6] Telegram 通報模組 (Communication Officer)
    負責格式化並發送系統信號、執行結果、異常警報與每日戰報。
    """

    VERSION = "v3.0.0-Stable"

    def __init__(self):
        """
        初始化通訊官：透過 DataBridge 載入配置
        """
        try:
            db = DataBridge()
            tg_config = db.load_tg_config()

            self.token = tg_config.get('TOKEN')
            self.chat_id = tg_config.get('GROUP_ID')
            self.api_url = f"https://api.telegram.org/bot{self.token}/sendMessage"

            if self.token and self.chat_id:
                logger.info("📱 Telegram Reporter initialized via DataBridge.")
            else:
                logger.warning("⚠️ Telegram configuration is incomplete. Check config.yaml")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Telegram Reporter: {e}")
            self.token, self.chat_id = None, None

    def _send(self, text):
        """
        底層發送邏輯
        :param text: 發送的文字內容 (支援 Markdown)
        """
        if not self.token or not self.chat_id:
            return None

        payload = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': 'Markdown'
        }

        try:
            # 加入 timeout 避免因網路問題卡死主程序
            response = requests.post(self.api_url, data=payload, timeout=10)
            if response.status_code != 200:
                logger.error(f"❌ TG API Error: {response.text}")
            return response.json()
        except Exception as e:
            logger.error(f"🚨 Telegram connection failed: {e}")
            return None

    # ==========================================
    # 🎯 訊號觸發通報 (Signal Alert + Beta Drift)
    # ==========================================
    def send_signal_alert(self, pair, z_score, side1, side2, beta, drift=0.0):
        """
        當雷達偵測到 Z-Score 達標時發送
        包含 Stage 6 的 Beta 漂移警示
        """
        drift_msg = f"\n⚠️ *BETA DRIFT:* '{drift:.1%}'" if drift > 0.15 else ""

        msg = (
            f"🎯 *SIGNAL DETECTED*\n"
            f"───────────────────\n"
            f"📊 *Pair:* '{pair}'\n"
            f"📉 *Z-Score:* '{z_score:+.4f}'\n"
            f"⚡ *Action:* {side1} Y / {side2} X\n"
            f"🧮 *Beta:* '{beta:.4f}'{drift_msg}\n"
            f"⏰ *Time:* {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC"
        )
        self._send(msg)

    # ==========================================
    # ⚓ 資金費率攔截通報 (Funding Guard Alert)
    # ==========================================
    def send_funding_alert(self, pair, current_rate, threshold):
        """
        [NEW] 當 Funding Guard 攔截高成本交易時發送
        這能讓艦長即時知道為何訊號被取消
        """
        msg = (
            f"⚓ *FUNDING GUARD ALERT*\n"
            f"───────────────────\n"
            f"📦 *Pair:* '{pair}'\n"
            f"💰 *Current Rate:* '{current_rate:.4%}' / 8h\n"
            f"⚠️ *Threshold:* '{threshold:.4%}' / 8h\n"
            f"🛑 *Status:* 'SIGNAL CANCELLED'\n"
            f"🚀 _Reason: High carry cost (Interest) detected._"
        )
        self._send(msg)

    # ==========================================
    # ✅ 執行結果通報 (Execution Report)
    # ==========================================
    def send_execution_report(self, pair, p1, p2, q1, q2, slippage, balance):
        """當開倉成功後發送成交回報"""
        msg = (
            f"✅ *TRADE EXECUTED*\n"
            f"───────────────────\n"
            f"📦 *Pair:* '{pair}'\n"
            f"💵 *Prices:* '{p1:.4f}' | '{p2:.4f}'\n"
            f"🔢 *Quant:* '{q1}' | '{q2}'\n"
            f"📉 *Slippage:* '{slippage:.3%}'\n"
            f"💰 *Wallet:* '{balance:.2f} USDT'\n"
            f"🚀 _Order status: EXECUTED via IOC_"
        )
        self._send(msg)

    # ==========================================
    # 🚨 系統異常警報 (Critical Error & Kill Switch)
    # ==========================================
    def send_error_alert(self, error_code, module, status="System Paused"):
        """當系統發生嚴重錯誤或觸發 Kill Switch 時發送"""
        msg = (
            f"🚨 *SYSTEM ALERT*\n"
            f"───────────────────\n"
            f"❌ *Error:* '{error_code}'\n"
            f"🏗️ *Module:* '{module}'\n"
            f"⚠️ *Status:* '{status}'\n"
            f"🔥 _Immediate attention required!_"
        )
        self._send(msg)

    # ==========================================
    # 💓 週期性「平安信」 (Heartbeat Pulse)
    # ==========================================
    def send_heartbeat(self, pnl, active_pairs, uptime):
        """確保系統運作正常的定期通報"""
        msg = (
            f"💓 *HEARTBEAT PULSE*\n"
            f"───────────────────\n"
            f"📈 *Daily PnL:* '{pnl:+.2%}'\n"
            f"📂 *Active Pairs:* '{active_pairs}'\n"
            f"🕒 *Uptime:* '{uptime}'\n"
            f"✅ _System is breathing normally._"
        )
        self._send(msg)

    # ==========================================
    # 📈 每日獲利與費用報告 (Daily Performance Report)
    # ==========================================
    def send_daily_report(self, total_pnl, fees, funding, win_rate, active_count):
        """
        [FINAL STAGE] 每日戰報統計
        """
        net_pnl = total_pnl - fees - funding
        status_emoji = "📈" if net_pnl >= 0 else "📉"

        msg = (
            f"{status_emoji} *DAILY PERFORMANCE REPORT*\n"
            f"───────────────────\n"
            f"💰 *Net Profit:* '{net_pnl:+.2f} USDT'\n"
            f"📊 *Realized PnL:* '{total_pnl:+.2f}'\n"
            f"💸 *Trading Fees:* '{fees:.2f}'\n"
            f"⚓ *Funding Paid:* '{funding:.2f}'\n"
            f"───────────────────\n"
            f"🏆 *Win Rate:* '{win_rate:.1%}'\n"
            f"📂 *Active Pairs:* '{active_count}'\n"
            f"🕒 *Report Time:* {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            f"🚀 _Stay disciplined, Captain._"
        )
        self._send(msg)