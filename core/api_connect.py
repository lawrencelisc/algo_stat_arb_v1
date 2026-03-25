import yaml
import os
import gc
from pathlib import Path
from loguru import logger


class DataBridge:
    """
    [STAGE 0] 數據橋樑模組 (Data Bridge)
    負責從外部配置檔案（config.yaml）安全地讀取 API 金鑰與通訊配置。
    """
    VERSION = "v3.0.0-Stable"

    def __init__(self):
        # 獲取專案根目錄下的配置路徑
        self.config_path = Path(__file__).resolve().parent.parent / 'config' / 'config.yaml'

    def load_bybit_api_config(self):
        """
        從 config.yaml 載入 Bybit API 配置
        對標區塊: algo_pair_trade
        """
        try:
            if not self.config_path.exists():
                raise FileNotFoundError(f"Config file not found: {self.config_path}")

            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)

                # 指定讀取子區塊
                bybit_sub = 'algo_pair_trade'
                bybit_sub_api = config.get(bybit_sub, {})

                # 驗證必要欄位
                required_keys = ['PT_API_KEY', 'PT_SECRET_KEY']
                for key in required_keys:
                    if key not in bybit_sub_api or not bybit_sub_api[key]:
                        raise Exception(f'Missing or empty {bybit_sub}.{key} in config.yaml')

                logger.info(f"✅ Successfully loaded Bybit API config from {bybit_sub}")

                # 記憶體清理並回傳
                gc.collect()
                return bybit_sub_api

        except Exception as e:
            logger.error(f"❌ Error occurred while loading Bybit API config: {e}")
            raise Exception(f'Error loading Bybit config: {e}')

    def load_tg_config(self):
        """
        [NEW] 從 config.yaml 載入 Telegram Bot 配置
        對標區塊: tg_bot (TOKEN, GROUP_ID)
        """
        try:
            if not self.config_path.exists():
                raise FileNotFoundError(f"Config file not found: {self.config_path}")

            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)

                # 讀取 Telegram 區塊
                tg_config = config.get('tg_bot', {})

                # 驗證必要欄位 (TOKEN 與 GROUP_ID)
                required_keys = ['TOKEN', 'GROUP_ID']
                for key in required_keys:
                    if key not in tg_config or not tg_config[key]:
                        raise Exception(f'Missing or empty tg_bot.{key} in config.yaml')

                logger.info("✅ Successfully loaded Telegram Bot config")

                # 記憶體清理並回傳
                gc.collect()
                return tg_config

        except Exception as e:
            logger.error(f"❌ Error occurred while loading Telegram config: {e}")
            raise Exception(f'Error loading TG config: {e}')