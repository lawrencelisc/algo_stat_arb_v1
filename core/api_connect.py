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
    VERSION = "v3.1.0-Stable"

    def __init__(self):
        self.config_path = Path(__file__).resolve().parent.parent / 'config' / 'config.yaml'

    def load_bybit_api_config(self, account_name='algo_pair_trade'):
        try:
            if not self.config_path.exists():
                raise FileNotFoundError(f"Config file not found: {self.config_path}")

            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)

                # 支援超旗艦版 ACCOUNTS 區塊
                if 'ACCOUNTS' in config:
                    acc_data = config['ACCOUNTS'].get(account_name)
                    if acc_data:
                        logger.info(f"✅ Loaded {account_name} credentials from ACCOUNTS")
                        return {
                            'PT_API_KEY': acc_data.get('key'),
                            'PT_SECRET_KEY': acc_data.get('secret')
                        }

                # 備選：支援舊版 algo_pair_trade 區塊
                sub_config = config.get(account_name, {})
                if not sub_config:
                    sub_config = config.get('algo_pair_trade', {})

                if 'PT_API_KEY' in sub_config and 'PT_SECRET_KEY' in sub_config:
                    logger.info(f"✅ Loaded Bybit API config from {account_name}")
                    gc.collect()
                    return sub_config

                raise Exception(f'Account {account_name} or keys not found in config.yaml')

        except Exception as e:
            logger.error(f"❌ Error loading Bybit API config: {e}")
            raise

    def load_tg_config(self):
        try:
            if not self.config_path.exists():
                raise FileNotFoundError(f"Config file not found: {self.config_path}")

            with open(self.config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
                tg_config = config.get('tg_bot', {})
                if not tg_config:
                    tg_config = config.get('TG_BOT', {})

                required_keys = ['TOKEN', 'GROUP_ID']
                for key in required_keys:
                    if key not in tg_config or not tg_config[key]:
                        raise Exception(f'Missing or empty {key} in config.yaml')

                logger.info("✅ Loaded Telegram Bot config")
                gc.collect()
                return tg_config
        except Exception as e:
            logger.error(f"❌ Error loading Telegram config: {e}")
            raise