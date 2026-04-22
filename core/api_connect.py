import yaml
from pathlib import Path
from loguru import logger


class DataBridge:
    """
    [STAGE 0] 數據橋樑模組 (Data Bridge)
    負責從外部配置檔案（config.yaml）安全地讀取 API 金鑰與通訊配置。
    """
    VERSION = "v3.2.0-Robust"

    def __init__(self):
        root = Path(__file__).resolve().parent.parent
        # 與 execution.py 一致：優先找 config/ 子目錄，找不到退回根目錄
        config_in_dir = root / 'config' / 'config.yaml'
        config_in_root = root / 'config.yaml'
        if config_in_dir.exists():
            self.config_path = config_in_dir
        elif config_in_root.exists():
            self.config_path = config_in_root
        else:
            # 保留路徑供後續讀取時拋出明確的 FileNotFoundError
            self.config_path = config_in_dir

    def _load_yaml(self):
        """讀取並解析 config.yaml，找不到時拋出明確錯誤。"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def load_bybit_api_config(self, account_name='algo_pair_trade'):
        # 不在此層 catch-log-reraise，避免呼叫方看到重複日誌
        config = self._load_yaml()

        # 優先：ACCOUNTS 區塊（新版格式）
        if 'ACCOUNTS' in config:
            acc_data = config['ACCOUNTS'].get(account_name)
            if acc_data:
                logger.info(f"✅ Loaded '{account_name}' credentials from ACCOUNTS block.")
                return {
                    'PT_API_KEY': acc_data.get('key'),
                    'PT_SECRET_KEY': acc_data.get('secret')
                }
            # ACCOUNTS 存在但找不到指定帳號 → 明確報錯，不靜默 fallthrough
            raise KeyError(
                f"Account '{account_name}' not found in ACCOUNTS block. "
                f"Available: {list(config['ACCOUNTS'].keys())}"
            )

        # 備選：舊版具名區塊（account_name 或 algo_pair_trade）
        sub_config = config.get(account_name) or config.get('algo_pair_trade', {})
        if 'PT_API_KEY' in sub_config and 'PT_SECRET_KEY' in sub_config:
            logger.info(f"✅ Loaded Bybit API config from '{account_name}' block.")
            return sub_config

        raise KeyError(f"Account '{account_name}' or required keys not found in config.yaml")

    def load_tg_config(self):
        # 不在此層 catch-log-reraise，避免呼叫方看到重複日誌
        config = self._load_yaml()

        tg_config = config.get('tg_bot') or config.get('TG_BOT', {})

        required_keys = ['TOKEN', 'GROUP_ID']
        for key in required_keys:
            if not tg_config.get(key):
                raise KeyError(f"Missing or empty '{key}' in tg_bot section of config.yaml")

        logger.info("✅ Loaded Telegram Bot config.")
        return tg_config