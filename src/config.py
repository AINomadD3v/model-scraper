import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, TypeVar, cast

import yaml
from dotenv import load_dotenv

T = TypeVar("T")
KT = TypeVar("KT")
VT = TypeVar("VT")


class Config:
    def __init__(self, config_path: str = "config.yaml"):
        load_dotenv()
        self.config: Dict[str, Any] = {}

        # Load and parse yaml file
        try:
            with open(config_path, "r") as f:
                self.config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing config file: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found at: {config_path}")

        # Resolve environment variables and setup logging
        self._resolve_env_vars()
        self._setup_logging()

    def _resolve_env_vars(self) -> None:
        """Resolve environment variables in config recursively"""

        def resolve_vars(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {
                    cast(str, k): resolve_vars(v)
                    for k, v in cast(Dict[str, Any], obj).items()
                }
            elif isinstance(obj, list):
                return [resolve_vars(item) for item in cast(List[Any], obj)]
            elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
                env_var = obj[2:-1]
                if env_var not in os.environ:
                    raise ValueError(f"Environment variable {env_var} not set")
                return os.environ[env_var]
            return obj

        self.config = resolve_vars(self.config)

    def _setup_logging(self) -> None:
        """Setup logging configuration with file and console handlers"""
        log_config: Dict[str, Any] = self.config.get("logging", {})
        if not log_config:
            raise ValueError("Logging configuration not found in config file")

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        # Get log file path and create directory if needed
        log_path: str = log_config.get("file_path", "./logs/scraper.log")
        log_dir: str = os.path.dirname(log_path)
        os.makedirs(log_dir, exist_ok=True)

        # Configure handlers
        handlers: List[logging.Handler] = [
            RotatingFileHandler(
                log_path,
                maxBytes=log_config.get("max_size", 10485760),  # 10MB default
                backupCount=log_config.get("backup_count", 5),
            ),
            logging.StreamHandler(),  # Console handler
        ]

        # Setup formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_config.get("level", "INFO"))

        # Remove existing handlers and add new ones
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        for handler in handlers:
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)

    def get_rate_limits(self) -> Dict[str, Any]:
        rate_limits = self.config.get("rate_limits")
        if not rate_limits:
            raise ValueError("Rate limits configuration not found")
        return cast(Dict[str, Any], rate_limits)

    def get_instagram_config(self) -> Dict[str, str]:
        instagram_config = self.config.get("instagram")
        if not instagram_config:
            raise ValueError("Instagram configuration not found")
        return cast(Dict[str, str], instagram_config)

    def get_bases(self) -> Dict[str, Dict[str, str]]:
        """Get base configurations with environment variable resolution"""
        try:
            bases = self.config["airtable"]["bases"]

            # For each base, check if we need to load the follower history table from env
            for base_name, base_config in bases.items():
                if "follower_history" not in base_config:
                    follower_history_table = os.getenv(
                        "ALEXIS_FOLLOWER_HISTORY_TABLE_ID"
                    )
                    if not follower_history_table:
                        raise ValueError(
                            "ALEXIS_FOLLOWER_HISTORY_TABLE_ID environment variable not set"
                        )
                    base_config["follower_history"] = follower_history_table

            return cast(Dict[str, Dict[str, str]], bases)
        except KeyError:
            raise ValueError("Airtable bases configuration not found")

    def get_airtable_api_key(self) -> str:
        try:
            api_key = self.config["airtable"]["api_key"]
        except KeyError:
            raise ValueError("Airtable API key not found in configuration")
        return cast(str, api_key)

    def validate_config(self) -> None:
        required_settings = {
            "rate_limits": dict,
            "instagram": dict,
            "airtable": dict,
            "logging": dict,
        }

        for setting, expected_type in required_settings.items():
            if setting not in self.config:
                raise ValueError(f"Missing required configuration: {setting}")
            if not isinstance(self.config[setting], expected_type):
                raise ValueError(
                    f"Invalid type for {setting}. Expected {expected_type.__name__}, "
                    f"got {type(self.config[setting]).__name__}"
                )
