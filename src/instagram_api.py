import logging
from typing import Any, Dict

import requests

from src.config import Config


class InstagramAPI:
    def __init__(self, config: Config):
        """Initialize Instagram API client with config"""
        instagram_config = config.get_instagram_config()
        self.api_key = instagram_config["api_key"]
        self.host = instagram_config["host"]
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def _make_request(self, endpoint: str) -> Dict[str, Any]:
        """Make API request with rate limiting"""
        url = f"https://{self.host}{endpoint}"
        self.logger.debug(f"Starting API request to endpoint: {url}")
        headers = {"x-rapidapi-key": self.api_key, "x-rapidapi-host": self.host}

        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            self.logger.debug("Response parsed successfully")
            return response.json()

        except requests.exceptions.Timeout:
            self.logger.error("Request timed out")
            return {}
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request error: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {e}")
            return {}

    def get_account_info(self, username: str) -> Dict[str, Any]:
        """Get account information"""
        self.logger.info(f"Fetching account info for {username}")
        endpoint = f"/v1/info?username_or_id_or_url={username}"
        return self._make_request(endpoint)
