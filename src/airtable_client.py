import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

from pyairtable import Api
from pyairtable.formulas import AND, match


class AirtableClient:
    def __init__(self, api_key: str, base_id: str, active_accounts_table: str):
        """Initialize Airtable client for a specific base."""
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing AirtableClient")
        self.api = Api(api_key)
        self.base_id = base_id
        self.logger.debug(f"Base ID: {self.base_id}")

        # Initialize all tables
        self.accounts_table = self.api.table(self.base_id, active_accounts_table)
        self.logger.debug("AirtableClient initialization complete")

    def get_active_accounts(self, max_records: int = 0) -> List[Tuple[str, str, int]]:
        """Get active accounts from the base with their follower counts."""
        self.logger.info("Fetching active accounts")
        try:
            # Fetch only the fields we need to reduce data transfer
            records = self.accounts_table.all(
                formula=match({"Status": "Active"}),
                fields=["Username", "Followers"],
                max_records=max_records,
            )

            active_accounts = [
                (
                    record["id"],
                    record["fields"].get("Username"),
                    record["fields"].get("Followers", 0),
                )
                for record in records
                if "Username" in record.get("fields", {})
            ]

            self.logger.info(f"Found {len(active_accounts)} active accounts")
            return active_accounts
        except Exception as e:
            self.logger.error(f"Failed to fetch active accounts: {e}")
            raise

    def update_account(self, account_id: str, account_data: Dict[str, Any]) -> bool:
        """Update account information."""
        try:
            formatted_data = self._format_account_data(account_data)
            self.accounts_table.update(account_id, formatted_data)
            self.logger.info(f"Updated account info for {account_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to update account {account_id}: {e}")
            return False

    def log_error(self, account_id: str, error_message: str) -> bool:
        """Log an error message to the account's 'API Error' field."""
        try:
            self.accounts_table.update(
                account_id, {"API Error": error_message, "Scraped": True}
            )
            self.logger.info(f"Logged error for account {account_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to log error for account {account_id}: {e}")
            return False

    @staticmethod
    def _format_account_data(account_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format account data for Airtable."""
        profile_pic = account_data.get("profile_pic_url_hd") or account_data.get(
            "profile_pic_url"
        )
        return {
            "Username": account_data.get("username"),
            "Bio": account_data.get("biography"),
            "PFP": [{"url": profile_pic}] if profile_pic else [],
            "Followers": account_data.get("follower_count"),
            "Following": account_data.get("following_count"),
            "Media Count": account_data.get("media_count"),
            "Full Name": account_data.get("full_name"),
            "Bio Link": account_data.get("external_url"),
            "Scraped": True,
            "API Error": "",  # Clear error on successful update
        }
