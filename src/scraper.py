import logging
import time

from src.airtable_client import AirtableClient
from src.config import Config
from src.instagram_api import InstagramAPI


class InstagramScraper:
    def __init__(self, config: Config):
        self.config = config
        self.instagram_api = InstagramAPI(config)
        self.logger = logging.getLogger(__name__)

        # Rate limits
        rate_limits = config.get_rate_limits()
        self.request_delay = 60.0 / rate_limits["requests_per_minute"]
        self.account_delay = rate_limits["delay_between_accounts"]

    def process_all_bases(self):
        """Process all configured bases sequentially"""
        airtable_config = self.config.get_airtable_config()
        airtable_client = AirtableClient(
            api_key=airtable_config["api_key"],
            base_id=airtable_config["base_id"],
            active_accounts_table=airtable_config["active_accounts_table"],
        )
        self.process_base(airtable_client)

    def process_base(self, airtable_client: AirtableClient):
        """Process all accounts in a single base"""
        self.logger.debug(f"Entering process_base")
        try:
            self.logger.debug("About to call get_active_accounts")
            active_accounts = airtable_client.get_active_accounts()
            self.logger.debug("Finished calling get_active_accounts")

            if not active_accounts:
                self.logger.info(f"No active accounts found in base")
                return

            total_accounts = len(active_accounts)
            self.logger.info(f"Found {total_accounts} active accounts")

            for i, (account_id, username, _) in enumerate(
                active_accounts, 1
            ):  # Note the _ to ignore followers here
                self.logger.info(
                    f"[{i}/{total_accounts}] Processing account: {username}"
                )
                try:
                    self.process_account(account_id, username, airtable_client)
                    if i < total_accounts:
                        time.sleep(self.account_delay)
                except Exception as e:
                    self.logger.error(f"Error processing account {username}: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"Error processing base: {str(e)}")
            raise

    def process_account(
        self, account_id: str, username: str, airtable_client: AirtableClient | None = None, skip_airtable_update: bool = False
    ) -> bool:
        """
        Process a single account's profile.
        Returns True on success, False on failure.
        """
        try:
            # Always fetch and update account/profile info
            self.logger.info(f"Fetching profile for {username}")
            account_info = self.instagram_api.get_account_info(username)

            if not account_info or "data" not in account_info:
                error_message = f"Failed to fetch account info for {username}"
                self.logger.error(error_message)
                if airtable_client and not skip_airtable_update:
                    airtable_client.log_error(account_id, error_message)
                return False

            if not skip_airtable_update and airtable_client:
                airtable_client.update_account(account_id, account_info["data"])
                self.logger.info(f"Updated profile for {username}")
            time.sleep(self.request_delay)
            return True

        except Exception as e:
            error_message = f"Error processing account {username}: {str(e)}"
            self.logger.error(error_message)
            if airtable_client and not skip_airtable_update:
                airtable_client.log_error(account_id, error_message)
            return False