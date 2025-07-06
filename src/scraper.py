import logging
import time

from pyairtable.formulas import match

from src.airtable_client import AirtableClient
from src.config import Config
from src.instagram_api import InstagramAPI


class InstagramScraper:
    def __init__(self, config: Config, scrape_content: bool = False):
        self.config = config
        self.instagram_api = InstagramAPI(config)
        self.logger = logging.getLogger(__name__)

        self.scrape_content = scrape_content

        # Rate limits
        rate_limits = config.get_rate_limits()
        self.request_delay = 60.0 / rate_limits["requests_per_minute"]
        self.account_delay = rate_limits["delay_between_accounts"]
        self.post_delay = rate_limits["delay_between_posts"]

    def process_all_bases(self):
        """Process all configured bases sequentially"""
        bases = self.config.get_bases()

        for base_name, base_config in bases.items():
            self.logger.info(f"Starting processing of base: {base_name}")
            try:
                airtable_client = AirtableClient(
                    self.config.get_airtable_api_key(), base_config
                )
                self.process_base(base_name, airtable_client)
            except Exception as e:
                self.logger.error(f"Failed to process base {base_name}: {str(e)}")
                continue

            self.logger.info(f"Completed processing of base: {base_name}")

    def process_base(self, base_name: str, airtable_client: AirtableClient):
        """Process all accounts in a single base"""
        self.logger.debug(f"Entering process_base for {base_name}")
        try:
            # First, update historical views
            if self.scrape_content:
                self.logger.info(f"Updating historical views for base: {base_name}")
                airtable_client.update_historical_views()
                airtable_client.update_historical_followers()
            else:
                self.logger.info(
                    f"Skipping view and follower history for base: {base_name}"
                )

            self.logger.debug("About to call get_active_accounts")
            active_accounts = airtable_client.get_active_accounts()
            self.logger.debug("Finished calling get_active_accounts")

            if not active_accounts:
                self.logger.info(f"No active accounts found in base: {base_name}")
                return

            total_accounts = len(active_accounts)
            self.logger.info(f"Found {total_accounts} active accounts in {base_name}")

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
            self.logger.error(f"Error processing base {base_name}: {str(e)}")
            raise

    def process_account(
        self, account_id: str, username: str, airtable_client: AirtableClient
    ):
        """Process a single account's profile and optionally its posts"""
        try:
            # Always fetch and update account/profile info
            self.logger.info(f"Fetching profile for {username}")
            account_info = self.instagram_api.get_account_info(username)

            if not account_info or "data" not in account_info:
                self.logger.error(f"Failed to fetch account info for {username}")
                return

            airtable_client.update_account(account_id, account_info)
            self.logger.info(f"Updated profile for {username}")
            time.sleep(self.request_delay)

            # Only proceed to post scraping if scrape_content is True
            if not self.scrape_content:
                self.logger.info(f"Skipping content scraping for {username}")
                return

            # Fetch and process posts
            self.logger.info(f"Fetching posts for {username}")
            posts_response = self.instagram_api.get_posts(username)
            posts = posts_response.get("items", [])

            for i, post in enumerate(posts, 1):
                post["account_id"] = account_id
                airtable_client.upsert_content(post)

                if i < len(posts):
                    time.sleep(self.post_delay)

            self.logger.info(f"Processed {len(posts)} posts for {username}")

        except Exception as e:
            self.logger.error(f"Error processing account {username}: {str(e)}")
            raise
