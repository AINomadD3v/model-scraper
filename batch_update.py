import logging
import time

from src.airtable_client import AirtableClient
from src.config import Config
from src.scraper import InstagramScraper

# Configure logging
logging.basicConfig(level=logging.INFO)


def batch_update():
    """
    Fetches a small batch of accounts from Airtable, scrapes their data,
    and updates their records.
    """
    try:
        logging.info("--- Starting Batch Update Script ---")

        # Load configuration
        config = Config(config_path="config.yaml")
        airtable_config = config.get_airtable_config()

        # Initialize AirtableClient
        airtable_client = AirtableClient(
            api_key=airtable_config["api_key"],
            base_id=airtable_config["base_id"],
            active_accounts_table=airtable_config["active_accounts_table"],
        )

        # Initialize InstagramScraper
        scraper = InstagramScraper(config)

        # --- Fetch a small batch of accounts from Airtable ---
        logging.info("Fetching a batch of 80 active accounts from Airtable...")
        active_accounts = airtable_client.get_active_accounts(max_records=80)

        if not active_accounts:
            logging.warning("No active accounts found in Airtable to process.")
            return

        total_accounts = len(active_accounts)
        logging.info(f"Found {total_accounts} accounts to process.")

        # --- Process each account ---
        for i, (account_id, username, _) in enumerate(active_accounts, 1):
            if not username:
                logging.warning(
                    f"Skipping account with ID {account_id} due to missing username."
                )
                continue

            logging.info(
                f"[{i}/{total_accounts}] Processing account: {username} (ID: {account_id})"
            )
            success = scraper.process_account(account_id, username, airtable_client)
            if success:
                logging.info(f"Successfully processed and updated {username}.")
            else:
                logging.error(f"Failed to process account {username}.")

            if i < total_accounts:
                logging.info(
                    f"Waiting for {scraper.account_delay} seconds before next account."
                )
                time.sleep(scraper.account_delay)

        logging.info("--- Batch Update Script Finished ---")

    except Exception as e:
        logging.error(
            f"An error occurred during the batch update script: {e}", exc_info=True
        )


if __name__ == "__main__":
    batch_update()
