import logging
from src.config import Config
from src.scraper import InstagramScraper
from src.airtable_client import AirtableClient

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_scraper_live():
    """
    Live test for InstagramScraper.
    This test will make real API calls to Instagram and Airtable.
    """
    try:
        logging.info(f"""--- Starting Live Scraper Test ---""")

        # Load configuration
        config = Config(config_path='config.yaml')
        airtable_config = config.get_airtable_config()

        # Initialize AirtableClient
        airtable_client = AirtableClient(
            api_key=airtable_config["api_key"],
            base_id=airtable_config["base_id"],
            active_accounts_table=airtable_config["active_accounts_table"],
        )

        # Initialize InstagramScraper
        scraper = InstagramScraper(config)

        # --- Get a single username from Airtable ---
        logging.info("Fetching a single active account from Airtable...")
        active_accounts = airtable_client.get_active_accounts(max_records=1)

        if not active_accounts:
            logging.warning("No active accounts found in Airtable to test with.")
            return

        account_id, username, _ = active_accounts[0]
        logging.info(f"Using username: {username} (ID: {account_id}) for scraper test.")

        # --- Run the Scraper for the single username ---
        logging.info(f"Calling process_account for {username}...")
        scraper.process_account(account_id, username, airtable_client)
        logging.info("""--- Scraper Test Finished ---""")

    except Exception as e:
        logging.error(f"An error occurred during the live scraper test: {e}", exc_info=True)

if __name__ == "__main__":
    test_scraper_live()