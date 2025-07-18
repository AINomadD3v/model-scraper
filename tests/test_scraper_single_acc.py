import logging
from src.config import Config
from src.scraper import InstagramScraper

# Configure logging
logging.basicConfig(level=logging.DEBUG)

def test_scraper_single_account():
    """
    Live test for InstagramScraper with a single, hardcoded username.
    This test will make real API calls to Instagram but will skip Airtable updates.
    """
    try:
        logging.info("--- Starting Single Account Scraper Test ---")

        # Load configuration
        config = Config(config_path='config.yaml')

        # Initialize InstagramScraper
        scraper = InstagramScraper(config)

        # Define the username to test
        username = "maddie_wave_90"
        account_id = "dummy_id" # Not used for Airtable update in this test

        # --- Run the Scraper for the single username ---
        logging.info(f"Calling process_account for {username} (skipping Airtable update)...")
        scraper.process_account(account_id=account_id, username=username, airtable_client=None, skip_airtable_update=True)
        logging.info("--- Single Account Scraper Test Finished ---")

    except Exception as e:
        logging.error(f"An error occurred during the single account scraper test: {e}", exc_info=True)

if __name__ == "__main__":
    test_scraper_single_account()