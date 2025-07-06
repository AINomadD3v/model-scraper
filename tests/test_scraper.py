import logging
from src.config import Config
from src.scraper import InstagramScraper

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_scraper_live():
    """
    Live test for InstagramScraper.
    This test will make real API calls to Instagram and Airtable.
    """
    try:
        # --- Configuration ---
        # Set this to True to scrape content (posts).
        # Set to False to only update account profiles.
        scrape_content = False

        logging.info(f"--- Starting Live Scraper Test (scrape_content={scrape_content}) ---")

        # Load configuration
        config = Config(config_path='config.yaml')

        # Initialize InstagramScraper
        scraper = InstagramScraper(config, scrape_content=scrape_content)

        # --- Run the Scraper ---
        logging.info("Calling process_all_bases...")
        scraper.process_all_bases()
        logging.info("--- Scraper Test Finished ---")

    except Exception as e:
        logging.error(f"An error occurred during the live scraper test: {e}", exc_info=True)

if __name__ == "__main__":
    test_scraper_live()
