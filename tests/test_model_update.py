import logging
from src.config import Config
from src.scraper import InstagramScraper
from src.airtable_client import AirtableClient

# Configure logging
logging.basicConfig(level=logging.DEBUG)

def test_model_update():
    """
    Test to validate the data model for updating an account.
    This test will fetch data for a single account and update the
    corresponding record in Airtable.
    """
    try:
        logging.info("--- Starting Model Update Test ---")

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
        logging.info(f"Using username: {username} (ID: {account_id}) for model update test.")

        # --- Fetch account info ---
        logging.info(f"Fetching account info for {username}...")
        account_info = scraper.instagram_api.get_account_info(username)

        if not account_info or "data" not in account_info:
            logging.error(f"Failed to fetch account info for {username}")
            return

        # --- Update the account in Airtable ---
        logging.info(f"Updating account {username} in Airtable...")
        update_successful = airtable_client.update_account(account_id, account_info["data"])

        assert update_successful, f"Failed to update account {username} in Airtable."
        
        logging.info(f"Successfully updated account {username} in Airtable.")
        
        logging.info("--- Model Update Test Finished ---")

    except Exception as e:
        logging.error(f"An error occurred during the model update test: {e}", exc_info=True)

if __name__ == "__main__":
    test_model_update()
