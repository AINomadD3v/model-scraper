import logging
from src.config import Config
from src.airtable_client import AirtableClient

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_airtable_live():
    """
    Live test for AirtableClient.
    This test will make real API calls to your Airtable base.
    """
    try:
        # Load configuration
        config = Config(config_path='config.yaml')
        airtable_api_key = config.get_airtable_api_key()
        bases = config.get_bases()
        alexis_base_config = bases.get("alexis")

        if not alexis_base_config:
            logging.error("Alexis base configuration not found in config.yaml")
            return

        # Initialize AirtableClient
        airtable_client = AirtableClient(api_key=airtable_api_key, base_config=alexis_base_config)

        # --- Test Get Active Accounts ---
        logging.info("--- Testing Get Active Accounts ---")
        active_accounts = airtable_client.get_active_accounts()
        if active_accounts:
            logging.info(f"Successfully retrieved {len(active_accounts)} active accounts.")
            for account in active_accounts:
                logging.info(f"  - Account ID: {account[0]}, Username: {account[1]}, Followers: {account[2]}")
        else:
            logging.warning("No active accounts found or an error occurred.")

        # --- Test Update Account ---
        # IMPORTANT: Replace with a REAL account ID and data to test the update.
        account_id_to_update = "REPLACE_WITH_REAL_ACCOUNT_ID"
        account_data_to_update = {
            "data": {
                "username": "test_username_from_script",
                "biography": "This is a test bio update.",
                # Add other fields you want to update
            }
        }
        
        if account_id_to_update != "REPLACE_WITH_REAL_ACCOUNT_ID":
            logging.info(f"--- Testing Update Account: {account_id_to_update} ---")
            success = airtable_client.update_account(account_id_to_update, account_data_to_update)
            if success:
                logging.info(f"Successfully updated account {account_id_to_update}.")
            else:
                logging.error(f"Failed to update account {account_id_to_update}.")
        else:
            logging.warning("Skipping account update test. Please provide a real account ID.")

    except Exception as e:
        logging.error(f"An error occurred during the live Airtable test: {e}", exc_info=True)

if __name__ == "__main__":
    test_airtable_live()
