import logging

from src.airtable_client import AirtableClient
from src.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)


def test_airtable_live():
    """
    Live test for AirtableClient.
    This test will make real API calls to your Airtable base.
    """
    try:
        # Load configuration
        config = Config(config_path="config.yaml")
        airtable_config = config.get_airtable_config()

        # Initialize AirtableClient
        airtable_client = AirtableClient(
            api_key=airtable_config["api_key"],
            base_id=airtable_config["base_id"],
            active_accounts_table=airtable_config["active_accounts_table"],
        )

        # --- Test Get Active Accounts ---
        logging.info("--- Testing Get Active Accounts ---")
        active_accounts = airtable_client.get_active_accounts(max_records=10)
        assert len(active_accounts) == 10
        if active_accounts:
            logging.info(
                f"Successfully retrieved {len(active_accounts)} active accounts."
            )
            for account in active_accounts:
                logging.info(
                    f"  - Account ID: {account[0]}, Username: {account[1]}, Followers: {account[2]}"
                )
        else:
            logging.warning("No active accounts found or an error occurred.")

    except Exception as e:
        logging.error(
            f"An error occurred during the live Airtable test: {e}", exc_info=True
        )


if __name__ == "__main__":
    test_airtable_live()
