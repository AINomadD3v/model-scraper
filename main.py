import argparse
import logging
import sys
import traceback

from pyairtable.formulas import match

from src.airtable_client import AirtableClient
from src.config import Config
from src.scraper import InstagramScraper


def main():
    """Main function to run the Instagram scraper"""
    try:
        parser = argparse.ArgumentParser(description="Instagram scraper")
        parser.add_argument(
            "--username", type=str, help="Process a single username (profile only)"
        )
        args = parser.parse_args()

        logging.info("🚀 Starting Instagram scraper")

        config = Config()
        logging.info("✅ Configuration validated successfully")

        # Initialize scraper with flag
        scraper = InstagramScraper(config)
        logging.info("✅ Scraper initialized")

        if args.username:
            # Single account profile scraping mode
            logging.info(f"🔍 Scraping profile for {args.username}")
            airtable_config = config.get_airtable_config()
            airtable_client = AirtableClient(
                api_key=airtable_config["api_key"],
                base_id=airtable_config["base_id"],
                active_accounts_table=airtable_config["active_accounts_table"],
            )
            # Attempt to find account ID from Airtable by username
            records = airtable_client.accounts_table.all(
                formula=match({"Username": args.username})
            )
            if not records:
                logging.warning(
                    f"⚠️ Username '{args.username}' not found in base"
                )
            else:
                account_id = records[0]["id"]
                scraper.process_account(account_id, args.username, airtable_client)
                logging.info("✅ Single account profile scraping completed")

        else:
            # Normal full-base mode
            logging.info("📊 Starting data collection and history tracking")
            scraper.process_all_bases()
            logging.info(
                "✅ Successfully completed data collection and history tracking"
            )

        logging.info("✨ All operations completed successfully")

    except Exception as e:
        logging.error(f"❌ Fatal error: {str(e)}", exc_info=True)
        logging.error(f"Stack trace:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    # Configure logging with timestamps
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    main()
