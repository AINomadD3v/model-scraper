import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

from pyairtable import Api
from pyairtable.formulas import match


class AirtableClient:
    def __init__(self, api_key: str, base_config: Dict):
        """Initialize Airtable client for a specific base."""
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing AirtableClient")
        self.api = Api(api_key)
        self.base_id = base_config["base_id"]
        self.logger.debug(f"Base ID: {self.base_id}")

        # Initialize all tables
        self.accounts_table = self.api.table(
            self.base_id, base_config["active_accounts_table"]
        )
        self.content_table = self.api.table(self.base_id, base_config["content_table"])
        self.view_history = self.api.table(self.base_id, base_config["view_history"])
        self.follower_history = self.api.table(
            self.base_id, base_config["follower_history"]
        )
        self.logger.debug("AirtableClient initialization complete")

    def get_active_accounts(self) -> List[Tuple[str, str, int]]:
        """Get active accounts from the base with their follower counts."""
        self.logger.info("Fetching active accounts")
        try:
            formula = match({"Status": "Active"})
            # Fetch only the fields we need to reduce data transfer
            records = self.accounts_table.all(
                formula=formula, fields=["Username", "Followers"]
            )

            active_accounts = [
                (
                    record["id"],
                    record["fields"].get("Username"),
                    record["fields"].get("Followers", 0),
                )
                for record in records
                if "Username" in record.get("fields", {})
            ]

            self.logger.info(f"Found {len(active_accounts)} active accounts")
            return active_accounts
        except Exception as e:
            self.logger.error(f"Failed to fetch active accounts: {e}")
            raise

    def update_historical_views(self) -> None:
        """
        Create view history records for all content and batch-update previous views.
        This method is optimized to use batch operations for improved performance.
        """
        self.logger.info("Starting historical view update process")
        try:
            # Fetch only the necessary fields to reduce data transfer
            content_records = self.content_table.all(
                fields=["ID", "Views", "Previous Views", "Account"]
            )
            self.logger.debug(f"Fetched {len(content_records)} content records")

            history_records_to_create = []
            content_records_to_update = []
            current_date = datetime.now().strftime("%Y-%m-%d")

            for record in content_records:
                fields = record.get("fields", {})
                current_views = fields.get("Views", 0)
                previous_views = fields.get("Previous Views", 0)

                # Prepare a new history record for creation
                history_records_to_create.append(
                    {
                        "Date": current_date,
                        "Content ID": fields.get("ID"),
                        "Content": [record["id"]],
                        "Account": fields.get("Account", []),
                        "View Count": current_views,
                        "Previous Day Views": previous_views,
                        "Daily Change": current_views - previous_views,
                    }
                )

                # Prepare the content record for updating its "Previous Views"
                content_records_to_update.append(
                    {"id": record["id"], "fields": {"Previous Views": current_views}}
                )

            # Batch-create all history records in a single operation
            if history_records_to_create:
                self.logger.info(
                    f"Creating {len(history_records_to_create)} view history records"
                )
                self.view_history.batch_create(history_records_to_create)
                self.logger.info("Successfully created view history records")

            # Batch-update all content records in a single operation
            if content_records_to_update:
                self.logger.info(
                    f"Updating {len(content_records_to_update)} content records"
                )
                self.content_table.batch_update(content_records_to_update)
                self.logger.info("Successfully updated content records")

        except Exception as e:
            self.logger.error(f"Failed to update view history: {e}")
            raise

    def update_historical_followers(self) -> None:
        """
        Create follower history records for all active accounts using a batch operation.
        """
        self.logger.info("Creating new follower history records")
        try:
            active_accounts = self.get_active_accounts()
            history_records = []
            current_date = datetime.now().strftime("%Y-%m-%d")

            for account_id, _, current_followers in active_accounts:
                # Find the latest follower record to calculate daily change
                latest_history = self.follower_history.first(
                    formula=f"AND(Account='{account_id}', Date < '{current_date}')",
                    sort=["-Date"],
                )

                previous_followers = (
                    latest_history["fields"].get("Follower Count", 0)
                    if latest_history
                    else current_followers
                )

                history_records.append(
                    {
                        "Date": current_date,
                        "Account": [account_id],
                        "Follower Count": current_followers,
                        "Previous Day Followers": previous_followers,
                        "Daily Change": current_followers - previous_followers,
                    }
                )

            # Batch-create all follower history records at once
            if history_records:
                self.logger.info(
                    f"Creating {len(history_records)} follower history records"
                )
                self.follower_history.batch_create(history_records)
                self.logger.info("Successfully created follower history records")

        except Exception as e:
            self.logger.error(f"Failed to update follower history: {e}")
            raise

    def upsert_content(self, content_data: Dict[str, Any]) -> bool:
        """
        Insert or update a single piece of content and its view history.
        Optimized to fetch minimal data for existence checks.
        """
        try:
            content_id = content_data.get("id")
            formatted_data = self._format_content_data(content_data)
            current_views = formatted_data.get("Views", 0)

            # Check if content exists, fetching only the 'Views' field
            existing_record = self.content_table.first(
                formula=f"ID='{content_id}'", fields=["Views"]
            )

            if existing_record:
                # --- UPDATE PATH ---
                record_id = existing_record["id"]
                previous_views = existing_record.get("fields", {}).get("Views", 0)

                self.content_table.update(record_id, formatted_data)
                self.logger.info(f"Updated content: {content_id}")

                # Create a corresponding view history record
                history_record = {
                    "Date": datetime.now().strftime("%Y-%m-%d"),
                    "Content ID": content_id,
                    "Content": [record_id],
                    "Account": formatted_data.get("Account", []),
                    "View Count": current_views,
                    "Previous Day Views": previous_views,
                    "Daily Change": current_views - previous_views,
                }
                self.view_history.create(history_record)
                self.logger.debug(f"Created view history for updated content")

            else:
                # --- CREATE PATH ---
                new_record = self.content_table.create(formatted_data)
                self.logger.info(f"Created new content: {content_id}")

                # Create the initial view history record
                history_record = {
                    "Date": datetime.now().strftime("%Y-%m-%d"),
                    "Content ID": content_id,
                    "Content": [new_record["id"]],
                    "Account": formatted_data.get("Account", []),
                    "View Count": current_views,
                    "Previous Day Views": 0,  # No previous views for new content
                    "Daily Change": current_views,
                }
                self.view_history.create(history_record)
                self.logger.debug(f"Created initial view history for new content")

            return True

        except Exception as e:
            self.logger.error(f"Failed to upsert content for ID {content_id}: {e}")
            return False

    def update_account(self, account_id: str, account_data: Dict[str, Any]) -> bool:
        """Update account information."""
        try:
            formatted_data = self._format_account_data(account_data)
            self.accounts_table.update(account_id, formatted_data)
            self.logger.info(f"Updated account info for {account_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to update account {account_id}: {e}")
            return False

    def _format_content_data(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format post data for Airtable."""
        caption = post_data.get("caption", {})
        caption_text = caption if isinstance(caption, str) else caption.get("text", "")
        content_id = post_data.get("id") or (
            caption.get("id") if isinstance(caption, dict) else None
        )

        return {
            "ID": content_id,
            "Caption": caption_text,
            "Account": (
                [post_data["account_id"]] if post_data.get("account_id") else []
            ),
            "Like Count": post_data.get("like_count"),
            "Media Type": "Reel" if post_data.get("media_type") == "REEL" else "Image",
            "Comments": post_data.get("comment_count"),
            "Content": (
                [{"url": post_data["video_url"]}] if post_data.get("video_url") else []
            ),
            "Thumbnail": (
                [{"url": post_data["thumbnail_url"]}]
                if post_data.get("thumbnail_url")
                else []
            ),
            "Views": post_data.get("play_count", 0),
        }

    @staticmethod
    def _format_account_data(account_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format account data for Airtable."""
        profile_pic = account_data.get("profile_pic_url_hd") or account_data.get(
            "profile_pic_url"
        )
        return {
            "Username": account_data.get("username"),
            "Bio": account_data.get("biography"),
            "PFP": [{"url": profile_pic}] if profile_pic else [],
            "Followers": account_data.get("follower_count"),
            "Following": account_data.get("following_count"),
            "Media Count": account_data.get("media_count"),
            "Full Name": account_data.get("full_name"),
            "Bio Link": account_data.get("external_url"),
            "ID": str(account_data.get("id")),
        }
