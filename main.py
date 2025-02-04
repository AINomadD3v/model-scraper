from typing import Dict, List, Tuple, Any
from dotenv import load_dotenv
import http.client
import logging
import time
import sys
import json
import os
import datetime

from pyairtable import Api, Table, Base
from pyairtable.formulas import match

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Constants
RATE_LIMIT_DELAY = 1.0  # Delay between API calls

class InstagramScraper:
    def __init__(self):
        print("\n🚀 Initializing Instagram Scraper...")
        self.instagram_api = InstagramAPI()
        self.airtable_client = AirtableClient()
        print("✅ Initialization complete\n")

    def process_account(self, account_id: str, username: str):
        """Process a single account and its posts with detailed logging"""
        logging.info(f"\n📱 Processing account: {username}")
        logging.info("=" * 50)
        
        try:
            # Step 1: Fetch and process account info
            logging.info("\n1️⃣ Fetching account info...")
            account_info = self.instagram_api.get_account_info(username)
            
            if not account_info or 'data' not in account_info:
                logging.error(f"❌ Failed to fetch account info for {username}")
                return
                
            logging.debug(f"Raw account info: {json.dumps(account_info, indent=2)}")
            
            self.airtable_client.update_account(account_id, account_info)
            time.sleep(RATE_LIMIT_DELAY)
            
            # Step 2: Fetch posts
            logging.info("\n2️⃣ Fetching posts from Instagram...")
            posts_response = self.instagram_api.get_posts(username)
            
            posts = posts_response.get('items', [])
            total_posts = len(posts)
            logging.info(f"\n📊 Found {total_posts} posts to process")
            
            # Step 3: Process each post
            for i, post in enumerate(posts, 1):
                post_id = post.get('id', 'unknown')
                logging.info(f"   [{i}/{total_posts}] Processing post ID: {post_id}")
                post['account_id'] = account_id
                self.airtable_client.upsert_content(post)
                
                if i < total_posts:
                    time.sleep(0.5)  # Rate limiting between posts
            
            logging.info(f"\n✅ Successfully processed {total_posts} posts")
            logging.info("=" * 50)
        
        except Exception as e:
            logging.error(f"\n❌ Error processing account {username}: {str(e)}")
            logging.debug("Full error trace:", exc_info=True)
            raise

    def run_single_account(self):
        """Process only the first active account"""
        try:
            active_accounts = self.airtable_client.get_active_accounts()
            if not active_accounts:
                print("❌ No active accounts found")
                return
            
            account_id, username = active_accounts[0]
            print(f"\n🎯 Selected account for processing: {username}")
            self.process_account(account_id, username)
        
        except Exception as e:
            print(f"\n❌ Fatal error: {str(e)}")
            raise


class AirtableClient:
    def __init__(self):
        """Initialize Airtable client and validate credentials"""
        logging.info("🔄 Initializing Airtable client...")

        # Load environment variables
        self.api_key = os.getenv("AIRTABLE_API_KEY")
        self.base_id = os.getenv("MADDISON_BASE_ID")
        self.table_name = os.getenv("MADDISON_ACTIVE_ACCOUNTS_TABLE_ID")
        self.content_table_id = os.getenv("MADDISON_POSTED_CONTENT_TABLE_ID")

        # Validate environment variables
        if not all([self.api_key, self.base_id, self.table_name]):
            raise ValueError("❌ ERROR: Missing required environment variables for Airtable")

        # Initialize Airtable API
        self.api = Api(self.api_key)
        self.accounts_table = self.api.table(self.base_id, self.table_name)
        self.content_table = self.api.table(self.base_id, self.content_table_id)
        logging.info("✅ Airtable client initialized")

    def get_active_accounts(self) -> List[Tuple[str, str]]:
        """Retrieve all active accounts from Airtable"""
        logging.info("🔍 Fetching active accounts from Airtable...")
        try:
            formula = match({"Status": "Active"})
            records = self.accounts_table.all(formula=formula)

            active_accounts = [
                (record['id'], record['fields'].get('Username'))
                for record in records if 'Username' in record.get('fields', {})
            ]

            logging.info(f"✅ Found {len(active_accounts)} active accounts")
            return active_accounts

        except Exception as e:
            logging.error(f"❌ Failed to fetch active accounts: {e}", exc_info=True)
            return []

    def update_account(self, account_id: str, account_data: Dict[str, Any]) -> bool:
        """Update account information in Airtable"""
        print(f"\n📝 Updating account {account_id}...")
        try:
            formatted_data = self._format_account_data(account_data)
            self.accounts_table.update(account_id, formatted_data)
            print("✅ Successfully updated account info")
            return True

        except Exception as e:
            print(f"❌ Failed to update account {account_id}: {e}")
            return False

    def upsert_content(self, content_data: Dict[str, Any]) -> bool:
        """Insert or update content in Airtable"""
        try:
            formatted_data = self._format_content_data(content_data)
            content_id = formatted_data.get('ID')

            if not content_id:
                print("⚠️ Skipping content with missing ID")
                return False

            # Use content_table instead of accounts_table
            existing_records = self.content_table.all(formula=f"ID='{content_id}'")
            
            if existing_records:
                record_id = existing_records[0]['id']
                self.content_table.update(record_id, formatted_data)
                print(f"✅ Updated content: {content_id}")
            else:
                self.content_table.create(formatted_data)
                print(f"✅ Created new content: {content_id}")

            return True

        except Exception as e:
            print(f"❌ Failed to upsert content: {e}")
            return False

    @staticmethod
    def _format_content_data(post_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format post data for Airtable based on schema"""
        # Add logging for media type data
        raw_media_type = post_data.get('media_type', '')
        print(f"\n🔍 Raw media_type from post: '{raw_media_type}'")
        
        # Extract nested data safely
        caption = post_data.get('caption', {})
        if isinstance(caption, str):
            caption_text = caption
            caption_id = post_data.get('id')
        else:
            caption_text = caption.get('text', '')
            caption_id = caption.get('id', post_data.get('id'))
            
        clips_metadata = post_data.get('clips_metadata', {})
        music_info = clips_metadata.get('music_info', {})
        music_asset_info = music_info.get('music_asset_info', {})

        # Use media_type instead of media_name
        media_type = post_data.get('media_type', '')
        print(f"📝 Processing media_type: '{media_type}'")
        
        # Convert to Airtable's expected format
        formatted_media_type = "Reel" if media_type == 'Reel' else "Image"
        print(f"✅ Final media_type selected: '{formatted_media_type}'")

        return {
            'ID': caption_id,
            'Caption': caption_text,
            'Account': [post_data.get('account_id')] if post_data.get('account_id') else None,
            'Sound Artist': music_asset_info.get('display_artist'),
            'Sound Used': music_asset_info.get('audio_id'),
            'Play Count': post_data.get('ig_play_count'),
            'Like Count': post_data.get('like_count'),
            'Media Type': formatted_media_type,
            'Comments': post_data.get('comment_count'),
            'Content': [{"url": post_data.get('video_url')}] if post_data.get('video_url') else None,
            'Thumbnail': [{"url": post_data.get('thumbnail_url')}] if post_data.get('thumbnail_url') else None
        }    
    

    @staticmethod
    def _format_account_data(account_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format account data for Airtable based on schema"""
        data = account_data.get('data', {})
        # Get the best quality profile picture URL available
        profile_pic = data.get('profile_pic_url_hd') or data.get('profile_pic_url')
        return {
            'Username': data.get('username'),
            'Bio': data.get('biography'),
            'PFP': [{"url": profile_pic}] if profile_pic else None,  # Format as attachment array
            'Followers': data.get('follower_count'),
            'Following': data.get('following_count'),
            'Media Count': data.get('media_count'),
            'Full Name': data.get('full_name'),
            'Bio Link': data.get('external_url'),
            'ID': data.get('id')
        }



class InstagramAPI:
    def __init__(self):
        """Initialize Instagram API client and validate credentials"""
        load_dotenv()
        self.api_key = os.getenv("RAPIDAPI_KEY")
        self.host = "instagram-scraper-api2.p.rapidapi.com"

        if not self.api_key:
            raise ValueError("❌ ERROR: Missing RAPIDAPI_KEY in environment variables")

        print("✅ Instagram API client initialized")

    def _make_request(self, endpoint: str) -> Dict[str, Any]:
        """Helper method to make API requests with error handling"""
        conn = http.client.HTTPSConnection(self.host)
        headers = {
            'x-rapidapi-key': self.api_key,
            'x-rapidapi-host': self.host
        }

        try:
            conn.request("GET", endpoint, headers=headers)
            res = conn.getresponse()
            if res.status != 200:
                print(f"⚠️ API request failed ({res.status}): {res.reason}")
                return {}

            data = json.loads(res.read().decode("utf-8"))
            return data

        except Exception as e:
            print(f"❌ API request error: {e}")
            return {}

        finally:
            conn.close()

    def get_account_info(self, username: str) -> Dict[str, Any]:
        """Retrieve account information for a given username"""
        print(f"🔍 Fetching account info for {username}...")
        endpoint = f"/v1/info?username_or_id_or_url={username}"
        response = self._make_request(endpoint)

        if not response or not isinstance(response, dict):
            print(f"⚠️ Invalid response format for {username}")
            return {}

        return response

    def get_posts(self, username: str) -> Dict[str, Any]:
        """Retrieve posts for a given username"""
        print(f"🔍 Fetching posts for {username}...")
        endpoint = f"/v1/posts?username_or_id_or_url={username}"
        response = self._make_request(endpoint)

        if not response or 'data' not in response:
            print(f"⚠️ No posts found for {username}")
            return {'status': 'ok', 'items': []}

        raw_items = response.get('data', {}).get('items', [])
        posts = []

        for item in raw_items:
            post = {
                'id': item.get('id'),
                'caption': item.get('caption', {}).get('text', '') if isinstance(item.get('caption'), dict) else item.get('caption', ''),
                'like_count': item.get('like_count', 0),
                'comment_count': item.get('comment_count', 0),
                'media_type': 'Reel' if item.get('is_video', False) else 'Image',
                'timestamp': item.get('taken_at'),
                'video_url': item.get('video_url', ''),
                'thumbnail_url': item.get('thumbnail_url') or item.get('image_versions2', {}).get('candidates', [{}])[0].get('url', ''),
                'view_count': item.get('view_count', 0),
                'play_count': item.get('play_count', 0)
            }
            posts.append(post)

        return {'status': 'ok', 'items': posts}


def main():
    """Main function to run the Instagram scraper for a single account"""
    logging.info("🚀 Starting Instagram Scraper...")

    try:
        scraper = InstagramScraper()
        scraper.run_single_account()
        logging.info("✅ Scraping completed successfully")
    
    except Exception as e:
        logging.error(f"❌ Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
