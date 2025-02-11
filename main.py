from typing import Dict, List, Tuple, Any, TypeVar, cast
import http.client
import logging
from logging.handlers import RotatingFileHandler
import time
import sys
import json
import os
# import yaml
from typing import Dict, Any, Union, List
import yaml
from dotenv import load_dotenv
from datetime import datetime
from pyairtable import Api
from pyairtable.formulas import match

T = TypeVar('T')
KT = TypeVar('KT')
VT = TypeVar('VT')

class Config:
    def __init__(self, config_path: str = "config.yaml"):
        load_dotenv()
        self.config: Dict[str, Any] = {}
        
        # Load and parse yaml file
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing config file: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found at: {config_path}")
        
        # Resolve environment variables and setup logging
        self._resolve_env_vars()
        self._setup_logging()

    def _resolve_env_vars(self) -> None:
        """Resolve environment variables in config recursively"""
        def resolve_vars(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {
                    cast(str, k): resolve_vars(v) 
                    for k, v in cast(Dict[str, Any], obj).items()
                }
            elif isinstance(obj, list):
                return [resolve_vars(item) for item in cast(List[Any], obj)]
            elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
                env_var = obj[2:-1]
                if env_var not in os.environ:
                    raise ValueError(f"Environment variable {env_var} not set")
                return os.environ[env_var]
            return obj
        
        self.config = resolve_vars(self.config)

    def _setup_logging(self) -> None:
        """Setup logging configuration with file and console handlers"""
        log_config: Dict[str, Any] = self.config.get('logging', {})
        if not log_config:
            raise ValueError("Logging configuration not found in config file")

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        # Get log file path and create directory if needed
        log_path: str = log_config.get('file_path', './logs/scraper.log')
        log_dir: str = os.path.dirname(log_path)
        os.makedirs(log_dir, exist_ok=True)

        # Configure handlers
        handlers: List[logging.Handler] = [
            RotatingFileHandler(
                log_path,
                maxBytes=log_config.get('max_size', 10485760),  # 10MB default
                backupCount=log_config.get('backup_count', 5)
            ),
            logging.StreamHandler()  # Console handler
        ]

        # Setup formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_config.get('level', 'INFO'))

        # Remove existing handlers and add new ones
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        for handler in handlers:
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)

    def get_rate_limits(self) -> Dict[str, Union[int, float]]:
        rate_limits = self.config.get('rate_limits')
        if not rate_limits:
            raise ValueError("Rate limits configuration not found")
        return cast(Dict[str, Union[int, float]], rate_limits)

    def get_instagram_config(self) -> Dict[str, str]:
        instagram_config = self.config.get('instagram')
        if not instagram_config:
            raise ValueError("Instagram configuration not found")
        return cast(Dict[str, str], instagram_config)

    def get_bases(self) -> Dict[str, Dict[str, str]]:
        """Get base configurations with environment variable resolution"""
        try:
            bases = self.config['airtable']['bases']
            
            # For each base, check if we need to load the follower history table from env
            for base_name, base_config in bases.items():
                if 'follower_history' not in base_config:
                    follower_history_table = os.getenv('MADDISON_FOLLOWER_HISTORY_TABLE_ID')
                    if not follower_history_table:
                        raise ValueError("MADDISON_FOLLOWER_HISTORY_TABLE_ID environment variable not set")
                    base_config['follower_history'] = follower_history_table
                    
            return cast(Dict[str, Dict[str, str]], bases)
        except KeyError:
            raise ValueError("Airtable bases configuration not found")

    

    def get_airtable_api_key(self) -> str:
        try:
            api_key = self.config['airtable']['api_key']
        except KeyError:
            raise ValueError("Airtable API key not found in configuration")
        return cast(str, api_key)

    def validate_config(self) -> None:
        required_settings = {
            'rate_limits': dict,
            'instagram': dict,
            'airtable': dict,
            'logging': dict
        }

        for setting, expected_type in required_settings.items():
            if setting not in self.config:
                raise ValueError(f"Missing required configuration: {setting}")
            if not isinstance(self.config[setting], expected_type):
                raise ValueError(
                    f"Invalid type for {setting}. Expected {expected_type.__name__}, "
                    f"got {type(self.config[setting]).__name__}"
                )

class InstagramAPI:
    def __init__(self, config: Config):
        """Initialize Instagram API client with config"""
        instagram_config = config.get_instagram_config()
        self.api_key = instagram_config['api_key']
        self.host = instagram_config['host']
        self.logger = logging.getLogger(__name__)

    def _make_request(self, endpoint: str) -> Dict[str, Any]:
        """Make API request with rate limiting"""
        self.logger.debug(f"Starting API request to endpoint: {endpoint}")
        conn = http.client.HTTPSConnection(self.host, timeout=30)  # Add timeout
        headers = {
            'x-rapidapi-key': self.api_key,
            'x-rapidapi-host': self.host
        }

        try:
            self.logger.debug("Sending request...")
            conn.request("GET", endpoint, headers=headers)
            self.logger.debug("Waiting for response...")
            res = conn.getresponse()
            self.logger.debug(f"Response received: Status {res.status}")
            
            if res.status != 200:
                self.logger.error(f"API request failed ({res.status}): {res.reason}")
                return {}

            data = json.loads(res.read().decode("utf-8"))
            self.logger.debug("Response parsed successfully")
            return data

        except http.client.HTTPException as e:
            self.logger.error(f"HTTP Exception in API request: {e}")
            return {}
        except TimeoutError:
            self.logger.error("Request timed out")
            return {}
        except Exception as e:
            self.logger.error(f"API request error: {e}")
            return {}
        finally:
            conn.close()

    def get_account_info(self, username: str) -> Dict[str, Any]:
        """Get account information"""
        self.logger.info(f"Fetching account info for {username}")
        endpoint = f"/v1/info?username_or_id_or_url={username}"
        return self._make_request(endpoint)

    def get_posts(self, username: str) -> Dict[str, Any]:
        """Get posts for account"""
        self.logger.info(f"Fetching posts for {username}")
        endpoint = f"/v1/posts?username_or_id_or_url={username}"
        response = self._make_request(endpoint)
        if not response or 'data' not in response:
            return {'status': 'ok', 'items': []}
        raw_items = response.get('data', {}).get('items', [])
        
        # Debug log
        self.logger.info(f"First item play_count: {raw_items[0].get('play_count') if raw_items else 'No items'}")
        self.logger.info(f"First item ig_play_count: {raw_items[0].get('ig_play_count') if raw_items else 'No items'}")
        
        posts = []
        for item in raw_items:
            # Get play count with debug logging
            play_count = item.get('play_count', 0)
            ig_play_count = item.get('ig_play_count', 0)
            final_play_count = play_count or ig_play_count
            
            self.logger.info(f"Processing item {item.get('id')}:")
            self.logger.info(f"  - play_count: {play_count}")
            self.logger.info(f"  - ig_play_count: {ig_play_count}")
            self.logger.info(f"  - final_play_count: {final_play_count}")
            
            post = {
                'id': item.get('id'),
                'caption': item.get('caption', {}).get('text', '') if isinstance(item.get('caption'), dict) else item.get('caption', ''),
                'like_count': item.get('like_count', 0),
                'comment_count': item.get('comment_count', 0),
                'media_type': 'Reel' if item.get('media_type') == 2 else 'Image',
                'timestamp': item.get('taken_at'),
                'video_url': item.get('video_url', ''),
                'thumbnail_url': item.get('thumbnail_url') or item.get('image_versions2', {}).get('candidates', [{}])[0].get('url', ''),
                'view_count': item.get('view_count', 0),
                'play_count': final_play_count
            }
            posts.append(post)
            
            # Debug log the post
            self.logger.info(f"Created post object with play_count: {post['play_count']}")
            
        return {'status': 'ok', 'items': posts}

class AirtableClient:
    def __init__(self, api_key: str, base_config: Dict):
        """Initialize Airtable client for specific base"""
        self.logger = logging.getLogger(__name__)
        
        self.logger.debug("Initializing AirtableClient")
        self.logger.debug(f"Base config: {base_config}")
        
        self.api = Api(api_key)
        self.base_id = base_config['base_id']
        self.logger.debug(f"Base ID: {self.base_id}")
        
        # Initialize all tables
        self.accounts_table = self.api.table(self.base_id, base_config['active_accounts_table'])
        self.content_table = self.api.table(self.base_id, base_config['content_table'])
        self.view_history = self.api.table(self.base_id, base_config['view_history'])
        self.follower_history = self.api.table(self.base_id, base_config['follower_history'])
        
        self.logger.debug("AirtableClient initialization complete")


    # In AirtableClient class, modify get_active_accounts:
    def get_active_accounts(self) -> List[Tuple[str, str, int]]:  # Updated return type to include followers
        """Get active accounts from base with their follower counts"""
        try:
            formula = match({"Status": "Active"})
            records = self.accounts_table.all(formula=formula)
            
            active_accounts = [
                (
                    record['id'],
                    record['fields'].get('Username'),
                    record['fields'].get('Followers', 0)  # Get current follower count
                )
                for record in records if 'Username' in record.get('fields', {})
            ]
            
            self.logger.info(f"Found {len(active_accounts)} active accounts")
            return active_accounts

        except Exception as e:
            self.logger.error(f"Failed to fetch active accounts: {e}")
            raise

    

    def update_historical_views(self) -> None:
        """Update view history records for all content"""
        self.logger.info("Creating new view history records")
        try:
            records = self.content_table.all()
            history_records = []
            current_date: str = datetime.now().strftime("%Y-%m-%d")
            
            for record in records:
                fields = record.get('fields', {})
                current_views = fields.get('Views', 0)
                previous_views = fields.get('Previous Views', 0)
                account_links = fields.get('Account', [])  # Get the linked account(s)
                
                # Create view history record with account link
                history_record = {
                    'Date': current_date,
                    'Content ID': fields.get('ID'),
                    'Content': [record['id']],
                    'Account': account_links,  # Link to the same account(s) as the content
                    'View Count': current_views,
                    'Previous Day Views': previous_views,
                    'Daily Change': current_views - previous_views
                }
                history_records.append(history_record)
                
                # Update the content record's Previous Views
                self.content_table.update(record['id'], {
                    'Previous Views': current_views
                })
            
            # Batch create history records
            batch_size = 10
            for i in range(0, len(history_records), batch_size):
                batch = history_records[i:i + batch_size]
                self.view_history.batch_create(batch)
                self.logger.debug(f"Created history batch {i//batch_size + 1}")
                time.sleep(1)  # Rate limiting
            
            self.logger.info(f"Created {len(history_records)} view history records")
            
        except Exception as e:
            self.logger.error(f"Failed to update view history: {e}")
            raise

    def update_historical_followers(self) -> None:
        """Update follower history records for all accounts"""
        self.logger.info("Creating new follower history records")
        try:
            # Get active accounts with their current follower counts
            active_accounts = self.get_active_accounts()
            history_records = []
            current_date: str = datetime.now().strftime("%Y-%m-%d")
            
            for account_id, username, current_followers in active_accounts:
                # Create follower history record
                history_record = {
                    'Date': current_date,
                    'Account': [account_id],
                    'Follower Count': current_followers,
                    'Previous Day Followers': current_followers,  # Use current followers as previous day
                    'Daily Change': 0  # First day will show 0 change
                }
                history_records.append(history_record)
            
            # Batch create history records
            batch_size = 10
            for i in range(0, len(history_records), batch_size):
                batch = history_records[i:i + batch_size]
                self.follower_history.batch_create(batch)
                self.logger.debug(f"Created follower history batch {i//batch_size + 1}")
                time.sleep(1)  # Rate limiting
            
            self.logger.info(f"Created {len(history_records)} follower history records")
            
        except Exception as e:
            self.logger.error(f"Failed to update follower history: {e}")
            raise

    def upsert_content(self, content_data: Dict[str, Any]) -> bool:
        """Insert or update content with view history tracking"""
        try:
            content_id = content_data.get('id')
            formatted_data = self._format_content_data(content_data)
            current_views = formatted_data.get('Views', 0)
            account_links = formatted_data.get('Account', [])  # Get account links
            
            # Check if content exists
            existing_records = self.content_table.all(formula=f"ID='{content_id}'")
            
            if existing_records:
                record_id = existing_records[0]['id']
                previous_views = existing_records[0].get('fields', {}).get('Views', 0)
                
                # Update content record
                self.content_table.update(record_id, formatted_data)
                self.logger.info(f"Updated content: {content_id}")
                
                # Create view history record
                current_date: str = datetime.now().strftime("%Y-%m-%d")
                history_record = {
                    'Date': current_date,
                    'Content ID': content_id,
                    'Content': [record_id],
                    'Account': account_links,  # Add account link
                    'View Count': current_views,
                    'Previous Day Views': previous_views,
                    'Daily Change': current_views - previous_views
                }
                self.view_history.create(history_record)
                self.logger.info(f"Created view history record for content: {content_id}")
            else:
                # Create new content record
                new_record = self.content_table.create(formatted_data)
                self.logger.info(f"Created new content: {content_id}")
                
                # Create initial view history record
                current_date: str = datetime.now().strftime("%Y-%m-%d")
                history_record = {
                    'Date': current_date,
                    'Content ID': content_id,
                    'Content': [new_record['id']],
                    'Account': account_links,  # Add account link
                    'View Count': current_views,
                    'Previous Day Views': current_views,
                    'Daily Change': 0
                }
                self.view_history.create(history_record)
                self.logger.info(f"Created initial view history record for content: {content_id}")

            return True

        except Exception as e:
            self.logger.error(f"Failed to upsert content: {e}")
            return False

    def update_account(self, account_id: str, account_data: Dict[str, Any]) -> bool:
        """Update account information"""
        try:
            formatted_data = self._format_account_data(account_data)
            self.accounts_table.update(account_id, formatted_data)
            self.logger.info(f"Updated account info for {account_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to update account {account_id}: {e}")
            return False

    def _format_content_data(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format post data for Airtable"""
        caption = post_data.get('caption', {})
        if isinstance(caption, str):
            caption_text = caption
            caption_id = post_data.get('id')
        else:
            caption_text = caption.get('text', '')
            caption_id = caption.get('id', post_data.get('id'))

        media_type = post_data.get('media_type', '')
        formatted_media_type = "Reel" if media_type == 'Reel' else "Image"

        return {
            'ID': caption_id,
            'Caption': caption_text,
            'Account': [post_data.get('account_id')] if post_data.get('account_id') else None,
            'Like Count': post_data.get('like_count'),
            'Media Type': formatted_media_type,
            'Comments': post_data.get('comment_count'),
            'Content': [{"url": post_data.get('video_url')}] if post_data.get('video_url') else None,
            'Thumbnail': [{"url": post_data.get('thumbnail_url')}] if post_data.get('thumbnail_url') else None,
            'Views': post_data.get('play_count', 0)
        }

    @staticmethod
    def _format_account_data(account_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format account data for Airtable"""
        data = account_data.get('data', {})
        profile_pic = data.get('profile_pic_url_hd') or data.get('profile_pic_url')
        
        return {
            'Username': data.get('username'),
            'Bio': data.get('biography'),
            'PFP': [{"url": profile_pic}] if profile_pic else None,
            'Followers': data.get('follower_count'),
            'Following': data.get('following_count'),
            'Media Count': data.get('media_count'),
            'Full Name': data.get('full_name'),
            'Bio Link': data.get('external_url'),
            'ID': data.get('id')
        }

class InstagramScraper:
    def __init__(self, config: Config):
        """Initialize scraper with configuration"""
        self.config = config
        self.instagram_api = InstagramAPI(config)
        self.logger = logging.getLogger(__name__)
        
        # Calculate delays based on rate limits
        rate_limits = config.get_rate_limits()
        self.request_delay = 60.0 / rate_limits['requests_per_minute']
        self.account_delay = rate_limits['delay_between_accounts']
        self.post_delay = rate_limits['delay_between_posts']

    def process_all_bases(self):
        """Process all configured bases sequentially"""
        bases = self.config.get_bases()
        
        for base_name, base_config in bases.items():
            self.logger.info(f"Starting processing of base: {base_name}")
            try:
                airtable_client = AirtableClient(
                    self.config.get_airtable_api_key(),
                    base_config
                )
                self.process_base(base_name, airtable_client)
            except Exception as e:
                self.logger.error(f"Failed to process base {base_name}: {str(e)}")
                continue
            
            self.logger.info(f"Completed processing of base: {base_name}")

    def process_base(self, base_name: str, airtable_client: AirtableClient):
        """Process all accounts in a single base"""
        self.logger.debug(f"Entering process_base for {base_name}")
        try:
            # First, update historical views
            self.logger.info(f"Updating historical views for base: {base_name}")
            airtable_client.update_historical_views()
            airtable_client.update_historical_followers()

            self.logger.debug("About to call get_active_accounts")
            active_accounts = airtable_client.get_active_accounts()
            self.logger.debug("Finished calling get_active_accounts")
            
            if not active_accounts:
                self.logger.info(f"No active accounts found in base: {base_name}")
                return
            
            total_accounts = len(active_accounts)
            self.logger.info(f"Found {total_accounts} active accounts in {base_name}")
            
            for i, (account_id, username, _) in enumerate(active_accounts, 1):  # Note the _ to ignore followers here
                self.logger.info(f"[{i}/{total_accounts}] Processing account: {username}")
                try:
                    self.process_account(account_id, username, airtable_client)
                    if i < total_accounts:
                        time.sleep(self.account_delay)
                except Exception as e:
                    self.logger.error(f"Error processing account {username}: {str(e)}")
                    continue

        except Exception as e:
            self.logger.error(f"Error processing base {base_name}: {str(e)}")
            raise

    def process_account(self, account_id: str, username: str, airtable_client: AirtableClient):
        """Process a single account and its posts"""
        try:
            # Fetch and process account info
            account_info = self.instagram_api.get_account_info(username)
            if not account_info or 'data' not in account_info:
                self.logger.error(f"Failed to fetch account info for {username}")
                return
            
            airtable_client.update_account(account_id, account_info)
            time.sleep(self.request_delay)
            
            # Fetch and process posts
            posts_response = self.instagram_api.get_posts(username)
            posts = posts_response.get('items', [])
            
            for i, post in enumerate(posts, 1):
                post['account_id'] = account_id
                airtable_client.upsert_content(post)
                
                if i < len(posts):
                    time.sleep(self.post_delay)
            
            self.logger.info(f"Processed {len(posts)} posts for {username}")

        except Exception as e:
            self.logger.error(f"Error processing account {username}: {str(e)}")
            raise

def main():
    """Main function to run the Instagram scraper"""
    try:
        logging.info("🚀 Starting Instagram scraper")
        
        # Initialize and validate config
        config = Config()
        logging.info("✅ Configuration validated successfully")
        
        # Initialize scraper
        scraper = InstagramScraper(config)
        logging.info("✅ Scraper initialized")
        
        # Process all bases (now includes both view and follower history)
        logging.info("📊 Starting data collection and history tracking")
        scraper.process_all_bases()
        logging.info("✅ Successfully completed data collection and history tracking")
        
        logging.info("✨ All operations completed successfully")
    
    except Exception as e:
        logging.error(f"❌ Fatal error: {str(e)}", exc_info=True)
        import traceback
        logging.error(f"Stack trace:\n{traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    # Configure logging with timestamps
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    main()

# def test_follower_history():
#     """Test function to verify follower history functionality"""
#     try:
#         # Initialize config
#         config = Config()
#         logging.info("✅ Configuration loaded")
#
#         # Get the first base config
#         bases = config.get_bases()
#         if not bases:
#             logging.error("❌ No bases found in configuration")
#             return
#
#         base_name = next(iter(bases))
#         base_config = bases[base_name]
#         logging.info(f"🔍 Testing with base: {base_name}")
#
#         # Initialize Airtable client
#         airtable_client = AirtableClient(
#             config.get_airtable_api_key(),
#             base_config
#         )
#         logging.info("✅ Airtable client initialized")
#
#         # Test getting active accounts
#         try:
#             active_accounts = airtable_client.get_active_accounts()
#             logging.info(f"✅ Found {len(active_accounts)} active accounts")
#
#             # Print first few accounts for verification
#             for i, (account_id, username, followers) in enumerate(active_accounts[:3], 1):
#                 logging.info(f"  Account {i}: {username} (ID: {account_id}, Followers: {followers})")
#
#         except Exception as e:
#             logging.error(f"❌ Failed to get active accounts: {str(e)}")
#             return
#
#         # Test follower history update
#         try:
#             airtable_client.update_historical_followers()
#             logging.info("✅ Follower history updated successfully")
#         except Exception as e:
#             logging.error(f"❌ Failed to update follower history: {str(e)}")
#             return
#
#         logging.info("✅ All tests completed successfully")
#
#     except Exception as e:
#         logging.error(f"❌ Test failed with error: {str(e)}")
#         import traceback
#         logging.error(f"Stack trace:\n{traceback.format_exc()}")
#
# if __name__ == "__main__":
#     # Setup basic logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(levelname)s - %(message)s'
#     )
#     test_follower_history()    

