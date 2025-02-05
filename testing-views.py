from typing import Dict, Any, List
import logging
from logging.handlers import RotatingFileHandler
import time
import sys
import os
import yaml
from dotenv import load_dotenv
from pyairtable import Api

class Config:
    def __init__(self, config_path: str = "config.yaml"):
        load_dotenv()  # Load environment variables first
        self.config: Dict[str, Any] = {}
        
        # Load and parse yaml file
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing config file: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found at: {config_path}")
        
        # Resolve environment variables
        self._resolve_env_vars()
        self._setup_logging()
        
        print("Config after resolution:", self.config)  # Debug print

    def _resolve_env_vars(self) -> None:
        """Resolve environment variables in config recursively"""
        def resolve_vars(obj: Any) -> Any:
            if isinstance(obj, dict):
                return {k: resolve_vars(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [resolve_vars(item) for item in obj]
            elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
                env_var = obj[2:-1]
                print(f"Resolving env var: {env_var} = {os.environ.get(env_var, 'NOT_FOUND')}")  # Debug
                if env_var not in os.environ:
                    raise ValueError(f"Environment variable {env_var} not set")
                return os.environ[env_var]
            return obj
        
        self.config = resolve_vars(self.config)

    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        log_config = self.config.get('logging', {})
        if not log_config:
            raise ValueError("Logging configuration not found in config file")

        logging.basicConfig(
            level=log_config.get('level', 'INFO'),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def get_bases(self) -> Dict[str, Dict[str, str]]:
        try:
            return self.config['airtable']['bases']
        except KeyError:
            raise ValueError("Airtable bases configuration not found")

    def get_airtable_api_key(self) -> str:
        try:
            return self.config['airtable']['api_key']
        except KeyError:
            raise ValueError("Airtable API key not found in configuration")

class AirtableClient:
    def __init__(self, api_key: str, base_config: Dict):
        self.logger = logging.getLogger(__name__)
        self.api = Api(api_key)
        self.base_id = base_config['base_id']
        self.content_table = self.api.table(self.base_id, base_config['content_table'])
        print(f"Initialized AirtableClient with base_id: {self.base_id}")  # Debug print

    def update_historical_views(self) -> None:
        """Update historical view counts"""
        self.logger.info("Updating historical view counts")
        try:
            records = self.content_table.all()
            updates = []
            
            for record in records:
                fields = record.get('fields', {})
                current_views = fields.get('Views', fields.get('View Count'))
                
                if current_views is not None:
                    updates.append({
                        'id': record['id'],
                        'fields': {
                            'Previous Views': current_views
                        }
                    })
            
            batch_size = 10
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                self.content_table.batch_update(batch)
                self.logger.debug(f"Updated batch {i//batch_size + 1}")
                time.sleep(1)
            
            self.logger.info(f"Updated {len(updates)} records")
            
        except Exception as e:
            self.logger.error(f"Failed to update historical views: {e}")
            raise

def main():
    """Update historical views for all bases"""
    try:
        config = Config()
        logging.info("✅ Configuration loaded successfully")
        
        bases = config.get_bases()
        api_key = config.get_airtable_api_key()
        
        for base_name, base_config in bases.items():
            logging.info(f"Processing base: {base_name}")
            try:
                print(f"Base config for {base_name}: {base_config}")  # Debug print
                airtable_client = AirtableClient(api_key, base_config)
                airtable_client.update_historical_views()
                logging.info(f"✅ Completed processing base: {base_name}")
            except Exception as e:
                logging.error(f"Failed to process base {base_name}: {str(e)}")
                continue
        
        logging.info("✅ All bases processed successfully")
    
    except Exception as e:
        logging.error(f"❌ Fatal error: {str(e)}")
        import traceback
        logging.error(f"Stack trace:\n{traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
