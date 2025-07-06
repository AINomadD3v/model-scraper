import http.client
import json
import logging
from typing import Any, Dict

from src.config import Config


class InstagramAPI:
    def __init__(self, config: Config):
        """Initialize Instagram API client with config"""
        instagram_config = config.get_instagram_config()
        self.api_key = instagram_config["api_key"]
        self.host = instagram_config["host"]
        self.logger = logging.getLogger(__name__)

    def _make_request(self, endpoint: str) -> Dict[str, Any]:
        """Make API request with rate limiting"""
        self.logger.debug(f"Starting API request to endpoint: {endpoint}")
        conn = http.client.HTTPSConnection(self.host, timeout=30)  # Add timeout
        headers = {"x-rapidapi-key": self.api_key, "x-rapidapi-host": self.host}

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
        if not response or "data" not in response:
            return {"status": "ok", "items": []}
        raw_items = response.get("data", {}).get("items", [])

        # Debug log
        self.logger.info(
            f"First item play_count: {raw_items[0].get('play_count') if raw_items else 'No items'}"
        )
        self.logger.info(
            f"First item ig_play_count: {raw_items[0].get('ig_play_count') if raw_items else 'No items'}"
        )

        posts = []
        for item in raw_items:
            # Get play count with debug logging
            play_count = item.get("play_count", 0)
            ig_play_count = item.get("ig_play_count", 0)
            final_play_count = play_count or ig_play_count

            self.logger.info(f"Processing item {item.get('id')}:")
            self.logger.info(f"  - play_count: {play_count}")
            self.logger.info(f"  - ig_play_count: {ig_play_count}")
            self.logger.info(f"  - final_play_count: {final_play_count}")

            post = {
                "id": item.get("id"),
                "caption": (
                    item.get("caption", {}).get("text", "")
                    if isinstance(item.get("caption"), dict)
                    else item.get("caption", "")
                ),
                "like_count": item.get("like_count", 0),
                "comment_count": item.get("comment_count", 0),
                "media_type": "Reel" if item.get("media_type") == 2 else "Image",
                "timestamp": item.get("taken_at"),
                "video_url": item.get("video_url", ""),
                "thumbnail_url": item.get("thumbnail_url")
                or item.get("image_versions2", {})
                .get("candidates", [{}])[0]
                .get("url", ""),
                "view_count": item.get("view_count", 0),
                "play_count": final_play_count,
            }
            posts.append(post)

            # Debug log the post
            self.logger.info(
                f"Created post object with play_count: {post['play_count']}"
            )

        return {"status": "ok", "items": posts}
