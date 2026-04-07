import json
import pandas as pd
from datetime import datetime

from .base_client import BaseClient


class RedditClient(BaseClient):
    """Client for ingesting Reddit posts from a subreddit."""

    BASE_URL = "https://www.reddit.com"
    DEFAULT_SUBREDDIT = "wildfires"
    DEFAULT_LIMIT = 100
    USER_AGENT = "env-data-pipeline/1.0 (by /u/yourusername)"

    def __init__(self, subreddit: str = None):
        super().__init__("reddit")
        self.subreddit = subreddit or self.DEFAULT_SUBREDDIT

    def fetch(self, subreddit: str = None, limit: int = None) -> pd.DataFrame:
        subreddit = subreddit or self.subreddit
        limit = limit or self.DEFAULT_LIMIT

        url = f"{self.BASE_URL}/r/{subreddit}/new.json?limit={limit}"
        response = self._get(url, headers={"User-Agent": self.USER_AGENT})
        self._validate_response(response)

        payload = response.json()
        posts = payload.get("data", {}).get("children", [])

        rows = []
        for post in posts:
            data = post.get("data", {})
            raw_text = data.get("title", "")
            selftext = data.get("selftext", "")
            if selftext:
                raw_text = f"{raw_text}\n\n{selftext}"

            rows.append({
                "source": "reddit",
                "ingestion_timestamp": self._utcnow(),
                "event_timestamp": datetime.utcfromtimestamp(data.get("created_utc")) if data.get("created_utc") else None,
                "lat": None,
                "lon": None,
                "raw_text": raw_text,
                "raw_payload": json.dumps(data, default=str),
                "metadata": json.dumps({
                    "subreddit": subreddit,
                    "score": data.get("score"),
                    "id": data.get("id"),
                    "url": data.get("url"),
                    "num_comments": data.get("num_comments"),
                    "author": data.get("author"),
                }, default=str),
            })

        return pd.DataFrame(rows)
