from __future__ import annotations

import os
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

from ingestion.firms_client import FIRMSClient
from ingestion.reddit_client import RedditClient
from serialization.csv_serializer import CSVSerializer
from storage.s3_client import LocalStorageClient

load_dotenv()


class BatchPipeline:
    """Batch pipeline orchestration for ingestion and storage."""

    def __init__(
        self,
        output_dir: Optional[str] = None,
        subreddit: Optional[str] = None,
    ):
        self.output_dir = output_dir or os.getenv("OUTPUT_DIR", "csv_outputs")
        self.storage_client = LocalStorageClient(self.output_dir)
        try:
            self.firms_client = FIRMSClient()
        except ValueError:
            self.firms_client = None
        self.reddit_client = RedditClient(subreddit=subreddit)

    def run_firms(self, source: str = "VIIRS_SNPP_NRT", days: int = 1, bbox: Optional[str] = None) -> str:
        if self.firms_client is None:
            raise ValueError("FIRMS client not available - set FIRMS_API_KEY environment variable")
        df = self.firms_client.fetch(source=source, days=days, bbox=bbox)
        key = self._build_local_key("firms")
        self.storage_client.upload_bytes(key, CSVSerializer.to_csv_bytes(df))
        return key

    def run_reddit(self, limit: int = 100) -> str:
        df = self.reddit_client.fetch(limit=limit)
        key = self._build_local_key("reddit")
        self.storage_client.upload_bytes(key, CSVSerializer.to_csv_bytes(df))
        return key

    def run_all(self, reddit_limit: int = 100, firms_days: int = 1, bbox: Optional[str] = None) -> dict[str, str]:
        results = {}
        if self.firms_client:
            results["firms"] = self.run_firms(days=firms_days, bbox=bbox)
        else:
            print("Skipping FIRMS - no API key set")
        results["reddit"] = self.run_reddit(limit=reddit_limit)
        return results

    def _build_local_key(self, source: str) -> str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        return f"{source}/date={date_str}/data.csv"
