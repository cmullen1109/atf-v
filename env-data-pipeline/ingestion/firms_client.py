import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from .base_client import BaseClient

load_dotenv()


class FIRMSClient(BaseClient):
    """
    Client for ingesting wildfire data from NASA FIRMS API.
    Returns raw data with minimal normalization.
    """

    BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

    def __init__(self):
        super().__init__("firms")
        self.api_key = os.getenv("FIRMS_API_KEY")
        if not self.api_key:
            raise ValueError("FIRMS_API_KEY not found in environment variables")

    def fetch(self, source="VIIRS_SNPP_NRT", days=1, bbox=None) -> pd.DataFrame:
        if not bbox:
            bbox = "-170,15,-50,75"

        url = f"{self.BASE_URL}/{self.api_key}/{source}/{bbox}/{days}"
        print(f"[FIRMS] Fetching data from: {url}")

        response = self._get(url)
        self._validate_response(response)

        df = self._read_csv(response.text)
        df["ingestion_timestamp"] = datetime.utcnow()

        print(f"[FIRMS] Retrieved {len(df)} records")
        return df


if __name__ == "__main__":
    client = FIRMSClient()
    df = client.fetch()
    print(df.head())
