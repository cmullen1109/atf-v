import requests
import pandas as pd
from datetime import datetime


class BaseClient:
    """Base client with shared utilities for ingestion modules."""

    def __init__(self, source: str = None):
        self.source = source or self.__class__.__name__.lower()

    @staticmethod
    def _utcnow():
        return datetime.utcnow()

    def _validate_response(self, response):
        if response.status_code != 200:
            raise Exception(
                f"{self.source} API request failed: {response.status_code} - {response.text}"
            )

    def _get(self, url, headers=None, **kwargs):
        return requests.get(url, headers=headers, **kwargs)

    def _read_csv(self, text):
        return pd.read_csv(pd.io.common.StringIO(text))
