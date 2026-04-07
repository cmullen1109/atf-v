import os
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from ingestion.firms_client import FIRMSClient
from ingestion.reddit_client import RedditClient


class TestFIRMSClient(unittest.TestCase):
    @patch("ingestion.base_client.requests.get")
    def test_fetch_success(self, mock_get):
        os.environ["FIRMS_API_KEY"] = "TEST_KEY"
        mock_response = MagicMock(status_code=200, text="latitude,longitude\n10,20\n")
        mock_get.return_value = mock_response

        client = FIRMSClient()
        df = client.fetch(days=1, bbox="-170,15,-50,75")

        self.assertEqual(len(df), 1)
        self.assertTrue("ingestion_timestamp" in df.columns)
        self.assertEqual(df.iloc[0]["latitude"], 10)
        self.assertEqual(df.iloc[0]["longitude"], 20)

    @patch("ingestion.base_client.requests.get")
    def test_fetch_failure_raises(self, mock_get):
        os.environ["FIRMS_API_KEY"] = "TEST_KEY"
        mock_response = MagicMock(status_code=500, text="Server error")
        mock_get.return_value = mock_response

        client = FIRMSClient()
        with self.assertRaises(Exception):
            client.fetch()


class TestRedditClient(unittest.TestCase):
    @patch("ingestion.base_client.requests.get")
    def test_fetch_success(self, mock_get):
        payload = {
            "data": {
                "children": [
                    {
                        "data": {
                            "id": "abc123",
                            "title": "Test post",
                            "selftext": "Body text",
                            "created_utc": 1700000000,
                            "score": 42,
                            "url": "https://reddit.com/r/wildfires/comments/abc123",
                            "num_comments": 3,
                            "author": "tester",
                        }
                    }
                ]
            }
        }

        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = payload
        mock_get.return_value = mock_response

        client = RedditClient(subreddit="wildfires")
        df = client.fetch(limit=1)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["source"], "reddit")
        self.assertIn("Test post", df.iloc[0]["raw_text"])
        self.assertIsNotNone(df.iloc[0]["event_timestamp"])

    @patch("ingestion.base_client.requests.get")
    def test_fetch_empty_returns_dataframe(self, mock_get):
        payload = {"data": {"children": []}}

        mock_response = MagicMock(status_code=200)
        mock_response.json.return_value = payload
        mock_get.return_value = mock_response

        client = RedditClient(subreddit="wildfires")
        df = client.fetch(limit=1)

        self.assertEqual(len(df), 0)


if __name__ == "__main__":
    unittest.main()
