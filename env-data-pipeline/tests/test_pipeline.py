import os
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from serialization.csv_serializer import CSVSerializer
from storage.s3_client import LocalStorageClient
from pipelines.batch_pipeline import BatchPipeline


class TestCSVSerializer(unittest.TestCase):
    def test_to_csv_bytes(self):
        import pandas as pd

        df = pd.DataFrame([{"a": 1, "b": 2}])
        result = CSVSerializer.to_csv_bytes(df)

        self.assertTrue(result.startswith(b"a,b"))
        self.assertIn(b"1,2", result)


class TestLocalStorageClient(unittest.TestCase):
    @patch("builtins.open", new_callable=unittest.mock.mock_open)
    @patch("os.makedirs")
    def test_upload_bytes_creates_file(self, mock_makedirs, mock_open):
        client = LocalStorageClient(output_dir="csv_outputs")
        client.upload_bytes("path/to/data.csv", b"hello,world")

        # makedirs called for output_dir in __init__ and for the path
        self.assertEqual(mock_makedirs.call_count, 2)
        mock_makedirs.assert_any_call("csv_outputs", exist_ok=True)
        mock_makedirs.assert_any_call("csv_outputs/path/to", exist_ok=True)
        mock_open.assert_called_once_with("csv_outputs/path/to/data.csv", "wb")
        mock_open().write.assert_called_once_with(b"hello,world")


class TestBatchPipeline(unittest.TestCase):
    @patch("pipelines.batch_pipeline.LocalStorageClient")
    @patch("pipelines.batch_pipeline.FIRMSClient")
    @patch("pipelines.batch_pipeline.RedditClient")
    def test_run_all_uploads_data(self, mock_reddit_cls, mock_firms_cls, mock_storage_cls):
        mock_firms = MagicMock()
        mock_firms.fetch.return_value = __import__("pandas").DataFrame([{"a": 1}])
        mock_firms_cls.return_value = mock_firms

        mock_reddit = MagicMock()
        mock_reddit.fetch.return_value = __import__("pandas").DataFrame([{"b": 2}])
        mock_reddit_cls.return_value = mock_reddit

        mock_storage = MagicMock()
        mock_storage_cls.return_value = mock_storage

        pipeline = BatchPipeline(output_dir="csv_outputs", subreddit="wildfires")
        result = pipeline.run_all(reddit_limit=1, firms_days=1)

        self.assertIn("firms", result)
        self.assertIn("reddit", result)
        self.assertEqual(mock_storage.upload_bytes.call_count, 2)

        mock_firms = MagicMock()
        mock_firms.fetch.return_value = __import__("pandas").DataFrame([{"a": 1}])
        mock_firms_cls.return_value = mock_firms

        mock_reddit = MagicMock()
        mock_reddit.fetch.return_value = __import__("pandas").DataFrame([{"b": 2}])
        mock_reddit_cls.return_value = mock_reddit

        mock_storage = MagicMock()
        mock_storage_cls.return_value = mock_storage

        pipeline = BatchPipeline(output_dir="csv_outputs", subreddit="wildfires")
        result = pipeline.run_all(reddit_limit=1, firms_days=1)

        self.assertIn("firms", result)
        self.assertIn("reddit", result)
        self.assertEqual(mock_storage.upload_bytes.call_count, 2)


if __name__ == "__main__":
    unittest.main()
