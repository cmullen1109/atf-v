import os
from typing import Optional


class LocalStorageClient:
    """Local file storage client for writing CSV payloads to a local folder."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = output_dir or os.getenv("OUTPUT_DIR", "csv_outputs")
        os.makedirs(self.output_dir, exist_ok=True)

    def upload_bytes(self, key: str, body: bytes, content_type: str = "text/csv") -> None:
        full_path = os.path.join(self.output_dir, key)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(body)

    def upload_file(self, key: str, filename: str, content_type: str = "text/csv") -> None:
        full_path = os.path.join(self.output_dir, key)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(filename, "rb") as src, open(full_path, "wb") as dst:
            dst.write(src.read())
