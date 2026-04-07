from __future__ import annotations

import pandas as pd


class CSVSerializer:
    """Simple CSV serializer for DataFrame payloads."""

    @staticmethod
    def to_csv_string(df: pd.DataFrame) -> str:
        return df.to_csv(index=False)

    @staticmethod
    def to_csv_bytes(df: pd.DataFrame, encoding: str = "utf-8") -> bytes:
        return CSVSerializer.to_csv_string(df).encode(encoding)

    @staticmethod
    def write_csv(df: pd.DataFrame, path: str, encoding: str = "utf-8") -> None:
        df.to_csv(path, index=False, encoding=encoding)
