from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class RawRecord:
    source: str
    ingestion_timestamp: datetime
    event_timestamp: Optional[datetime] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    raw_text: Optional[str] = None
    raw_payload: Optional[str] = None
    metadata: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "ingestion_timestamp": self.ingestion_timestamp,
            "event_timestamp": self.event_timestamp,
            "lat": self.lat,
            "lon": self.lon,
            "raw_text": self.raw_text,
            "raw_payload": self.raw_payload,
            "metadata": self.metadata,
        }
