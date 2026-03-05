"""Alert and summary models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class Alert:
    """Represents one triggered alert."""

    rule_name: str
    triggered_at: datetime
    message: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "triggered_at": self.triggered_at.isoformat(),
            "message": self.message,
            "metadata": self.metadata,
        }


@dataclass(slots=True)
class ProcessSummary:
    """Aggregated runtime information for one execution."""

    rows_processed: int = 0
    rows_with_valid_date: int = 0
    chunks_processed: int = 0
    alerts_emitted: int = 0
