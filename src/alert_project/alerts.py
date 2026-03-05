"""Alert sinks (outputs)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol

from .models import Alert


class AlertSink(Protocol):
    """Sink interface for emitted alerts."""

    def send(self, alerts: list[Alert]) -> None:
        ...


class JsonLineAlertSink:
    """Writes alerts to a JSONL file."""

    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def send(self, alerts: list[Alert]) -> None:
        if not alerts:
            return

        with self.output_path.open("a", encoding="utf-8") as destination:
            for alert in alerts:
                destination.write(json.dumps(alert.to_record(), ensure_ascii=True))
                destination.write("\n")


class ListAlertSink:
    """In-memory sink useful for testing."""

    def __init__(self) -> None:
        self.alerts: list[Alert] = []

    def send(self, alerts: list[Alert]) -> None:
        self.alerts.extend(alerts)
