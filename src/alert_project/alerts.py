"""Alert sinks (outputs)."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .models import Alert

LOGGER = logging.getLogger(__name__)


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


class WebhookAlertSink:
    """Sends alerts to an HTTP webhook endpoint."""

    def __init__(
        self,
        url: str,
        timeout_seconds: float = 5.0,
        max_retries: int = 2,
        backoff_seconds: float = 0.5,
    ) -> None:
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    def send(self, alerts: list[Alert]) -> None:
        if not alerts:
            return

        payload = json.dumps({"alerts": [alert.to_record() for alert in alerts]}).encode("utf-8")
        request = Request(
            self.url,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "alert-project/0.1"},
            method="POST",
        )

        for attempt in range(self.max_retries + 1):
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    status_code = getattr(response, "status", response.getcode())
                    if 200 <= status_code < 300:
                        return
                    raise URLError(f"Webhook returned non-success status: {status_code}")
            except (HTTPError, URLError, OSError, TimeoutError) as error:
                is_last_attempt = attempt == self.max_retries
                if is_last_attempt:
                    LOGGER.error("Failed to deliver alerts to webhook %s: %s", self.url, error)
                    return
                sleep_seconds = self.backoff_seconds * (2**attempt)
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)


class CompositeAlertSink:
    """Fan-out sink that forwards alerts to multiple sinks."""

    def __init__(self, sinks: list[AlertSink]) -> None:
        self.sinks = sinks

    def send(self, alerts: list[Alert]) -> None:
        for sink in self.sinks:
            sink.send(alerts)


class NullAlertSink:
    """No-op sink useful for pure throughput benchmarks."""

    def send(self, alerts: list[Alert]) -> None:
        _ = alerts


class ListAlertSink:
    """In-memory sink useful for testing."""

    def __init__(self) -> None:
        self.alerts: list[Alert] = []

    def send(self, alerts: list[Alert]) -> None:
        self.alerts.extend(alerts)