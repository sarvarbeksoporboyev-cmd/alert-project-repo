"""Core processing loop."""

from __future__ import annotations

from typing import Iterable, Sequence

import pandas as pd

from .alerts import AlertSink
from .models import ProcessSummary
from .rules.base import AlertRule


class AlertEngine:
    """Runs rules over normalized chunks and forwards alerts to sink."""

    def __init__(self, rules: Sequence[AlertRule], sink: AlertSink) -> None:
        self.rules = list(rules)
        self.sink = sink

    def run(self, chunks: Iterable[pd.DataFrame]) -> ProcessSummary:
        summary = ProcessSummary()

        for chunk in chunks:
            summary.chunks_processed += 1
            summary.rows_processed += len(chunk)

            alerts_batch = []
            for rule in self.rules:
                alerts_batch.extend(rule.process(chunk))

            summary.alerts_emitted += len(alerts_batch)
            self.sink.send(alerts_batch)

        return summary
