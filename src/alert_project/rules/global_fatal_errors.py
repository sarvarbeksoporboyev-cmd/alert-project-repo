"""Global fatal spike rule (>N fatal events in <1 minute)."""

from __future__ import annotations

from collections import deque
from datetime import timedelta

import pandas as pd

from ..models import Alert
from .base import AlertRule


class GlobalFatalBurstRule(AlertRule):
    """Triggers when fatal events exceed threshold inside a 1-minute window."""

    name = "global_fatal_errors_per_minute"

    def __init__(self, threshold: int = 10, window: timedelta | None = None) -> None:
        self.threshold = threshold
        self.window = window or timedelta(minutes=1)
        self._timestamps: deque[pd.Timestamp] = deque()
        self._above_threshold = False

    def process(self, events: pd.DataFrame) -> list[Alert]:
        alerts: list[Alert] = []

        fatal_timestamps = events.loc[events["is_fatal"], "date"].dropna().sort_values()
        if fatal_timestamps.empty:
            return alerts

        for timestamp in fatal_timestamps:
            self._timestamps.append(timestamp)
            self._prune_old_entries(timestamp)

            current_count = len(self._timestamps)
            if current_count > self.threshold and not self._above_threshold:
                window_start = self._timestamps[0]
                alerts.append(
                    Alert(
                        rule_name=self.name,
                        triggered_at=timestamp.to_pydatetime(warn=False),
                        message=(
                            f"Fatal errors exceeded {self.threshold} in under "
                            f"{int(self.window.total_seconds())} seconds"
                        ),
                        metadata={
                            "count": current_count,
                            "window_seconds": int(self.window.total_seconds()),
                            "window_start": window_start.isoformat(),
                            "window_end": timestamp.isoformat(),
                        },
                    )
                )
                self._above_threshold = True
            elif current_count <= self.threshold:
                self._above_threshold = False

        return alerts

    def _prune_old_entries(self, current_timestamp: pd.Timestamp) -> None:
        while self._timestamps and (current_timestamp - self._timestamps[0]) >= self.window:
            self._timestamps.popleft()
