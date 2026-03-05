"""Bundle-scoped fatal spike rule (>N fatal events in <1 hour)."""

from __future__ import annotations

from collections import deque
from datetime import timedelta

import pandas as pd

from ..models import Alert
from .base import AlertRule


class BundleFatalBurstRule(AlertRule):
    """Triggers when one bundle exceeds fatal threshold inside a 1-hour window."""

    name = "bundle_fatal_errors_per_hour"

    def __init__(self, threshold: int = 10, window: timedelta | None = None) -> None:
        self.threshold = threshold
        self.window = window or timedelta(hours=1)
        self._timestamps_by_bundle: dict[str, deque[pd.Timestamp]] = {}
        self._above_threshold_by_bundle: dict[str, bool] = {}

    def process(self, events: pd.DataFrame) -> list[Alert]:
        alerts: list[Alert] = []

        fatal_events = events.loc[events["is_fatal"], ["bundle_id", "date"]].dropna().sort_values("date")
        if fatal_events.empty:
            self._prune_stale_bundles(events["date"].max())
            return alerts

        for row in fatal_events.itertuples(index=False):
            bundle_id = str(row.bundle_id)
            timestamp = row.date

            queue = self._timestamps_by_bundle.setdefault(bundle_id, deque())
            queue.append(timestamp)
            self._prune_old_entries(queue, timestamp)

            current_count = len(queue)
            is_above = self._above_threshold_by_bundle.get(bundle_id, False)

            if current_count > self.threshold and not is_above:
                window_start = queue[0]
                alerts.append(
                    Alert(
                        rule_name=self.name,
                        triggered_at=timestamp.to_pydatetime(warn=False),
                        message=(
                            f"Bundle {bundle_id} exceeded {self.threshold} fatal errors "
                            f"in under {int(self.window.total_seconds())} seconds"
                        ),
                        metadata={
                            "bundle_id": bundle_id,
                            "count": current_count,
                            "window_seconds": int(self.window.total_seconds()),
                            "window_start": window_start.isoformat(),
                            "window_end": timestamp.isoformat(),
                        },
                    )
                )
                self._above_threshold_by_bundle[bundle_id] = True
            elif current_count <= self.threshold:
                self._above_threshold_by_bundle[bundle_id] = False

        self._prune_stale_bundles(fatal_events["date"].max())
        return alerts

    def _prune_old_entries(self, queue: deque[pd.Timestamp], current_timestamp: pd.Timestamp) -> None:
        while queue and (current_timestamp - queue[0]) >= self.window:
            queue.popleft()

    def _prune_stale_bundles(self, watermark: pd.Timestamp | float) -> None:
        if not isinstance(watermark, pd.Timestamp):
            return

        empty_keys: list[str] = []
        for bundle_id, queue in self._timestamps_by_bundle.items():
            self._prune_old_entries(queue, watermark)
            if not queue:
                empty_keys.append(bundle_id)

        for bundle_id in empty_keys:
            self._timestamps_by_bundle.pop(bundle_id, None)
            self._above_threshold_by_bundle.pop(bundle_id, None)
