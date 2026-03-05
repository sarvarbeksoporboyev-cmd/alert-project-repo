from __future__ import annotations

import pandas as pd

from alert_project.alerts import ListAlertSink
from alert_project.engine import AlertEngine
from alert_project.rules.bundle_fatal_errors import BundleFatalBurstRule
from alert_project.rules.global_fatal_errors import GlobalFatalBurstRule


def _event_frame(timestamps: list[pd.Timestamp], bundle_id: str = "bundle-a") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "severity": ["fatal"] * len(timestamps),
            "bundle_id": [bundle_id] * len(timestamps),
            "date": timestamps,
            "is_fatal": [True] * len(timestamps),
        }
    )


def test_global_rule_triggers_for_more_than_ten_in_under_one_minute() -> None:
    base = pd.Timestamp("2026-01-01T00:00:00Z")
    timestamps = [base + pd.Timedelta(seconds=5 * i) for i in range(11)]

    rule = GlobalFatalBurstRule(threshold=10)
    alerts = rule.process(_event_frame(timestamps))

    assert len(alerts) == 1
    assert alerts[0].rule_name == "global_fatal_errors_per_minute"
    assert alerts[0].metadata["count"] == 11


def test_global_rule_handles_chunk_boundaries() -> None:
    base = pd.Timestamp("2026-01-01T00:00:00Z")
    chunk_1 = _event_frame([base + pd.Timedelta(seconds=5 * i) for i in range(6)])
    chunk_2 = _event_frame([base + pd.Timedelta(seconds=5 * i) for i in range(6, 12)])

    sink = ListAlertSink()
    engine = AlertEngine(rules=[GlobalFatalBurstRule(threshold=10)], sink=sink)

    summary = engine.run([chunk_1, chunk_2])

    assert summary.alerts_emitted == 1
    assert len(sink.alerts) == 1


def test_bundle_rule_only_counts_per_bundle() -> None:
    base = pd.Timestamp("2026-01-01T00:00:00Z")

    mixed = pd.DataFrame(
        {
            "severity": ["fatal"] * 11,
            "bundle_id": ["bundle-a"] * 6 + ["bundle-b"] * 5,
            "date": [base + pd.Timedelta(minutes=i) for i in range(11)],
            "is_fatal": [True] * 11,
        }
    )

    rule = BundleFatalBurstRule(threshold=10)
    assert rule.process(mixed) == []

    bundle_a_burst = _event_frame([base + pd.Timedelta(minutes=i) for i in range(11)], bundle_id="bundle-a")
    alerts = rule.process(bundle_a_burst)

    assert len(alerts) == 1
    assert alerts[0].metadata["bundle_id"] == "bundle-a"
