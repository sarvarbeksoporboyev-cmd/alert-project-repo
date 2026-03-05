from __future__ import annotations

import csv

from alert_project.benchmark import run_once


def test_run_once_returns_throughput_metrics(tmp_path) -> None:
    csv_path = tmp_path / "benchmark_sample.csv"

    header = [str(index) for index in range(24)]
    rows = []
    for offset in range(5):
        row = ["x"] * 24
        row[2] = "fatal"
        row[15] = "bundle-1"
        row[23] = str(1_700_000_000 + offset)
        rows.append(row)

    with csv_path.open("w", newline="", encoding="utf-8") as destination:
        writer = csv.writer(destination)
        writer.writerow(header)
        writer.writerows(rows)

    metrics = run_once(
        input_path=csv_path,
        chunksize=2,
        fatal_severity="fatal",
        global_threshold=10,
        bundle_threshold=10,
    )

    assert metrics["rows_processed"] == 5.0
    assert metrics["rows_with_valid_date"] == 5.0
    assert metrics["elapsed_seconds"] > 0.0
    assert metrics["rows_per_second"] > 0.0
    assert metrics["projected_rows_per_day"] > 0.0