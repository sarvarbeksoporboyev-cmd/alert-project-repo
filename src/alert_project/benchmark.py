"""Benchmark command for throughput and capacity estimation."""

from __future__ import annotations

import argparse
import statistics
import time
from pathlib import Path

from .alerts import NullAlertSink
from .engine import AlertEngine
from .ingestion import normalize_chunk, read_log_chunks
from .rules.bundle_fatal_errors import BundleFatalBurstRule
from .rules.global_fatal_errors import GlobalFatalBurstRule

SECONDS_PER_DAY = 86_400


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark alert processing throughput.")
    parser.add_argument("--input", type=Path, default=Path("data.csv"), help="Input CSV file path")
    parser.add_argument("--chunksize", type=int, default=200_000, help="Rows per chunk")
    parser.add_argument("--fatal-severity", type=str, default="fatal", help="Severity treated as fatal")
    parser.add_argument("--global-threshold", type=int, default=10, help="Global threshold")
    parser.add_argument("--bundle-threshold", type=int, default=10, help="Bundle threshold")
    parser.add_argument("--repeat", type=int, default=1, help="Number of repeated runs")
    return parser


def run_once(
    input_path: Path,
    chunksize: int,
    fatal_severity: str,
    global_threshold: int,
    bundle_threshold: int,
) -> dict[str, float]:
    raw_rows = 0
    rows_with_valid_date = 0

    rules = [
        GlobalFatalBurstRule(threshold=global_threshold),
        BundleFatalBurstRule(threshold=bundle_threshold),
    ]
    engine = AlertEngine(rules=rules, sink=NullAlertSink())

    def normalized_stream():
        nonlocal raw_rows, rows_with_valid_date
        for raw_chunk in read_log_chunks(input_path, chunksize=chunksize):
            raw_rows += len(raw_chunk)
            normalized = normalize_chunk(raw_chunk, fatal_severity=fatal_severity)
            rows_with_valid_date += len(normalized)
            yield normalized

    started = time.perf_counter()
    summary = engine.run(normalized_stream())
    elapsed = time.perf_counter() - started

    rows_per_second = raw_rows / elapsed if elapsed else 0.0
    projected_rows_per_day = rows_per_second * SECONDS_PER_DAY

    return {
        "elapsed_seconds": elapsed,
        "rows_processed": float(raw_rows),
        "rows_with_valid_date": float(rows_with_valid_date),
        "alerts_emitted": float(summary.alerts_emitted),
        "rows_per_second": rows_per_second,
        "projected_rows_per_day": projected_rows_per_day,
    }


def main() -> int:
    args = build_parser().parse_args()

    if not args.input.exists():
        raise FileNotFoundError(f"Input file does not exist: {args.input}")

    runs: list[dict[str, float]] = []
    for run_index in range(1, args.repeat + 1):
        metrics = run_once(
            input_path=args.input,
            chunksize=args.chunksize,
            fatal_severity=args.fatal_severity,
            global_threshold=args.global_threshold,
            bundle_threshold=args.bundle_threshold,
        )
        runs.append(metrics)
        print(
            f"run={run_index} elapsed={metrics['elapsed_seconds']:.3f}s "
            f"rows={int(metrics['rows_processed'])} rows_per_second={metrics['rows_per_second']:.2f} "
            f"projected_rows_per_day={metrics['projected_rows_per_day']:.0f}"
        )

    avg_rows_per_second = statistics.mean(metric["rows_per_second"] for metric in runs)
    avg_projected_rows_per_day = statistics.mean(metric["projected_rows_per_day"] for metric in runs)

    print("summary")
    print(f"runs={len(runs)}")
    print(f"avg_rows_per_second={avg_rows_per_second:.2f}")
    print(f"avg_projected_rows_per_day={avg_projected_rows_per_day:.0f}")
    print(f"meets_100m_per_day={avg_projected_rows_per_day >= 100_000_000}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())