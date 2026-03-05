"""CLI entrypoint for log processing."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .alerts import JsonLineAlertSink
from .config import DEFAULT_CHUNK_SIZE, DEFAULT_FATAL_SEVERITY
from .engine import AlertEngine
from .ingestion import normalize_chunk, read_log_chunks
from .rules.bundle_fatal_errors import BundleFatalBurstRule
from .rules.global_fatal_errors import GlobalFatalBurstRule


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Process CSV logs and emit alerts.")
    parser.add_argument("--input", type=Path, default=Path("data.csv"), help="Input CSV file path")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/alerts.jsonl"),
        help="Output JSONL file path",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Number of rows per chunk",
    )
    parser.add_argument(
        "--fatal-severity",
        type=str,
        default=DEFAULT_FATAL_SEVERITY,
        help="Severity value to treat as fatal",
    )
    parser.add_argument(
        "--global-threshold",
        type=int,
        default=10,
        help="Threshold for fatal errors in <1 minute",
    )
    parser.add_argument(
        "--bundle-threshold",
        type=int,
        default=10,
        help="Threshold for fatal errors per bundle in <1 hour",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    if not args.input.exists():
        raise FileNotFoundError(f"Input file does not exist: {args.input}")

    sink = JsonLineAlertSink(args.output)
    rules = [
        GlobalFatalBurstRule(threshold=args.global_threshold),
        BundleFatalBurstRule(threshold=args.bundle_threshold),
    ]

    engine = AlertEngine(rules=rules, sink=sink)

    raw_rows = 0
    rows_with_valid_date = 0

    def normalized_stream():
        nonlocal raw_rows, rows_with_valid_date
        for raw_chunk in read_log_chunks(args.input, chunksize=args.chunksize):
            raw_rows += len(raw_chunk)
            normalized = normalize_chunk(raw_chunk, fatal_severity=args.fatal_severity)
            rows_with_valid_date += len(normalized)
            yield normalized

    summary = engine.run(normalized_stream())
    summary.rows_processed = raw_rows
    summary.rows_with_valid_date = rows_with_valid_date

    logging.info("Processing completed")
    logging.info("Chunks processed: %s", summary.chunks_processed)
    logging.info("Rows processed: %s", summary.rows_processed)
    logging.info("Rows with valid date: %s", summary.rows_with_valid_date)
    logging.info("Alerts emitted: %s", summary.alerts_emitted)
    logging.info("Alerts written to: %s", args.output)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
