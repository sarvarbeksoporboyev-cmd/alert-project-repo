# Alert Project

Streaming log analyzer for CSV mobile app logs with pluggable alert rules.

## Implemented alert rules
1. More than 10 fatal errors in less than 1 minute (global).
2. More than 10 fatal errors in less than 1 hour for the same `bundle_id`.

## Why this architecture
- Stream processing with `pandas.read_csv(..., chunksize=...)` keeps memory bounded and supports high-volume datasets.
- Rules are stateful objects implementing one common interface, so new rules can be added without changing ingestion or output code.
- Alert sinks are separate from rules (JSONL file sink, list sink for tests), so output destinations can evolve independently.
- Date-window counting uses deques for O(n) behavior over event streams.

## Project layout
- `src/alert_project/ingestion.py`: CSV parsing and normalization.
- `src/alert_project/engine.py`: orchestration loop.
- `src/alert_project/rules/`: pluggable rule implementations.
- `src/alert_project/alerts.py`: alert sinks.
- `src/alert_project/cli.py`: command-line entrypoint.
- `tests/`: unit tests for rules and chunk-boundary behavior.

## Local run
```bash
python -m venv .venv
. .venv/Scripts/Activate.ps1
pip install -r requirements.txt
pip install -e .[dev]
python -m alert_project.cli --input data.csv --output output/alerts.jsonl --chunksize 200000
```

## Docker run (single command)
```bash
docker compose up --build
```

Generated alerts are written to `output/alerts.jsonl`.

## How to add a new alert rule
1. Create a class in `src/alert_project/rules/` that inherits `AlertRule` and implements `process(events: pd.DataFrame) -> list[Alert]`.
2. Keep any rolling window state inside the rule instance.
3. Register the rule in `src/alert_project/cli.py` (or in a future rule registry module).
4. Add unit tests in `tests/`.

This keeps new rules isolated, testable, and deployable without touching ingestion or sink code.
