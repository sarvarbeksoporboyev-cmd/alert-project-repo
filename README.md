# Alert Project

Streaming log analyzer for mobile-app CSV logs with pluggable alert rules.

## Implemented alert rules
1. More than 10 fatal errors in less than 1 minute (global).
2. More than 10 fatal errors in less than 1 hour for the same `bundle_id`.

## Architecture and technology decisions
- **Pandas + chunked CSV reading**: `pandas.read_csv(..., chunksize=...)` keeps memory bounded and is suitable for large daily volumes.
- **Pluggable rule engine**: each rule is a stateful class implementing one interface (`AlertRule`), so new rules are added without changing ingestion or sinks.
- **Separated sink layer**: alerts can be written to JSONL, sent to webhooks, or fanned out to multiple destinations with `CompositeAlertSink`.
- **Rolling windows via deque**: O(n) event-window tracking for efficient threshold checks.

## Project layout
- `src/alert_project/ingestion.py`: CSV parsing and normalization.
- `src/alert_project/engine.py`: orchestration loop.
- `src/alert_project/rules/`: pluggable alert rules.
- `src/alert_project/alerts.py`: output sinks (JSONL, webhook, composite, null).
- `src/alert_project/cli.py`: processing entrypoint.
- `src/alert_project/benchmark.py`: reproducible throughput benchmark.
- `tests/`: unit tests.

## Local run
```bash
python -m venv .venv
. .venv/Scripts/Activate.ps1
pip install -r requirements.txt
pip install -e .[dev]
python -m alert_project.cli --input data.csv --output output/alerts.jsonl --chunksize 200000
```

## Webhook alert delivery
- Set environment variable:
```bash
$env:ALERT_WEBHOOK_URL="https://example.com/my-webhook"
python -m alert_project.cli --input data.csv --output output/alerts.jsonl
```
- Or pass directly:
```bash
python -m alert_project.cli --input data.csv --output output/alerts.jsonl --webhook-url https://example.com/my-webhook
```

## Docker run (single command)
```bash
docker compose up --build
```

`docker-compose.yml` supports optional webhook forwarding via `ALERT_WEBHOOK_URL`.

## Benchmark for 100M/day capacity
Run the reproducible benchmark command:
```bash
python -m alert_project.benchmark --input data.csv --chunksize 200000 --repeat 3
```

Example output includes:
- `avg_rows_per_second`
- `avg_projected_rows_per_day`
- `meets_100m_per_day`

## Data note for this dataset
The provided dataset uses `severity` values `error/success` (not `fatal`).
If you want rules to trigger on this dataset as-is, run:
```bash
python -m alert_project.cli --input data.csv --output output/alerts.jsonl --fatal-severity error
```

## How to add a new alert rule
1. Create a class in `src/alert_project/rules/` that inherits `AlertRule` and implements `process(events: pd.DataFrame) -> list[Alert]`.
2. Keep rolling state inside the rule instance.
3. Register the rule in `src/alert_project/cli.py` and in `src/alert_project/benchmark.py`.
4. Add tests in `tests/`.