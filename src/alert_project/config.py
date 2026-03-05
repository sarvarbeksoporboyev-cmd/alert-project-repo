"""Static configuration for CSV schema and defaults."""

from __future__ import annotations

CSV_COLUMNS: list[str] = [
    "error_code",
    "error_message",
    "severity",
    "log_location",
    "mode",
    "model",
    "graphics",
    "session_id",
    "sdkv",
    "test_mode",
    "flow_id",
    "flow_type",
    "sdk_date",
    "publisher_id",
    "game_id",
    "bundle_id",
    "appv",
    "language",
    "os",
    "adv_id",
    "gdpr",
    "ccpa",
    "country_code",
    "date",
]

DEFAULT_CHUNK_SIZE = 200_000
DEFAULT_FATAL_SEVERITY = "fatal"
