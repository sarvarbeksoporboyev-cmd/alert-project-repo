"""CSV ingestion and normalization utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from .config import CSV_COLUMNS


def detect_csv_layout(path: Path) -> tuple[int | None, list[str] | None]:
    """Detect whether file has numeric headers, expected headers, or no headers."""

    first_line = ""
    with path.open("r", encoding="utf-8", errors="replace", newline="") as csv_file:
        first_line = csv_file.readline().strip()

    numeric_header = ",".join(str(index) for index in range(len(CSV_COLUMNS)))
    if first_line == numeric_header:
        return 0, CSV_COLUMNS

    expected_header = ",".join(CSV_COLUMNS)
    if first_line.lower() == expected_header.lower():
        return 0, None

    return None, CSV_COLUMNS


def read_log_chunks(path: Path, chunksize: int) -> Iterable[pd.DataFrame]:
    """Yield chunks of raw CSV records."""

    header, names = detect_csv_layout(path)
    return pd.read_csv(
        path,
        chunksize=chunksize,
        header=header,
        names=names,
        dtype=str,
        on_bad_lines="skip",
        quotechar='"',
        keep_default_na=False,
    )


def _parse_dates(date_series: pd.Series) -> pd.Series:
    """Parse mixed date formats: epoch seconds or datetime strings."""

    raw_dates = date_series.astype(str).str.strip()

    numeric_dates = pd.to_numeric(raw_dates, errors="coerce")
    parsed_numeric = pd.to_datetime(numeric_dates, unit="s", errors="coerce", utc=True)

    parsed_text = pd.to_datetime(raw_dates.where(numeric_dates.isna()), errors="coerce", utc=True)

    return parsed_numeric.fillna(parsed_text)


def normalize_chunk(chunk: pd.DataFrame, fatal_severity: str = "fatal") -> pd.DataFrame:
    """Normalize fields required for rule evaluation."""

    normalized = chunk.copy()

    if list(normalized.columns) != CSV_COLUMNS:
        normalized.columns = CSV_COLUMNS[: len(normalized.columns)]

    normalized["severity"] = normalized["severity"].astype(str).str.strip().str.lower()
    normalized["bundle_id"] = normalized["bundle_id"].astype(str).str.strip()
    normalized["bundle_id"] = normalized["bundle_id"].replace({"": "unknown"})
    normalized["date"] = _parse_dates(normalized["date"])

    normalized = normalized.dropna(subset=["date"])
    normalized = normalized[["severity", "bundle_id", "date"]]

    normalized["is_fatal"] = normalized["severity"] == fatal_severity.lower()

    return normalized