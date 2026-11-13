"""CSV parsing and validation, shared by the API pre-flight and the cleaner.

Input contract
--------------
UTF-8 text, header row (case-insensitive)::

    station,utc_hour,rain_tenth_mm

* ``station``     : non-empty station id (trimmed), max 64 chars
* ``utc_hour``    : UTC integral hour, ISO-8601; ``YYYY-MM-DDTHH:MM:SS[Z|+00:00]``
  or space separated; any explicit offset must be UTC; seconds/minutes must be 0
* ``rain_tenth_mm``: non-negative integer (tenths of a millimetre)

Rules: every row must belong to the requested UTC date; the same
``(station, utc_hour)`` pair must not appear twice.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime, timezone

REQUIRED_HEADER = ("station", "utc_hour", "rain_tenth_mm")


class ValidationError(ValueError):
    """Structured validation failure; ``message`` is safe to return to clients."""

    def __init__(self, message: str, line: int | None = None):
        if line is not None:
            message = f"line {line}: {message}"
        super().__init__(message)
        self.message = message
        self.line = line


@dataclass(frozen=True)
class RainRow:
    station: str
    ts: datetime
    rain_tenth_mm: int


def _parse_ts(raw: str, line: int) -> datetime:
    raw = raw.strip()
    if not raw:
        raise ValidationError("empty utc_hour", line)
    text = raw.replace("Z", "+00:00") if raw.endswith(("Z", "z")) else raw
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(f"invalid utc_hour '{raw}'", line) from exc
    if dt.tzinfo is None:
        # Naive timestamps are interpreted as UTC (documented contract).
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        offset = dt.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise ValidationError(f"utc_hour '{raw}' is not a UTC timestamp", line)
        dt = dt.astimezone(timezone.utc)
    if dt.minute != 0 or dt.second != 0 or dt.microsecond != 0:
        raise ValidationError(f"utc_hour '{raw}' is not an integral hour", line)
    return dt


def _parse_rain(raw: str, line: int) -> int:
    raw = raw.strip()
    if not raw:
        raise ValidationError("empty rain_tenth_mm", line)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValidationError(f"rain_tenth_mm '{raw}' is not an integer", line) from exc
    if value < 0:
        raise ValidationError(f"rain_tenth_mm {value} is negative", line)
    return value


def parse_csv(data: bytes, target_date: str) -> list[RainRow]:
    """Validate raw CSV bytes; return ordered, de-duplicated rows."""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError("file is not valid UTF-8 text") from exc
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise ValidationError("empty file: header row required") from exc
    normalized = [h.strip().lower() for h in header]
    if normalized != list(REQUIRED_HEADER):
        raise ValidationError(
            f"header must be {','.join(REQUIRED_HEADER)}, got {','.join(normalized) or '<empty>'}"
        )

    expected: date
    try:
        expected = date.fromisoformat(target_date)
    except ValueError as exc:
        raise ValidationError(f"invalid target date '{target_date}'") from exc

    rows: list[RainRow] = []
    seen: set[tuple[str, datetime]] = set()
    any_row = False
    for lineno, record in enumerate(reader, start=2):
        if not record or all(not c.strip() for c in record):
            continue  # tolerate blank lines
        any_row = True
        if len(record) != 3:
            raise ValidationError(f"expected 3 columns, got {len(record)}", lineno)
        station = record[0].strip()
        if not station:
            raise ValidationError("empty station", lineno)
        if len(station) > 64:
            raise ValidationError("station id longer than 64 characters", lineno)
        ts = _parse_ts(record[1], lineno)
        if ts.date() != expected:
            raise ValidationError(
                f"utc_hour '{record[1].strip()}' does not belong to UTC date {target_date}",
                lineno,
            )
        rain = _parse_rain(record[2], lineno)
        key = (station, ts)
        if key in seen:
            raise ValidationError(
                f"duplicate measurement for station '{station}' at {ts.isoformat()}",
                lineno,
            )
        seen.add(key)
        rows.append(RainRow(station=station, ts=ts, rain_tenth_mm=rain))

    if not any_row:
        raise ValidationError("no data rows found")
    return rows
