"""Cleaning subprocess.

Runs in its own OS process, isolated from the API. It re-validates the raw
upload (the API pre-flight check is only a convenience), converts tenths of
millimetres into millimetres and writes an Arrow IPC stream that the encoder
consumes. On success it prints one JSON line::

    {"ok": true, "row_count": N}

Any failure prints ``{"ok": false, "error": "..."}`` and exits non-zero.
"""

from __future__ import annotations

import json
import sys

# Allow ``python -P clean.py`` to import the app package regardless of CWD.
# RAIN_APP_ROOT is injected by the pipeline; /app is the container default.
import os

sys.path.insert(0, os.environ.get("RAIN_APP_ROOT", "/app"))

import pyarrow as pa  # noqa: E402

from app.validation import ValidationError, parse_csv  # noqa: E402


def main() -> int:
    if len(sys.argv) != 5:
        print(
            json.dumps({"ok": False, "error": "usage: clean.py INPUT ARROW DATE REV_ID"}),
            flush=True,
        )
        return 2
    input_path, arrow_path, target_date, rev_id = sys.argv[1:5]
    try:
        delay = float(os.environ.get("RAIN_DEMO_WORK_DELAY", "0") or 0)
        if delay > 0:
            import time

            time.sleep(delay)
        with open(input_path, "rb") as fh:
            data = fh.read()
        rows = parse_csv(data, target_date)

        stations = pa.array([r.station for r in rows], type=pa.string())
        # UTC timestamps, microsecond precision is plenty for integral hours.
        timestamps = pa.array(
            [r.ts for r in rows], type=pa.timestamp("us", tz="UTC")
        )
        rain_mm = pa.array(
            [r.rain_tenth_mm / 10.0 for r in rows], type=pa.float64()
        )
        table = pa.table(
            {
                "station": stations,
                "utc_hour": timestamps,
                "rain_mm": rain_mm,
            }
        )
        with pa.OSFile(arrow_path, "wb") as sink:
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)

        print(json.dumps({"ok": True, "row_count": len(rows), "rev_id": rev_id}), flush=True)
        return 0
    except ValidationError as exc:
        print(json.dumps({"ok": False, "error": exc.message}), flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 - surfaced as structured failure
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
