"""Parquet encoding subprocess.

Reads the Arrow IPC stream emitted by :mod:`app.subprocs.clean` and writes a
zstd-compressed Parquet file. The output is only ever written to a staged
path; the pipeline swaps it into place atomically after this process exits 0.
"""

from __future__ import annotations

import json
import sys

import os

sys.path.insert(0, os.environ.get("RAIN_APP_ROOT", "/app"))

import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

# Explicit, documented partition schema. Rainfall is stored in millimetres.
PARTITION_SCHEMA = pa.schema(
    [
        pa.field("station", pa.string(), nullable=False),
        pa.field("utc_hour", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("rain_mm", pa.float64(), nullable=False),
    ]
)


def main() -> int:
    if len(sys.argv) != 4:
        print(
            json.dumps({"ok": False, "error": "usage: encode.py ARROW PARQUET REV_ID"}),
            flush=True,
        )
        return 2
    arrow_path, parquet_path, rev_id = sys.argv[1:4]
    try:
        delay = float(os.environ.get("RAIN_DEMO_WORK_DELAY", "0") or 0)
        if delay > 0:
            import time

            time.sleep(delay)
        with pa.memory_map(arrow_path, "r") as source:
            table = pa.ipc.open_stream(source).read_all()
        if table.schema != PARTITION_SCHEMA:
            table = table.cast(PARTITION_SCHEMA)
        pq.write_table(
            table,
            parquet_path,
            compression="zstd",
            compression_level=3,
            version="2.6",
        )
        # Re-open to make sure the file is a complete, readable Parquet.
        validated = pq.ParquetFile(parquet_path)
        print(
            json.dumps(
                {
                    "ok": True,
                    "row_count": validated.metadata.num_rows,
                    "rev_id": rev_id,
                }
            ),
            flush=True,
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}), flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
