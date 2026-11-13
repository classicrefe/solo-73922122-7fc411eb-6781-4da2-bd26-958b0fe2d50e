"""Runtime configuration and on-disk layout.

Layout under ``RAIN_DATA_DIR`` (default ``/data``)::

    rainfall.db                  SQLite metadata / revision ledger
    partitions/YYYY-MM-DD.parquet   published, immutable-after-replace files
    tmp/inputs/YYYY-MM-DD-rN.csv    accepted raw uploads (audit trail)
    tmp/work/<rev>.arrow            cleaned Arrow IPC intermediate
    tmp/parquet/<rev>.parquet       encoded file, staged before atomic swap
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

MAX_UPLOAD_BYTES = 8 * 1024 * 1024  # 8 MiB hard limit


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    partition_dir: Path
    input_dir: Path
    work_dir: Path
    staged_dir: Path
    demo_delay: float

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = Path(os.environ.get("RAIN_DATA_DIR", "/data")).resolve()
        demo_delay = float(os.environ.get("RAIN_DEMO_DELAY", "0") or 0)
        s = cls(
            data_dir=data_dir,
            db_path=data_dir / "rainfall.db",
            partition_dir=data_dir / "partitions",
            input_dir=data_dir / "tmp" / "inputs",
            work_dir=data_dir / "tmp" / "work",
            staged_dir=data_dir / "tmp" / "parquet",
            demo_delay=demo_delay,
        )
        for p in (s.data_dir, s.partition_dir, s.input_dir, s.work_dir, s.staged_dir):
            p.mkdir(parents=True, exist_ok=True)
        return s

    def partition_path(self, date: str) -> Path:
        return self.partition_dir / f"{date}.parquet"

    def input_path(self, date: str, seq: int) -> Path:
        return self.input_dir / f"{date}-r{seq}.csv"

    def work_arrow_path(self, rev_id: int) -> Path:
        return self.work_dir / f"rev{rev_id}.arrow"

    def staged_parquet_path(self, rev_id: int) -> Path:
        return self.staged_dir / f"rev{rev_id}.parquet"


settings = Settings.from_env()
