"""Runtime configuration and on-disk layout.

Layout under ``RAIN_DATA_DIR`` (default ``/data``)::

    rainfall.db                         SQLite metadata / revision ledger
    partitions/YYYY-MM-DD-rev<id>.parquet
                                        immutable per-revision partition files;
                                        the publication row points at exactly one
    tmp/inputs/YYYY-MM-DD-rN.csv        accepted raw uploads (audit trail, also
                                        the source recovery rebuilds from)
    tmp/work/<rev>.arrow                cleaned Arrow IPC intermediate
    tmp/parquet/<rev>.parquet           encoded file, staged before the rename

Legacy layout (pre immutable-files, still migrated at startup)::

    partitions/YYYY-MM-DD.parquet       single live path, swapped in place
    tmp/parquet/rollback-<date>-r<seq>.parquet
                                        pre-swap backup of the live file
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

    def partition_path(self, date: str, revision_id: int) -> Path:
        """Immutable published file of one revision.

        Every revision publishes under its own name; the publication row in
        the ledger is the only pointer that ever changes, so a crash between
        the file rename and the ledger commit can never corrupt what
        downloads serve.
        """
        return self.partition_dir / f"{date}-rev{revision_id}.parquet"

    def legacy_partition_path(self, date: str) -> Path:
        """Single live path used by the pre-immutable-layout version."""
        return self.partition_dir / f"{date}.parquet"

    def input_path(self, date: str, seq: int) -> Path:
        return self.input_dir / f"{date}-r{seq}.csv"

    def work_arrow_path(self, rev_id: int) -> Path:
        return self.work_dir / f"rev{rev_id}.arrow"

    def staged_parquet_path(self, rev_id: int) -> Path:
        return self.staged_dir / f"rev{rev_id}.parquet"


settings = Settings.from_env()
