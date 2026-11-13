"""Pytest fixtures: isolated data dir + in-process FastAPI client."""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def client(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("RAIN_DATA_DIR", str(data_dir))
    monkeypatch.setenv("RAIN_DEMO_DELAY", "0")

    # Reload configuration-bound modules so they pick up the temp data dir and
    # a fresh PipelineManager (fresh asyncio queues/process registry). Order
    # matters: dependents must be removed before their dependencies.
    for name in ("app.main", "app.pipeline", "app.db", "app.config"):
        sys.modules.pop(name, None)
    import app.db as db  # noqa: F401  (import builds schema against temp dir)
    import app.main as main

    from fastapi.testclient import TestClient

    with TestClient(main.app) as c:
        yield c, db, data_dir


def make_csv(rows: list[tuple[str, str, int]], header: str = "station,utc_hour,rain_tenth_mm") -> bytes:
    lines = [header]
    lines.extend(f"{s},{ts},{r}" for s, ts, r in rows)
    return ("\n".join(lines) + "\n").encode()


def upload(c, date: str, content: bytes, filename: str = "up.csv"):
    return c.post(
        f"/uploads/{date}",
        files={"file": (filename, io.BytesIO(content), "text/csv")},
    )


DAY = "2026-11-10"
GOOD_ROWS = [
    ("ST001", f"{DAY}T00:00:00Z", 0),
    ("ST001", f"{DAY}T01:00:00Z", 12),
    ("ST002", f"{DAY}T00:00:00Z", 5),
    ("ST002", f"{DAY}T01:00:00Z", 210),
]


def wait_status(c, date: str, timeout: float = 20.0) -> dict:
    import time

    deadline = time.time() + timeout
    last = {}
    while time.time() < deadline:
        last = c.get(f"/status/{date}").json()
        task = last.get("latest_task")
        if task and task["status"] in ("published", "failed", "cancelled"):
            return last
        time.sleep(0.05)
    raise AssertionError(f"revision did not terminate: {last}")
