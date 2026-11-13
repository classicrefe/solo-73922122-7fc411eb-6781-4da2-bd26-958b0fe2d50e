"""End-to-end behaviour tests for the rainfall partition service."""

from __future__ import annotations

import contextlib
import io
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from conftest import DAY, GOOD_ROWS, make_csv, upload, wait_status

ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------- environment
def _kill_allowed() -> bool:
    """Some CI sandboxes block os.kill entirely (then SIGKILL demos can't run)."""
    probe = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(2)"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.2)
    try:
        probe.kill()
        probe.wait(timeout=5)
        return True
    except (PermissionError, ProcessLookupError):
        return False


KILL_ALLOWED = _kill_allowed()
needs_kill = pytest.mark.skipif(
    not KILL_ALLOWED, reason="host blocks os.kill; covered by unit test and the Docker demo"
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_health(base: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{base}/health", timeout=2) as resp:
                if resp.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.2)
    raise AssertionError(f"service at {base} never became healthy")


def _http(method: str, url: str, data: bytes | None = None, headers: dict | None = None):
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def _multipart(boundary: str, filename: str, content: bytes) -> bytes:
    return (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()


@contextmanager
def server(tmp_path, demo_delay: str = "0.7"):
    """Real uvicorn process against an isolated data dir."""
    data_dir = tmp_path / "data"
    port = _free_port()
    env = {
        **os.environ,
        "RAIN_DATA_DIR": str(data_dir),
        "RAIN_DEMO_DELAY": demo_delay,
        "RAIN_ENABLE_SHUTDOWN": "1",
        "PYTHONPATH": str(ROOT),
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        _wait_health(base)
        yield base
    finally:
        # Prefer the in-process self-shutdown (works where os.kill is blocked);
        # fall back to signals where permitted.
        with contextlib.suppress(OSError):
            _http("POST", f"{base}/test/shutdown")
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(PermissionError, ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                proc.wait(timeout=5)


def _wait_terminal(base: str, date: str, timeout: float = 25.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = json.loads(_http("GET", f"{base}/status/{date}")[1])
        task = status["latest_task"]
        if task and task["status"] in ("published", "failed", "cancelled"):
            return status
        time.sleep(0.1)
    raise AssertionError("pipeline never settled")


# --------------------------------------------------------------- in-process
def test_happy_path_publishes_parquet_in_mm(client):
    c, _, _ = client
    resp = upload(c, DAY, make_csv(GOOD_ROWS))
    assert resp.status_code == 202, resp.text
    rev_id = resp.json()["revision_id"]

    final = wait_status(c, DAY)
    assert final["latest_task"]["status"] == "published"
    assert final["current_publication"]["revision_id"] == rev_id

    dl = c.get(f"/download/{DAY}")
    assert dl.status_code == 200
    table = pq.read_table(io.BytesIO(dl.content))
    assert table.schema.names == ["station", "utc_hour", "rain_mm"]
    assert str(table.schema.field("utc_hour").type) == "timestamp[us, tz=UTC]"
    data = table.to_pydict()
    assert data["rain_mm"] == [0.0, 1.2, 0.5, 21.0]
    assert data["station"] == ["ST001", "ST001", "ST002", "ST002"]
    rg = pq.ParquetFile(io.BytesIO(dl.content)).metadata.row_group(0)
    assert {rg.column(i).compression for i in range(rg.num_columns)} == {"ZSTD"}


def test_rejects_wrong_date_duplicate_negative_and_non_integer(client):
    c, _, _ = client
    r = upload(
        c,
        DAY,
        make_csv([("ST001", "2026-11-09T23:00:00Z", 1), ("ST001", f"{DAY}T01:00:00Z", 2)]),
    )
    assert r.status_code == 422 and "does not belong" in r.json()["detail"]

    r = upload(
        c,
        DAY,
        make_csv([("ST001", f"{DAY}T00:00:00Z", 1), ("ST001", f"{DAY}T00:00:00Z", 2)]),
    )
    assert r.status_code == 422 and "duplicate" in r.json()["detail"]

    r = upload(c, DAY, make_csv([("ST001", f"{DAY}T00:00:00Z", -3)]))
    assert r.status_code == 422 and "negative" in r.json()["detail"]

    r = upload(c, DAY, make_csv([("ST001", f"{DAY}T00:00:00Z", 1.5)]))
    assert r.status_code == 422 and "not an integer" in r.json()["detail"]

    r = upload(c, DAY, make_csv([("ST001", f"{DAY}T00:30:00Z", 1)]))
    assert r.status_code == 422 and "integral hour" in r.json()["detail"]

    assert c.get(f"/status/{DAY}").json()["latest_task"] is None


def test_size_limit_enforced(client):
    c, _, _ = client
    oversized = make_csv(GOOD_ROWS) + b"#" + b"x" * (8 * 1024 * 1024)
    assert upload(c, DAY, oversized).status_code == 413


def test_correction_publishes_new_revision_and_switches_partition(client):
    c, _, _ = client
    first = upload(c, DAY, make_csv(GOOD_ROWS)).json()
    wait_status(c, DAY)
    first_pub = c.get(f"/download/{DAY}").content

    corrected_rows = [
        ("ST001", f"{DAY}T00:00:00Z", 0),
        ("ST001", f"{DAY}T01:00:00Z", 99),  # corrected: 9.9 mm
        ("ST002", f"{DAY}T00:00:00Z", 5),
        ("ST002", f"{DAY}T01:00:00Z", 210),
    ]
    second = upload(c, DAY, make_csv(corrected_rows)).json()
    assert second["seq"] == first["seq"] + 1
    final = wait_status(c, DAY)
    assert final["latest_task"]["revision_id"] == second["revision_id"]
    assert final["latest_task"]["status"] == "published"

    table = pq.read_table(io.BytesIO(c.get(f"/download/{DAY}").content)).to_pydict()
    assert table["rain_mm"][1] == 9.9
    assert c.get(f"/download/{DAY}").content != first_pub
    assert final["current_publication"]["revision_id"] == second["revision_id"]


def test_different_dates_run_in_parallel_and_independently(client):
    c, _, _ = client
    other = "2026-11-11"
    rows_b = [
        ("ST003", f"{other}T00:00:00Z", 17),
        ("ST003", f"{other}T01:00:00Z", 3),
    ]
    assert upload(c, DAY, make_csv(GOOD_ROWS)).status_code == 202
    assert upload(c, other, make_csv(rows_b)).status_code == 202
    wait_status(c, DAY)
    wait_status(c, other)
    assert c.get(f"/download/{DAY}").status_code == 200
    assert c.get(f"/download/{other}").status_code == 200


# --------------------------------------------------------------- real server
def test_supersede_while_running_cancels_in_flight_revision(tmp_path):
    with server(tmp_path, demo_delay="0.7") as base:
        code, out = _http(
            "POST", f"{base}/uploads/{DAY}",
            _multipart("b", "up.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        assert code == 202, out
        first_id = json.loads(out)["revision_id"]

        code, out = _http(
            "POST", f"{base}/uploads/{DAY}",
            _multipart("b", "up2.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        assert code == 202, out
        payload = json.loads(out)
        assert first_id in payload["invalidated_revision_ids"]

        status = _wait_terminal(base, DAY)
        history = {r["revision_id"]: r for r in json.loads(_http("GET", f"{base}/revisions/{DAY}")[1])}
        assert history[first_id]["status"] == "cancelled"
        assert "superseded" in (history[first_id]["error"] or "")
        assert status["current_publication"]["revision_id"] == payload["revision_id"]


def test_pause_blocks_stages_and_resume_completes(tmp_path):
    with server(tmp_path, demo_delay="0.4") as base:
        assert json.loads(_http("POST", f"{base}/admin/pause")[1])["paused"] is True
        code, out = _http(
            "POST", f"{base}/uploads/{DAY}",
            _multipart("b", "up.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        assert code == 202, out
        rev = json.loads(out)["revision_id"]

        time.sleep(1.2)
        task = json.loads(_http("GET", f"{base}/status/{DAY}")[1])["latest_task"]
        assert task["status"] == "queued"
        assert _http("GET", f"{base}/download/{DAY}")[0] == 404

        assert json.loads(_http("POST", f"{base}/admin/resume")[1])["paused"] is False
        status = _wait_terminal(base, DAY)
        assert status["latest_task"]["status"] == "published"
        events = [e["event"] for e in json.loads(_http("GET", f"{base}/revisions/{rev}/events")[1])]
        assert "paused" in events


@needs_kill
def test_force_kill_marks_failed_and_keeps_old_partition(tmp_path):
    with server(tmp_path, demo_delay="1.0") as base:
        code, out = _http(
            "POST", f"{base}/uploads/{DAY}",
            _multipart("b", "up.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        assert code == 202
        _wait_terminal(base, DAY)
        good = _http("GET", f"{base}/download/{DAY}")[1]

        code, out = _http(
            "POST", f"{base}/uploads/{DAY}",
            _multipart("b", "up2.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        rev2 = json.loads(out)["revision_id"]

        killed = False
        deadline = time.time() + 15
        while time.time() < deadline:
            task = json.loads(_http("GET", f"{base}/status/{DAY}")[1])["latest_task"]
            if task["running"]:
                code, out = _http("POST", f"{base}/admin/revisions/{rev2}/kill")
                assert code == 200, out
                killed = True
                break
            time.sleep(0.05)
        assert killed

        status = _wait_terminal(base, DAY)
        assert status["latest_task"]["status"] == "failed"
        assert _http("GET", f"{base}/download/{DAY}")[1] == good
        assert _http("POST", f"{base}/admin/revisions/{rev2}/kill")[0] == 409


def test_kill_without_running_subprocess_is_409(client):
    c, _, _ = client
    rev = upload(c, DAY, make_csv(GOOD_ROWS)).json()["revision_id"]
    wait_status(c, DAY)
    assert c.post(f"/admin/revisions/{rev}/kill").status_code == 409


def test_status_serializes_ready_stage(client):
    """Regression: stage='ready' must be a valid API enum (no 500 in the swap window)."""
    c, db, _ = client
    rev = upload(c, DAY, make_csv(GOOD_ROWS)).json()["revision_id"]
    wait_status(c, DAY)
    db.update_revision(rev, status="ready", stage="ready")
    payload = c.get(f"/status/{DAY}").json()
    assert payload["latest_task"]["stage"] == "ready"
    assert payload["latest_task"]["status"] == "ready"


def test_force_kill_unit_with_fake_process(client, monkeypatch):
    """The kill path itself, independent of host signal permissions."""
    c, _, _ = client
    import app.main as main
    import app.pipeline as pipeline_mod

    class FakeProc:
        def __init__(self):
            self.returncode = None
            self.killed = False

        def kill(self):
            self.killed = True
            self.returncode = -9

    fake = FakeProc()
    rev = upload(c, DAY, make_csv(GOOD_ROWS)).json()["revision_id"]
    wait_status(c, DAY)
    monkeypatch.setitem(main.manager._processes, rev, fake)

    # A live-looking fake process is killed and the event trail records it.
    import asyncio

    assert asyncio.run(main.manager.force_kill(rev)) is True
    assert fake.killed is True
    events = [e["event"] for e in c.get(f"/revisions/{rev}/events").json()]
    assert "force_killed" in events

    # Second call reports nothing to kill (process already exited).
    assert asyncio.run(main.manager.force_kill(rev)) is False


def test_recovery_after_restart_requeues_and_finishes(tmp_path):
    """Stop the real server mid-pipeline; restart must resume, never publish a remnant."""
    data_dir = tmp_path / "data"
    port = _free_port()
    env = {
        **os.environ,
        "RAIN_DATA_DIR": str(data_dir),
        "RAIN_DEMO_DELAY": "0",
        "RAIN_ENABLE_SHUTDOWN": "1",
        "PYTHONPATH": str(ROOT),
    }
    base = f"http://127.0.0.1:{port}"

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_health(base)
        assert json.loads(_http("POST", f"{base}/admin/pause")[1])["paused"] is True
        code, out = _http(
            "POST", f"{base}/uploads/{DAY}",
            _multipart("b", "up.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        assert code == 202
        rev = json.loads(out)["revision_id"]
    finally:
        with contextlib.suppress(OSError):
            _http("POST", f"{base}/test/shutdown")
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(PermissionError, ProcessLookupError):
                proc.kill()

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_health(base)
        status = json.loads(_http("GET", f"{base}/status/{DAY}")[1])
        assert status["latest_task"]["revision_id"] == rev
        assert status["latest_task"]["status"] != "published"
        assert _http("GET", f"{base}/download/{DAY}")[0] == 404

        assert json.loads(_http("POST", f"{base}/admin/resume")[1])["paused"] is False
        status = _wait_terminal(base, DAY)
        assert status["latest_task"]["status"] == "published"
        assert _http("GET", f"{base}/download/{DAY}")[0] == 200
    finally:
        with contextlib.suppress(OSError):
            _http("POST", f"{base}/test/shutdown")
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(PermissionError, ProcessLookupError):
                proc.kill()


def test_recovery_discards_stale_superseded_revisions(tmp_path):
    """Two unfinished revisions at crash: only the newest is resumed."""
    data_dir = tmp_path / "data"
    port = _free_port()
    env = {
        **os.environ,
        "RAIN_DATA_DIR": str(data_dir),
        "RAIN_DEMO_DELAY": "0",
        "RAIN_ENABLE_SHUTDOWN": "1",
        "PYTHONPATH": str(ROOT),
    }
    base = f"http://127.0.0.1:{port}"
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_health(base)
        _http("POST", f"{base}/admin/pause")
        first = json.loads(_http(
            "POST", f"{base}/uploads/{DAY}",
            _multipart("b", "a.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )[1])["revision_id"]
        second = json.loads(_http(
            "POST", f"{base}/uploads/{DAY}",
            _multipart("b", "b.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )[1])["revision_id"]
    finally:
        with contextlib.suppress(OSError):
            _http("POST", f"{base}/test/shutdown")
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(PermissionError, ProcessLookupError):
                proc.kill()

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(port)],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_health(base)
        _http("POST", f"{base}/admin/resume")
        _wait_terminal(base, DAY)
        history = {r["revision_id"]: r for r in json.loads(_http("GET", f"{base}/revisions/{DAY}")[1])}
        assert history[first]["status"] == "cancelled"
        assert history[second]["status"] == "published"
    finally:
        with contextlib.suppress(OSError):
            _http("POST", f"{base}/test/shutdown")
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(PermissionError, ProcessLookupError):
                proc.kill()
