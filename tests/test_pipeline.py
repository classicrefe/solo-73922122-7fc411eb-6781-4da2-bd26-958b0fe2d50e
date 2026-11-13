"""End-to-end behaviour tests for the rainfall partition service."""

from __future__ import annotations

import contextlib
import io
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from conftest import DAY, GOOD_ROWS, make_csv, upload, wait_status

ROOT = Path(__file__).resolve().parents[1]

CORRECTED_ROWS = [
    ("ST001", f"{DAY}T00:00:00Z", 0),
    ("ST001", f"{DAY}T01:00:00Z", 99),  # corrected: 9.9 mm
    ("ST002", f"{DAY}T00:00:00Z", 5),
    ("ST002", f"{DAY}T01:00:00Z", 210),
]
GOOD_MM = [0.0, 1.2, 0.5, 21.0]
CORRECTED_MM = [0.0, 9.9, 0.5, 21.0]


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


class Server:
    """Restartable real-server harness for crash/recovery tests."""

    def __init__(self, tmp_path: Path, demo_delay: str = "0"):
        self.data_dir = tmp_path / "data"
        self.port = _free_port()
        self.demo_delay = demo_delay
        self.proc: subprocess.Popen | None = None
        self.base = f"http://127.0.0.1:{self.port}"
        self._log = tmp_path / "server.log"

    def start(self) -> None:
        env = {
            **os.environ,
            "RAIN_DATA_DIR": str(self.data_dir),
            "RAIN_DEMO_DELAY": self.demo_delay,
            "RAIN_ENABLE_SHUTDOWN": "1",
            "PYTHONPATH": str(ROOT),
        }
        self._log_handle = open(self._log, "ab")  # noqa: SIM115 - closed in stop()
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(self.port)],
            cwd=str(ROOT),
            env=env,
            stdout=self._log_handle,
            stderr=subprocess.STDOUT,
        )
        try:
            _wait_health(self.base)
        except Exception:
            self.stop()
            raise

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            with contextlib.suppress(OSError):
                _http("POST", f"{self.base}/test/shutdown")
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(PermissionError, ProcessLookupError):
                    self.proc.kill()
                with contextlib.suppress(Exception):
                    self.proc.wait(timeout=5)
        self.proc = None
        with contextlib.suppress(Exception):
            self._log_handle.close()

    def expect_exit(self, timeout: float = 30.0) -> int:
        """Wait for the armed hard crash to kill the server process."""
        assert self.proc is not None
        return self.proc.wait(timeout=timeout)


@contextmanager
def server(tmp_path, demo_delay: str = "0.7"):
    """Real uvicorn process against an isolated data dir."""
    srv = Server(tmp_path, demo_delay=demo_delay)
    srv.start()
    try:
        yield srv.base
    finally:
        srv.stop()


def _rain_mm(parquet_bytes: bytes) -> list[float]:
    return pq.read_table(io.BytesIO(parquet_bytes)).to_pydict()["rain_mm"]


def _partition_names(data_dir: Path) -> set[str]:
    return {p.name for p in (data_dir / "partitions").iterdir() if p.is_file()}


def _set_paused(data_dir: Path, paused: bool) -> None:
    """Flip the pause switch directly in the ledger while the server is down."""
    conn = sqlite3.connect(data_dir / "rainfall.db")
    try:
        conn.execute(
            "INSERT INTO pipeline_state(key, value) VALUES('paused', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("1" if paused else "0",),
        )
        conn.commit()
    finally:
        conn.close()


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


# ------------------------------------------------------- crash-point demos
def test_crash_before_publish_commit_restart_still_serves_old_partition(tmp_path):
    """Exit after the file rename but before the publication commit.

    After the restart — and until the recomputation of the interrupted
    revision finishes — downloads must keep serving the previous partition,
    exactly matching the still-current publication row.
    """
    srv = Server(tmp_path, demo_delay="0.2")
    srv.start()
    try:
        code, out = _http(
            "POST", f"{srv.base}/uploads/{DAY}",
            _multipart("b", "v1.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        assert code == 202, out
        rev1 = json.loads(out)["revision_id"]
        _wait_terminal(srv.base, DAY)
        old_bytes = _http("GET", f"{srv.base}/download/{DAY}")[1]
        assert _rain_mm(old_bytes) == GOOD_MM

        armed = json.loads(
            _http("POST", f"{srv.base}/admin/crash/arm?point=before_publish_commit")[1]
        )
        assert armed["armed"] is True
        code, out = _http(
            "POST", f"{srv.base}/uploads/{DAY}",
            _multipart("b", "v2.csv", make_csv(CORRECTED_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        assert code == 202, out
        rev2 = json.loads(out)["revision_id"]
        assert srv.expect_exit() == 1  # hard exit mid publish switch
    finally:
        srv.stop()

    # Freeze the pipeline while the server is down so the recovery window is
    # deterministic: recomputation is re-queued but blocked at the paused gate.
    _set_paused(srv.data_dir, True)
    srv.start()
    try:
        status = json.loads(_http("GET", f"{srv.base}/status/{DAY}")[1])
        assert status["paused"] is True
        # The publication row still points at rev1 and downloads serve exactly
        # that — the never-committed rev2 bytes must not leak.
        dl = _http("GET", f"{srv.base}/download/{DAY}")
        assert dl[0] == 200
        assert dl[1] == old_bytes
        assert status["current_publication"]["revision_id"] == rev1
        assert status["latest_task"]["revision_id"] == rev2
        assert status["latest_task"]["status"] not in ("published", "failed")
        # The orphaned, never-committed file was garbage-collected at startup.
        assert _partition_names(srv.data_dir) == {f"{DAY}-rev{rev1}.parquet"}

        assert json.loads(_http("POST", f"{srv.base}/admin/resume")[1])["paused"] is False
        final = _wait_terminal(srv.base, DAY)
        assert final["latest_task"]["status"] == "published"
        assert final["current_publication"]["revision_id"] == rev2
        new_bytes = _http("GET", f"{srv.base}/download/{DAY}")[1]
        assert new_bytes != old_bytes
        assert _rain_mm(new_bytes) == CORRECTED_MM
        # The superseded rev1 file was retired after the commit.
        assert _partition_names(srv.data_dir) == {f"{DAY}-rev{rev2}.parquet"}
    finally:
        srv.stop()


def test_crash_after_publish_commit_restart_keeps_new_partition(tmp_path):
    """Exit right after the publication commit: the switch must stick.

    The restarted service keeps serving the new partition; there is no
    rollback to the previous one and nothing is re-queued.
    """
    srv = Server(tmp_path, demo_delay="0.2")
    srv.start()
    try:
        code, out = _http(
            "POST", f"{srv.base}/uploads/{DAY}",
            _multipart("b", "v1.csv", make_csv(GOOD_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        assert code == 202, out
        rev1 = json.loads(out)["revision_id"]
        _wait_terminal(srv.base, DAY)
        old_bytes = _http("GET", f"{srv.base}/download/{DAY}")[1]

        armed = json.loads(
            _http("POST", f"{srv.base}/admin/crash/arm?point=after_publish_commit")[1]
        )
        assert armed["armed"] is True
        code, out = _http(
            "POST", f"{srv.base}/uploads/{DAY}",
            _multipart("b", "v2.csv", make_csv(CORRECTED_ROWS)),
            {"Content-Type": "multipart/form-data; boundary=b"},
        )
        assert code == 202, out
        rev2 = json.loads(out)["revision_id"]
        assert srv.expect_exit() == 1
    finally:
        srv.stop()

    srv.start()
    try:
        dl = _http("GET", f"{srv.base}/download/{DAY}")
        assert dl[0] == 200
        assert dl[1] != old_bytes
        assert _rain_mm(dl[1]) == CORRECTED_MM
        status = json.loads(_http("GET", f"{srv.base}/status/{DAY}")[1])
        assert status["current_publication"]["revision_id"] == rev2
        assert status["latest_task"]["revision_id"] == rev2
        assert status["latest_task"]["status"] == "published"
        assert status["worker_active"] is False
        # The crash happened before the old file was retired; startup GC
        # removed the orphan and must not have touched the live file.
        assert _partition_names(srv.data_dir) == {f"{DAY}-rev{rev2}.parquet"}
        # Stable: nothing gets re-queued or reverted behind our back.
        time.sleep(1.0)
        status = json.loads(_http("GET", f"{srv.base}/status/{DAY}")[1])
        assert status["latest_task"]["status"] == "published"
        assert _rain_mm(_http("GET", f"{srv.base}/download/{DAY}")[1]) == CORRECTED_MM
        assert rev1 != rev2
    finally:
        srv.stop()


# ------------------------------------------- legacy data-dir/SQLite migration
LEGACY_SCHEMA = """
CREATE TABLE IF NOT EXISTS revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    status          TEXT NOT NULL,
    stage           TEXT NOT NULL DEFAULT 'queued',
    error           TEXT,
    source_filename TEXT NOT NULL,
    input_path      TEXT NOT NULL,
    row_count       INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(date, seq)
);
CREATE TABLE IF NOT EXISTS publications (
    date              TEXT PRIMARY KEY,
    revision_id       INTEGER NOT NULL REFERENCES revisions(id),
    partition_path    TEXT NOT NULL,
    row_count         INTEGER NOT NULL,
    published_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE IF NOT EXISTS revision_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    revision_id INTEGER NOT NULL REFERENCES revisions(id),
    event       TEXT NOT NULL,
    detail      TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE IF NOT EXISTS pipeline_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _write_partition(path: Path, rows: list[tuple[str, str, int]]) -> None:
    """Write a partition file the way the pipeline would (tenth-mm -> mm)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "station": pa.array([r[0] for r in rows], type=pa.string()),
            "utc_hour": pa.array(
                [datetime.fromisoformat(r[1].replace("Z", "+00:00")) for r in rows],
                type=pa.timestamp("us", tz="UTC"),
            ),
            "rain_mm": pa.array([r[2] / 10.0 for r in rows], type=pa.float64()),
        }
    )
    pq.write_table(table, path, compression="zstd")


def _legacy_data_dir(
    data_dir: Path,
    *,
    revisions: list[tuple],
    publication: tuple,
    live_rows: list | None,
    rollback_rows: list | None,
    rollback_seq: int = 2,
    paused: bool = True,
) -> None:
    """Build a data dir + SQLite ledger as the old in-place-swap code left it.

    ``live_rows``     : content of ``partitions/<date>.parquet`` (None = missing)
    ``rollback_rows`` : content of ``tmp/parquet/rollback-<date>-r<seq>.parquet``
    """
    for sub in ("partitions", "tmp/inputs", "tmp/work", "tmp/parquet"):
        (data_dir / sub).mkdir(parents=True, exist_ok=True)
    legacy_path = data_dir / "partitions" / f"{DAY}.parquet"
    if live_rows is not None:
        _write_partition(legacy_path, live_rows)
    if rollback_rows is not None:
        _write_partition(
            data_dir / "tmp" / "parquet" / f"rollback-{DAY}-r{rollback_seq}.parquet",
            rollback_rows,
        )
    for (_id, _date, seq, *_rest) in revisions:
        (data_dir / "tmp" / "inputs" / f"{_date}-r{seq}.csv").write_bytes(
            make_csv(GOOD_ROWS if seq == 1 else CORRECTED_ROWS)
        )

    conn = sqlite3.connect(data_dir / "rainfall.db")
    try:
        conn.executescript(LEGACY_SCHEMA)
        for (rev_id, date, seq, status, stage) in revisions:
            conn.execute(
                "INSERT INTO revisions(id, date, seq, status, stage, "
                "source_filename, input_path, row_count) VALUES (?,?,?,?,?,?,?,?)",
                (
                    rev_id,
                    date,
                    seq,
                    status,
                    stage,
                    f"r{seq}.csv",
                    str(data_dir / "tmp" / "inputs" / f"{date}-r{seq}.csv"),
                    4,
                ),
            )
        conn.execute(
            "INSERT INTO publications(date, revision_id, partition_path, row_count) "
            "VALUES (?,?,?,?)",
            (publication[0], publication[1], str(legacy_path), publication[2]),
        )
        if paused:
            conn.execute("INSERT INTO pipeline_state(key, value) VALUES('paused','1')")
        conn.commit()
    finally:
        conn.close()


def test_legacy_window_renamed_not_committed_is_restored(tmp_path):
    """Old code crashed after the rename, before the commit.

    The ledger's publication row still points at rev1 while the live file
    already holds the uncommitted rev2 bytes (旧发布修订对应新文件). Startup
    must restore the published bytes and keep serving them until the
    re-queued rev2 legitimately publishes.
    """
    srv = Server(tmp_path, demo_delay="0.1")
    _legacy_data_dir(
        srv.data_dir,
        revisions=[
            (1, DAY, 1, "published", "published"),
            (2, DAY, 2, "ready", "ready"),
        ],
        publication=(DAY, 1, 4),
        live_rows=CORRECTED_ROWS,   # uncommitted bytes already in place
        rollback_rows=GOOD_ROWS,    # published bytes only in the backup
    )
    srv.start()
    try:
        dl = _http("GET", f"{srv.base}/download/{DAY}")
        assert dl[0] == 200
        assert _rain_mm(dl[1]) == GOOD_MM  # published rev1, not the rev2 bytes
        status = json.loads(_http("GET", f"{srv.base}/status/{DAY}")[1])
        assert status["current_publication"]["revision_id"] == 1
        assert status["current_publication"]["partition_path"].endswith(
            f"{DAY}-rev1.parquet"
        )
        # Uncommitted live file and the leftover backup are gone.
        assert _partition_names(srv.data_dir) == {f"{DAY}-rev1.parquet"}
        assert not list((srv.data_dir / "tmp" / "parquet").glob("rollback-*"))
        events = [e["event"] for e in json.loads(_http("GET", f"{srv.base}/revisions/1/events")[1])]
        assert "recovered" in events

        # Newest unfinished revision continues; the switch happens only via
        # a real recompute + commit.
        _http("POST", f"{srv.base}/admin/resume")
        final = _wait_terminal(srv.base, DAY)
        assert final["latest_task"]["revision_id"] == 2
        assert final["latest_task"]["status"] == "published"
        assert _rain_mm(_http("GET", f"{srv.base}/download/{DAY}")[1]) == CORRECTED_MM
    finally:
        srv.stop()


def test_legacy_window_renamed_away_missing_file_is_restored(tmp_path):
    """Old code crashed between renaming the live file aside and replacing it.

    The publication row pointed at a path that no longer existed (旧分区下载
    404). Startup must bring the published partition back from the backup.
    """
    srv = Server(tmp_path, demo_delay="0.1")
    _legacy_data_dir(
        srv.data_dir,
        revisions=[
            (1, DAY, 1, "published", "published"),
            (2, DAY, 2, "ready", "ready"),
        ],
        publication=(DAY, 1, 4),
        live_rows=None,             # live path vanished: downloads were 404
        rollback_rows=GOOD_ROWS,
    )
    srv.start()
    try:
        dl = _http("GET", f"{srv.base}/download/{DAY}")
        assert dl[0] == 200
        assert _rain_mm(dl[1]) == GOOD_MM
        status = json.loads(_http("GET", f"{srv.base}/status/{DAY}")[1])
        assert status["current_publication"]["revision_id"] == 1
        assert _partition_names(srv.data_dir) == {f"{DAY}-rev1.parquet"}

        _http("POST", f"{srv.base}/admin/resume")
        final = _wait_terminal(srv.base, DAY)
        assert final["latest_task"]["status"] == "published"
        assert _rain_mm(_http("GET", f"{srv.base}/download/{DAY}")[1]) == CORRECTED_MM
    finally:
        srv.stop()


def test_legacy_window_restore_falls_back_to_rollback_when_csv_missing(tmp_path):
    """If the published revision's raw CSV is gone, the rollback backup is used."""
    srv = Server(tmp_path, demo_delay="0.1")
    _legacy_data_dir(
        srv.data_dir,
        revisions=[(1, DAY, 1, "published", "published")],
        publication=(DAY, 1, 4),
        live_rows=CORRECTED_ROWS,   # uncommitted bytes at the live path
        rollback_rows=GOOD_ROWS,    # only on-disk copy of the published bytes
        paused=False,
    )
    (srv.data_dir / "tmp" / "inputs" / f"{DAY}-r1.csv").unlink()
    srv.start()
    try:
        dl = _http("GET", f"{srv.base}/download/{DAY}")
        assert dl[0] == 200
        assert _rain_mm(dl[1]) == GOOD_MM
        status = json.loads(_http("GET", f"{srv.base}/status/{DAY}")[1])
        assert status["current_publication"]["revision_id"] == 1
        assert _partition_names(srv.data_dir) == {f"{DAY}-rev1.parquet"}
    finally:
        srv.stop()


def test_legacy_window_committed_is_not_rolled_back(tmp_path):
    """Old code crashed after the commit, leaving a stale rollback backup.

    The live file already is the published rev2 content; recovery must keep
    it (不得回退) and only migrate it to the immutable per-revision path.
    """
    srv = Server(tmp_path, demo_delay="0.1")
    _legacy_data_dir(
        srv.data_dir,
        revisions=[
            (1, DAY, 1, "published", "published"),
            (2, DAY, 2, "published", "published"),
        ],
        publication=(DAY, 2, 4),
        live_rows=CORRECTED_ROWS,   # committed content
        rollback_rows=GOOD_ROWS,    # stale backup of the previous partition
        paused=False,
    )
    srv.start()
    try:
        dl = _http("GET", f"{srv.base}/download/{DAY}")
        assert dl[0] == 200
        assert _rain_mm(dl[1]) == CORRECTED_MM  # not rolled back to rev1 bytes
        status = json.loads(_http("GET", f"{srv.base}/status/{DAY}")[1])
        assert status["current_publication"]["revision_id"] == 2
        assert status["current_publication"]["partition_path"].endswith(
            f"{DAY}-rev2.parquet"
        )
        assert status["latest_task"]["status"] == "published"
        assert status["worker_active"] is False
        assert _partition_names(srv.data_dir) == {f"{DAY}-rev2.parquet"}
        assert not list((srv.data_dir / "tmp" / "parquet").glob("rollback-*"))
        events = [e["event"] for e in json.loads(_http("GET", f"{srv.base}/revisions/2/events")[1])]
        assert "migrated" in events
    finally:
        srv.stop()
