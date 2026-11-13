"""Asynchronous revision pipeline.

Concurrency model
-----------------
* One serial asyncio worker per UTC date: revisions of the same date never run
  concurrently, while different dates run fully in parallel.
* Cleaning and Parquet encoding are separate OS subprocesses. Encoding starts
  only after the cleaner exits successfully.
* A revision re-checks the ledger before every stage and before publishing.
  A revision that was cancelled (superseded, restart, ...) stops immediately,
  so a late-finishing subprocess can never be published.
* The partition file is swapped into place atomically and the publication row
  is committed in the same guarded transition; any failure leaves the
  previously published partition untouched and downloadable.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
from pathlib import Path

from . import db
from .config import settings

CLEAN_SCRIPT = Path(__file__).parent / "subprocs" / "clean.py"
ENCODE_SCRIPT = Path(__file__).parent / "subprocs" / "encode.py"

TERMINAL_STATUSES = {"published", "cancelled", "failed"}


class RevisionCancelled(RuntimeError):
    """Raised internally when a stage gate finds the revision was cancelled."""


def _fsync_dir(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class PipelineManager:
    def __init__(self) -> None:
        # date -> serial queue; each date gets one long-lived worker task.
        self._queues: dict[str, asyncio.Queue[int]] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}
        # revision id -> handle of its currently running subprocess.
        self._processes: dict[int, asyncio.subprocess.Process] = {}
        self._procs_lock = asyncio.Lock()

    # ------------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        db.connect()
        await self._recover()

    async def shutdown(self) -> None:
        for task in self._workers.values():
            task.cancel()
        for proc in list(self._processes.values()):
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
        for task in list(self._workers.values()):
            with contextlib.suppress(asyncio.CancelledError):
                await task

    # ------------------------------------------------------------------ submission
    def submit(self, date: str, revision_id: int) -> None:
        q = self._queues.get(date)
        if q is None:
            q = asyncio.Queue()
            self._queues[date] = q
            self._workers[date] = asyncio.create_task(
                self._date_worker(date, q), name=f"worker-{date}"
            )
        q.put_nowait(revision_id)

    def is_worker_active(self, date: str) -> bool:
        task = self._workers.get(date)
        return bool(task and not task.done())

    # ------------------------------------------------------------------ recovery
    async def _recover(self) -> None:
        """Re-queue the newest unfinished revision per date after a restart.

        Nothing ever auto-publishes at boot: unfinished work is redone from the
        retained raw CSV, and older revisions that were already superseded are
        discarded instead of being resurrected.
        """
        open_revs = db.non_terminal_revisions()
        by_date: dict[str, list[dict]] = {}
        for rev in open_revs:
            by_date.setdefault(rev["date"], []).append(rev)

        for date, revs in by_date.items():
            revs.sort(key=lambda r: r["id"])
            newest = revs[-1]
            stale = revs[:-1]
            for rev in stale:
                db.update_revision(
                    rev["id"],
                    status="cancelled",
                    stage=rev["stage"],
                    error="superseded before API restart; not resumed",
                )
                db.add_event(rev["id"], "cancelled", "stale revision discarded at restart")
            # Remove partial artefacts; the raw CSV stays for an audit trail.
            for path in (
                settings.work_arrow_path(newest["id"]),
                settings.staged_parquet_path(newest["id"]),
            ):
                with contextlib.suppress(FileNotFoundError):
                    path.unlink()
            db.reset_for_recovery(newest["id"])
            self.submit(date, newest["id"])

    # ------------------------------------------------------------------ worker
    async def _date_worker(
        self, date: str, queue: "asyncio.Queue[int]"
    ) -> None:
        while True:
            revision_id = await queue.get()
            try:
                await self._run_revision(revision_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - defensive top-level guard
                db.mark_failed_if_active(
                    revision_id, "pipeline", f"internal error: {exc!r}"
                )
            finally:
                queue.task_done()

    # ------------------------------------------------------------------ gates
    async def _gate(self, revision_id: int, stage: str) -> None:
        """Block while the pipeline is paused; abort if the revision is dead.

        Active statuses are ``queued`` and ``ready``; any terminal status
        (published/cancelled/failed) means the work must stop immediately.
        """
        while True:
            rev = db.get_revision(revision_id)
            if rev is None or rev["status"] in TERMINAL_STATUSES:
                raise RevisionCancelled(
                    f"revision {revision_id} status={rev['status'] if rev else 'missing'}"
                )
            if not db.get_paused():
                db.update_revision(revision_id, stage=stage)
                db.add_event(revision_id, "stage", stage)
                if settings.demo_delay > 0:
                    await asyncio.sleep(settings.demo_delay)
                return
            db.add_event(revision_id, "paused", f"waiting at stage {stage}")
            await asyncio.sleep(0.25)

    async def _run_subprocess(
        self,
        revision_id: int,
        args: list[str],
        stage: str,
    ) -> dict:
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-P",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={
                **os.environ,
                "PYTHONDONTWRITEBYTECODE": "1",
                # Root containing the ``app`` package (container: /app).
                "RAIN_APP_ROOT": str(Path(__file__).resolve().parent.parent),
                # Artificial work time inside the child so pause/kill demos
                # have a reliable window; real files take longer anyway.
                "RAIN_DEMO_WORK_DELAY": str(settings.demo_delay),
            },
        )
        async with self._procs_lock:
            self._processes[revision_id] = proc
        try:
            stdout, stderr = await proc.communicate()
        finally:
            async with self._procs_lock:
                self._processes.pop(revision_id, None)

        payload: dict = {}
        for line in reversed(stdout.decode("utf-8", "replace").splitlines()):
            line = line.strip()
            if line.startswith("{"):
                try:
                    payload = json.loads(line)
                    break
                except json.JSONDecodeError:
                    continue
        if proc.returncode != 0:
            detail = payload.get("error") if payload else None
            if not detail:
                detail = (
                    f"{stage} subprocess exited {proc.returncode}: "
                    f"{stderr.decode('utf-8', 'replace')[-500:] or 'no stderr'}"
                )
            raise RuntimeError(detail)
        if not payload.get("ok"):
            raise RuntimeError(payload.get("error", f"{stage} reported failure"))
        return payload

    # ------------------------------------------------------------------ pipeline
    async def _run_revision(self, revision_id: int) -> None:
        rev = db.get_revision(revision_id)
        if rev is None:
            return
        date = rev["date"]
        arrow_path = settings.work_arrow_path(revision_id)
        staged_path = settings.staged_parquet_path(revision_id)
        for p in (arrow_path, staged_path):
            with contextlib.suppress(FileNotFoundError):
                p.unlink()

        try:
            # Stage 1: cleaning subprocess (full re-validation lives there).
            await self._gate(revision_id, "cleaning")
            clean = await self._run_subprocess(
                revision_id,
                [
                    str(CLEAN_SCRIPT),
                    rev["input_path"],
                    str(arrow_path),
                    date,
                    str(revision_id),
                ],
                "cleaning",
            )
            row_count = int(clean.get("row_count", 0))
            db.update_revision(revision_id, row_count=row_count)
            db.add_event(revision_id, "cleaned", f"{row_count} rows")

            # Stage 2: encoding subprocess — only after cleaning succeeded.
            await self._gate(revision_id, "encoding")
            enc = await self._run_subprocess(
                revision_id,
                [str(ENCODE_SCRIPT), str(arrow_path), str(staged_path), str(revision_id)],
                "encoding",
            )
            row_count = int(enc.get("row_count", row_count))
            db.update_revision(revision_id, stage="ready", row_count=row_count)
            db.add_event(revision_id, "encoded", f"{row_count} rows staged")

            # Stage 3: publication gate + atomic swap + guarded commit.
            await self._gate(revision_id, "publishing")
            db.update_revision(revision_id, status="ready", stage="ready")
            db.add_event(revision_id, "ready", "staged file awaits publication")
            rev = db.get_revision(revision_id)
            if rev is None or rev["status"] != "ready":
                raise RevisionCancelled("revision left ready state before publish")

            target = settings.partition_path(date)
            # Keep the currently served file (if any) so the swap can be fully
            # rolled back when the ledger rejects the publication.
            backup: Path | None = None
            if target.exists():
                backup = settings.staged_dir / f"rollback-{date}-r{rev['seq']}.parquet"
                with contextlib.suppress(FileNotFoundError):
                    backup.unlink()
                os.replace(target, backup)
            try:
                os.replace(staged_path, target)
                _fsync_dir(settings.partition_dir)

                if not db.set_published(revision_id, date, str(target), row_count):
                    raise RevisionCancelled("publication rejected by ledger guard")
            except BaseException:
                # Restore the previous partition; never leave a half-swapped
                # file or expose the losing revision's bytes.
                with contextlib.suppress(FileNotFoundError):
                    target.unlink()
                if backup is not None:
                    os.replace(backup, target)
                _fsync_dir(settings.partition_dir)
                raise

            if backup is not None:
                with contextlib.suppress(FileNotFoundError):
                    backup.unlink()
            with contextlib.suppress(FileNotFoundError):
                arrow_path.unlink()

        except RevisionCancelled as exc:
            db.add_event(revision_id, "aborted", str(exc))
            for p in (arrow_path, staged_path):
                with contextlib.suppress(FileNotFoundError):
                    p.unlink()
        except Exception as exc:  # noqa: BLE001
            if db.mark_failed_if_active(revision_id, rev["stage"], str(exc)):
                for p in (arrow_path, staged_path):
                    with contextlib.suppress(FileNotFoundError):
                        p.unlink()
            else:
                db.add_event(revision_id, "aborted", f"after termination: {exc}")

    # ------------------------------------------------------------------ admin
    async def force_kill(self, revision_id: int) -> bool:
        """SIGKILL the revision's live subprocess (admin demo)."""
        async with self._procs_lock:
            proc = self._processes.get(revision_id)
            if proc is None or proc.returncode is not None:
                return False
            try:
                proc.kill()
            except (ProcessLookupError, PermissionError):
                return False
        db.add_event(revision_id, "force_killed", "SIGKILL delivered by operator")
        return True

    def running_revision(self, revision_id: int) -> bool:
        proc = self._processes.get(revision_id)
        return bool(proc and proc.returncode is None)
