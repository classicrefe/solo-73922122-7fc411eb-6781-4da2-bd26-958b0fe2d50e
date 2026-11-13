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

Crash-safe publication
----------------------
Every revision publishes to its own immutable file
(``partitions/<date>-rev<id>.parquet``). The ledger commit that repoints the
publication row is the single atomic switch; the previously published file is
never renamed or deleted before that commit. A crash therefore lands in one
of two safe states:

* after the file rename but before the commit — the ledger still points at
  the previous partition, which is untouched and keeps being served; the
  never-committed file is an orphan that startup garbage collection removes;
* after the commit — the ledger points at the new file, which is already
  durable; there is nothing to roll back.

Startup recovery additionally migrates data directories left by the old
in-place-swap implementation (including its crash windows: a renamed-away or
replaced live file whose publish never committed) back to a state where the
served bytes match the publication row.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import sys
from pathlib import Path

from . import db
from .config import settings

CLEAN_SCRIPT = Path(__file__).parent / "subprocs" / "clean.py"
ENCODE_SCRIPT = Path(__file__).parent / "subprocs" / "encode.py"

TERMINAL_STATUSES = {"published", "cancelled", "failed"}

# Backups created by the old in-place-swap implementation.
LEGACY_ROLLBACK_RE = re.compile(
    r"rollback-(?P<date>\d{4}-\d{2}-\d{2})-r(?P<seq>\d+)\.parquet"
)


class RevisionCancelled(RuntimeError):
    """Raised internally when a stage gate finds the revision was cancelled."""


def _fsync_dir(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_file(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class PipelineManager:
    # Points at which a one-shot hard crash can be armed for the restart demo.
    CRASH_POINTS = frozenset({"before_publish_commit", "after_publish_commit"})

    def __init__(self) -> None:
        # date -> serial queue; each date gets one long-lived worker task.
        self._queues: dict[str, asyncio.Queue[int]] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}
        # revision id -> handle of its currently running subprocess.
        self._processes: dict[int, asyncio.subprocess.Process] = {}
        self._procs_lock = asyncio.Lock()
        # Armed crash points (one-shot; a restart comes back disarmed).
        self._crash_points: set[str] = set()

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
        """Restore a consistent state, then re-queue unfinished work.

        Order matters: legacy migration reads the old rollback backups from
        the staging directory, transient directories are wiped afterwards,
        unreferenced partition files are garbage-collected next, and only
        then is the newest unfinished revision per date re-queued. Nothing
        ever auto-publishes at boot: unfinished work is redone from the
        retained raw CSV, and older revisions that were already superseded
        are discarded instead of being resurrected.
        """
        await self._migrate_legacy_layout()
        self._wipe_transient_dirs()
        self._gc_unreferenced_partitions()
        self._requeue_unfinished()

    async def _migrate_legacy_layout(self) -> None:
        """Repair data dirs left by the old in-place-swap implementation.

        The old code renamed the live partition aside
        (``tmp/parquet/rollback-<date>-r<seq>.parquet``) and moved the new
        file over ``partitions/<date>.parquet`` *before* committing the
        publication row. A crash in that window left the ledger pointing at
        bytes of a different revision (``旧发布修订对应新文件``) or at a file
        that no longer exists (``旧分区下载 404``). For every publication
        still using the legacy path:

        * a *foreign* rollback backup (belonging to a revision that is not
          the published one) proves the live file was swapped without a
          commit — the published partition is rebuilt from the published
          revision's retained raw CSV, falling back to the newest foreign
          backup, and the uncommitted live file is discarded;
        * otherwise the live file is known to match the publication row and
          is simply moved to its immutable per-revision path;
        * the publication row is then repointed at the migrated file.
        """
        for pub in db.list_publications():
            date = pub["date"]
            legacy_path = settings.legacy_partition_path(date)
            if pub["partition_path"] != str(legacy_path):
                continue  # already on the immutable per-revision layout
            pub_rev_id = int(pub["revision_id"])
            new_path = settings.partition_path(date, pub_rev_id)

            def _event(event: str, detail: str) -> None:
                # A hand-built or partially corrupted legacy ledger may
                # reference a missing revision; events are best-effort.
                if db.get_revision(pub_rev_id) is not None:
                    db.add_event(pub_rev_id, event, detail)

            foreign: list[tuple[int, Path]] = []
            for p in settings.staged_dir.glob(f"rollback-{date}-r*.parquet"):
                m = LEGACY_ROLLBACK_RE.fullmatch(p.name)
                if not m:
                    continue
                seq = int(m.group("seq"))
                rev = db.get_revision_by_date_seq(date, seq)
                if rev is None or int(rev["id"]) != pub_rev_id:
                    foreign.append((seq, p))
            foreign.sort()

            restored = False
            if foreign:
                # The live file (if any) holds bytes of a revision that never
                # committed: discard it and restore exactly what the
                # publication row refers to.
                with contextlib.suppress(FileNotFoundError):
                    legacy_path.unlink()
                restored = await self._rebuild_published_partition(pub_rev_id, new_path)
                if not restored:
                    # Newest crashed attempt's backup is the best on-disk
                    # copy of the published bytes.
                    os.replace(foreign[-1][1], new_path)
                    restored = True
                _event(
                    "recovered",
                    "published partition restored after a pre-commit crash "
                    "of a later revision (legacy layout)",
                )
            elif legacy_path.exists():
                os.replace(legacy_path, new_path)
                restored = True
                _event(
                    "migrated",
                    "legacy partition moved to immutable per-revision path",
                )
            else:
                _event(
                    "missing",
                    "published partition file not found during recovery",
                )
            if restored:
                _fsync_dir(settings.partition_dir)
                db.update_publication_path(date, str(new_path))

        # Rollback backups are only meaningful to the migration above; any
        # that remain are stale (e.g. the publish had in fact committed).
        for p in settings.staged_dir.glob("rollback-*.parquet"):
            with contextlib.suppress(FileNotFoundError):
                p.unlink()

    async def _rebuild_published_partition(
        self, revision_id: int, new_path: Path
    ) -> bool:
        """Recompute a published revision's partition from its retained CSV."""
        rev = db.get_revision(revision_id)
        if rev is None:
            return False
        input_path = Path(rev["input_path"])
        if not input_path.exists():
            return False
        arrow = settings.work_dir / f"recover-rev{revision_id}.arrow"
        staged = settings.staged_dir / f"recover-rev{revision_id}.parquet"
        try:
            await self._run_subprocess(
                revision_id,
                [
                    str(CLEAN_SCRIPT),
                    str(input_path),
                    str(arrow),
                    rev["date"],
                    str(revision_id),
                ],
                "recovery-clean",
            )
            await self._run_subprocess(
                revision_id,
                [str(ENCODE_SCRIPT), str(arrow), str(staged), str(revision_id)],
                "recovery-encode",
            )
            _fsync_file(staged)
            os.replace(staged, new_path)
            return True
        except Exception:  # noqa: BLE001 - fall back to the rollback backup
            return False
        finally:
            for p in (arrow, staged):
                with contextlib.suppress(FileNotFoundError):
                    p.unlink()

    def _wipe_transient_dirs(self) -> None:
        """Remove half-written Arrow/Parquet artefacts from before the crash.

        Raw uploaded CSVs are kept: unfinished revisions are rebuilt from
        them. Nothing is in flight yet when this runs.
        """
        for directory in (settings.work_dir, settings.staged_dir):
            for p in directory.iterdir():
                if p.is_file():
                    with contextlib.suppress(FileNotFoundError):
                        p.unlink()

    def _gc_unreferenced_partitions(self) -> None:
        """Delete partition files no publication row points at.

        This is what makes the publish window crash-safe: a file that was
        renamed into place but never committed is garbage-collected, while
        the file the ledger points at is never removed — so downloads keep
        serving exactly the current publication during recovery, instead of
        being switched off to hide the inconsistency.
        """
        referenced = {row["partition_path"] for row in db.list_publications()}
        for p in settings.partition_dir.iterdir():
            if p.is_file() and str(p) not in referenced:
                with contextlib.suppress(FileNotFoundError):
                    p.unlink()

    def _requeue_unfinished(self) -> None:
        """Re-queue the newest unfinished revision per date after a restart."""
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

            # Stage 3: publication gate + atomic pointer switch.
            await self._gate(revision_id, "publishing")
            db.update_revision(revision_id, status="ready", stage="ready")
            db.add_event(revision_id, "ready", "staged file awaits publication")
            rev = db.get_revision(revision_id)
            if rev is None or rev["status"] != "ready":
                raise RevisionCancelled("revision left ready state before publish")

            final_path = settings.partition_path(date, revision_id)
            previous = db.get_publication(date)
            previous_path = Path(previous["partition_path"]) if previous else None

            # Durable immutable artefact first, ledger pointer second. The
            # currently published file is never renamed or deleted before the
            # commit, so a crash anywhere in this window keeps downloads
            # serving exactly what the publication row says.
            _fsync_file(staged_path)
            os.replace(staged_path, final_path)
            _fsync_dir(settings.partition_dir)
            self._maybe_crash("before_publish_commit")

            if not db.set_published(revision_id, date, str(final_path), row_count):
                # Superseded while publishing: drop the never-committed file;
                # the previous publication stays fully intact.
                with contextlib.suppress(FileNotFoundError):
                    final_path.unlink()
                raise RevisionCancelled("publication rejected by ledger guard")
            db.add_event(revision_id, "switched", f"publication now at {final_path.name}")
            self._maybe_crash("after_publish_commit")

            # Committed — retire the superseded partition file. A crash
            # before this unlink only leaves an orphan that the startup
            # garbage collector removes; never roll the pointer back.
            if previous_path is not None and previous_path != final_path:
                with contextlib.suppress(FileNotFoundError):
                    previous_path.unlink()
                with contextlib.suppress(OSError):
                    _fsync_dir(settings.partition_dir)
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
    def arm_crash(self, point: str) -> None:
        """Arm a one-shot hard crash at a publish switch point (demo)."""
        if point not in self.CRASH_POINTS:
            raise ValueError(
                f"unknown crash point {point!r}; choose from {sorted(self.CRASH_POINTS)}"
            )
        self._crash_points.add(point)

    def armed_crash_points(self) -> list[str]:
        return sorted(self._crash_points)

    def _maybe_crash(self, point: str) -> None:
        """Hard-exit the process if this point is armed (power-loss demo).

        ``os._exit`` skips every cleanup path on purpose — this simulates the
        API dying mid-switch. The arm is in-memory, so the restarted process
        comes back disarmed and recovery can be observed uninterrupted.
        """
        if point in self._crash_points:
            os._exit(1)

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
