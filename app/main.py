"""FastAPI application: upload, correction, status, download and admin demos.

Endpoints
---------
POST   /uploads/{date}                   upload/re-upload one UTC day (<= 8 MiB)
GET    /status/{date}                    latest task vs. current publication
GET    /revisions/{date}                 full revision history of a day
GET    /revisions/{revision_id}/events   revision event trail
GET    /download/{date}                  download the last successfully published
                                         Parquet partition (stable while processing)
POST   /admin/pause                      hold new pipeline stages
POST   /admin/resume
POST   /admin/revisions/{id}/kill        SIGKILL the live subprocess (demo)
POST   /admin/crash/arm?point=...        arm a one-shot hard crash around the
                                         publish commit (restart-recovery demo)
GET    /admin/crash                      list armed crash points
GET    /health
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import date as date_cls
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse

from . import db
from .config import MAX_UPLOAD_BYTES, settings
from .pipeline import PipelineManager
from .schemas import (
    CrashArmResponse,
    CrashPoint,
    CrashState,
    EventOut,
    KillResponse,
    PauseResponse,
    PublicationOut,
    RevisionOut,
    StatusResponse,
    UploadResponse,
)
from .validation import ValidationError, parse_csv


@asynccontextmanager
async def lifespan(app: FastAPI):
    await manager.start()
    try:
        yield
    finally:
        await manager.shutdown()


app = FastAPI(
    title="Rainfall Correction Partition API",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "Ingests corrected hourly station rainfall CSV (tenths of mm) into "
        "per-UTC-day Parquet partitions (mm). Every accepted upload creates a "
        "new revision; previously unpublished work is invalidated and can never "
        "publish, even if it finishes late."
    ),
)
manager = PipelineManager()


# --------------------------------------------------------------------- helpers
def _validate_date(value: str) -> str:
    try:
        date_cls.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid UTC date '{value}'") from exc
    return value


def _fsync_file(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _revision_out(rev: dict) -> RevisionOut:
    return RevisionOut(
        revision_id=rev["id"],
        date=rev["date"],
        seq=rev["seq"],
        status=rev["status"],
        stage=rev["stage"],
        running=manager.running_revision(rev["id"]),
        error=rev["error"],
        source_filename=rev["source_filename"],
        row_count=rev["row_count"],
        created_at=rev["created_at"],
        updated_at=rev["updated_at"],
    )


def _publication_out(pub: dict) -> PublicationOut:
    return PublicationOut(
        date=pub["date"],
        revision_id=pub["revision_id"],
        partition_path=pub["partition_path"],
        row_count=pub["row_count"],
        published_at=pub["published_at"],
    )


# --------------------------------------------------------------------- upload
@app.post(
    "/uploads/{date}",
    response_model=UploadResponse,
    status_code=202,
    summary="Upload (or correct) one UTC day of hourly rainfall",
)
async def upload(date: str, file: UploadFile = File(..., description="CSV, max 8 MiB")):
    _validate_date(date)

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="uploaded file is empty")
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"file is {len(data)} bytes; limit is {MAX_UPLOAD_BYTES} bytes (8 MiB)",
        )

    # API-side pre-flight validation so malformed uploads are rejected at the
    # door. The cleaning subprocess re-validates authoritatively.
    try:
        parse_csv(data, date)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.message) from exc

    seq = db.next_seq(date)
    input_path = settings.input_path(date, seq)
    input_path.write_bytes(data)
    _fsync_file(input_path)

    revision_id = db.insert_revision(
        date, seq, file.filename or "upload.csv", str(input_path)
    )
    invalidated = db.invalidate_open_for_date(date, revision_id)
    manager.submit(date, revision_id)

    return UploadResponse(
        date=date,
        revision_id=revision_id,
        seq=seq,
        status="queued",
        stage="queued",
        invalidated_revision_ids=invalidated,
        message=(
            "revision accepted"
            if not invalidated
            else f"revision accepted; {len(invalidated)} earlier unpublished revision(s) cancelled"
        ),
    )


# --------------------------------------------------------------------- status
@app.get("/status/{date}", response_model=StatusResponse, summary="Task vs. publication status")
async def status(date: str):
    _validate_date(date)
    latest = db.latest_revision(date)
    pub = db.get_publication(date)
    return StatusResponse(
        date=date,
        paused=db.get_paused(),
        latest_task=_revision_out(latest) if latest else None,
        current_publication=_publication_out(pub) if pub else None,
        worker_active=manager.is_worker_active(date),
    )


@app.get(
    "/revisions/{date}",
    response_model=list[RevisionOut],
    summary="All revisions of a UTC day (newest first)",
)
async def revisions(date: str):
    _validate_date(date)
    return [_revision_out(r) for r in db.list_revisions(date)]


@app.get(
    "/revisions/{revision_id}/events",
    response_model=list[EventOut],
    summary="Event trail of one revision",
)
async def revision_events(revision_id: int):
    if not db.get_revision(revision_id):
        raise HTTPException(status_code=404, detail=f"revision {revision_id} not found")
    return [EventOut(**e) for e in db.list_events(revision_id)]


# ------------------------------------------------------------------- download
@app.get(
    "/download/{date}",
    summary="Download the current published Parquet partition (millimetres)",
    response_class=FileResponse,
)
async def download(date: str):
    _validate_date(date)
    pub = db.get_publication(date)
    if pub is None or not Path(pub["partition_path"]).exists():
        raise HTTPException(status_code=404, detail=f"no published partition for {date}")
    return FileResponse(
        pub["partition_path"],
        media_type="application/vnd.apache.parquet",
        filename=f"rainfall-{date}.parquet",
    )


# --------------------------------------------------------------------- admin
@app.post("/admin/pause", response_model=PauseResponse, summary="Pause pipeline stage starts")
async def pause():
    db.set_paused(True)
    return PauseResponse(
        paused=True,
        message="pipeline paused; running subprocesses finish, no new stages start",
    )


@app.post("/admin/resume", response_model=PauseResponse, summary="Resume the pipeline")
async def resume():
    db.set_paused(False)
    return PauseResponse(paused=False, message="pipeline resumed")


@app.post(
    "/admin/revisions/{revision_id}/kill",
    response_model=KillResponse,
    summary="Force-kill a revision's live subprocess (failure demo)",
)
async def kill(revision_id: int):
    rev = db.get_revision(revision_id)
    if rev is None:
        raise HTTPException(status_code=404, detail=f"revision {revision_id} not found")
    killed = await manager.force_kill(revision_id)
    if not killed:
        raise HTTPException(
            status_code=409,
            detail=(
                f"revision {revision_id} has no running subprocess "
                f"(stage={rev['stage']}, status={rev['status']})"
            ),
        )
    return KillResponse(
        revision_id=revision_id,
        killed=True,
        message="SIGKILL delivered; the revision will be marked failed",
    )


@app.post(
    "/admin/crash/arm",
    response_model=CrashArmResponse,
    summary="Arm a one-shot hard crash around the publish commit (restart demo)",
)
async def arm_crash(
    point: CrashPoint = Query(
        ...,
        description=(
            "before_publish_commit: new partition file renamed into place, "
            "publication row not yet committed. after_publish_commit: "
            "publication row committed, superseded file not yet retired."
        ),
    ),
):
    manager.arm_crash(point.value)
    return CrashArmResponse(
        armed=True,
        point=point.value,
        armed_points=manager.armed_crash_points(),
        message=(
            "the next publish reaching this point hard-exits the process "
            "(os._exit, no cleanup); restart the container to watch recovery. "
            "One-shot: the restarted process comes back disarmed."
        ),
    )


@app.get(
    "/admin/crash",
    response_model=CrashState,
    summary="List currently armed crash points",
)
async def crash_state():
    return CrashState(armed_points=manager.armed_crash_points())


@app.get("/health", summary="Liveness probe")
async def health():
    return {"status": "ok", "paused": db.get_paused()}


# Test-only self-shutdown hook. Never registered unless explicitly enabled.
if os.environ.get("RAIN_ENABLE_SHUTDOWN") == "1":

    @app.post("/test/shutdown", include_in_schema=False)
    async def _test_shutdown():
        await manager.shutdown()
        os._exit(0)
