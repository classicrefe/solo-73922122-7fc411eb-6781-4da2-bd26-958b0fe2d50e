"""Pydantic response models for the public API."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class RevisionStatus(str, Enum):
    QUEUED = "queued"
    READY = "ready"
    PUBLISHED = "published"
    CANCELLED = "cancelled"
    FAILED = "failed"


class Stage(str, Enum):
    QUEUED = "queued"
    CLEANING = "cleaning"
    ENCODING = "encoding"
    PUBLISHING = "publishing"
    READY = "ready"
    PUBLISHED = "published"


class UploadResponse(BaseModel):
    date: str
    revision_id: int
    seq: int
    status: RevisionStatus
    stage: Stage
    invalidated_revision_ids: list[int] = Field(default_factory=list)
    message: str


class RevisionOut(BaseModel):
    revision_id: int
    date: str
    seq: int
    status: RevisionStatus
    stage: Stage
    running: bool
    error: str | None
    source_filename: str
    row_count: int
    created_at: str
    updated_at: str


class PublicationOut(BaseModel):
    date: str
    revision_id: int
    partition_path: str
    row_count: int
    published_at: str


class StatusResponse(BaseModel):
    date: str
    paused: bool
    latest_task: RevisionOut | None
    current_publication: PublicationOut | None
    worker_active: bool


class EventOut(BaseModel):
    id: int
    revision_id: int
    event: str
    detail: str | None
    created_at: str


class PauseResponse(BaseModel):
    paused: bool
    message: str


class KillResponse(BaseModel):
    revision_id: int
    killed: bool
    message: str


class ErrorResponse(BaseModel):
    detail: str
