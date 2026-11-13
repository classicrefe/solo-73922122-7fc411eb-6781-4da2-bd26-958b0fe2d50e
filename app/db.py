"""SQLite ledger: revisions, publications, events and pipeline state.

All access is funnelled through one connection guarded by a re-entrant lock;
WAL mode keeps concurrent readers (download/status) responsive while the
pipeline writes state transitions.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any

from .config import settings

SCHEMA = """
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

CREATE INDEX IF NOT EXISTS idx_revisions_date ON revisions(date, id DESC);
CREATE INDEX IF NOT EXISTS idx_events_rev ON revision_events(revision_id, id);
"""

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def connect() -> sqlite3.Connection:
    global _conn
    with _lock:
        if _conn is None:
            conn = sqlite3.connect(
                settings.db_path, check_same_thread=False, isolation_level=None
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            # FULL: a committed publication row must survive a process kill.
            # The ledger is tiny and written infrequently, so the fsync per
            # commit is cheap insurance for the publish-switch guarantee.
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(SCHEMA)
            _conn = conn
        return _conn


@contextmanager
def tx() -> Iterator[sqlite3.Connection]:
    """Serialized write transaction (BEGIN IMMEDIATE)."""
    conn = connect()
    with _lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def _now() -> str:
    return connect().execute(
        "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')"
    ).fetchone()[0]


def add_event(revision_id: int, event: str, detail: str | None = None) -> None:
    with _lock:
        connect().execute(
            "INSERT INTO revision_events(revision_id, event, detail) VALUES (?,?,?)",
            (revision_id, event, detail),
        )


def next_seq(date: str) -> int:
    with tx() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM revisions WHERE date = ?",
            (date,),
        ).fetchone()
        return int(row["next"])


def insert_revision(
    date: str, seq: int, source_filename: str, input_path: str
) -> int:
    with tx() as conn:
        cur = conn.execute(
            """INSERT INTO revisions(date, seq, status, stage, source_filename, input_path)
               VALUES (?,?,'queued','queued',?,?)""",
            (date, seq, source_filename, input_path),
        )
        rev_id = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO revision_events(revision_id, event) VALUES (?, 'created')",
            (rev_id,),
        )
    return rev_id


def invalidate_open_for_date(
    date: str, new_rev_id: int, reason: str = "superseded by newer upload"
) -> list[int]:
    """Mark every still-unfinished revision of ``date`` as cancelled.

    Published revisions are terminal and stay untouched. Returns the ids
    that were invalidated.
    """
    terminal = ("published", "cancelled", "failed")
    out: list[int] = []
    with tx() as conn:
        rows = conn.execute(
            f"SELECT id, status, stage FROM revisions WHERE date = ? "
            f"AND id != ? AND status NOT IN ({','.join('?' for _ in terminal)})",
            (date, new_rev_id, *terminal),
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE revisions SET status='cancelled', error=?, updated_at=? WHERE id=?",
                (reason, _now(), row["id"]),
            )
            conn.execute(
                "INSERT INTO revision_events(revision_id, event, detail) VALUES (?, 'cancelled', ?)",
                (row["id"], reason),
            )
            out.append(int(row["id"]))
    add_event(new_rev_id, "supersedes", ",".join(map(str, out)) if out else None)
    return out


def get_revision(revision_id: int) -> dict[str, Any] | None:
    with _lock:
        row = connect().execute(
            "SELECT * FROM revisions WHERE id = ?", (revision_id,)
        ).fetchone()
    return dict(row) if row else None


def get_revision_by_date_seq(date: str, seq: int) -> dict[str, Any] | None:
    with _lock:
        row = connect().execute(
            "SELECT * FROM revisions WHERE date = ? AND seq = ?", (date, seq)
        ).fetchone()
    return dict(row) if row else None


def list_revisions(date: str) -> list[dict[str, Any]]:
    with _lock:
        rows = connect().execute(
            "SELECT * FROM revisions WHERE date = ? ORDER BY id DESC", (date,)
        ).fetchall()
    return [dict(r) for r in rows]


def latest_revision(date: str) -> dict[str, Any] | None:
    with _lock:
        row = connect().execute(
            "SELECT * FROM revisions WHERE date = ? ORDER BY id DESC LIMIT 1", (date,)
        ).fetchone()
    return dict(row) if row else None


def update_revision(
    revision_id: int,
    *,
    status: str | None = None,
    stage: str | None = None,
    error: str | None = None,
    row_count: int | None = None,
) -> None:
    fields: list[str] = ["updated_at = ?"]
    params: list[Any] = [_now()]
    if status is not None:
        fields.append("status = ?")
        params.append(status)
    if stage is not None:
        fields.append("stage = ?")
        params.append(stage)
    if error is not None:
        fields.append("error = ?")
        params.append(error)
    if row_count is not None:
        fields.append("row_count = ?")
        params.append(row_count)
    params.append(revision_id)
    with _lock:
        connect().execute(
            f"UPDATE revisions SET {', '.join(fields)} WHERE id = ?", params
        )


def mark_failed_if_active(revision_id: int, stage: str, error: str) -> bool:
    """Transition to ``failed`` only when the revision is still active."""
    with tx() as conn:
        row = conn.execute(
            "SELECT status FROM revisions WHERE id = ?", (revision_id,)
        ).fetchone()
        if row is None or row["status"] in ("cancelled", "published", "failed"):
            return False
        conn.execute(
            "UPDATE revisions SET status='failed', stage=?, error=?, updated_at=? WHERE id=?",
            (stage, error, _now(), revision_id),
        )
        conn.execute(
            "INSERT INTO revision_events(revision_id, event, detail) VALUES (?, 'failed', ?)",
            (revision_id, f"{stage}: {error}"),
        )
        return True


def set_published(revision_id: int, date: str, partition_path: str, row_count: int) -> bool:
    """Conditional publish: succeeds only if the revision is still active.

    The new partition file already sits at its final immutable path when this
    is called; the commit below is the single atomic pointer switch. When it
    returns False the caller unlinks the never-committed file — the previous
    publication's file is never touched either way.
    """
    with tx() as conn:
        row = conn.execute(
            "SELECT status FROM revisions WHERE id = ?", (revision_id,)
        ).fetchone()
        if row is None or row["status"] != "ready":
            return False
        conn.execute(
            "UPDATE revisions SET status='published', stage='published', "
            "row_count=?, updated_at=? WHERE id=?",
            (row_count, _now(), revision_id),
        )
        conn.execute(
            """INSERT INTO publications(date, revision_id, partition_path, row_count)
               VALUES (?,?,?,?)
               ON CONFLICT(date) DO UPDATE SET
                 revision_id=excluded.revision_id,
                 partition_path=excluded.partition_path,
                 row_count=excluded.row_count,
                 published_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')""",
            (date, revision_id, partition_path, row_count),
        )
        conn.execute(
            "INSERT INTO revision_events(revision_id, event) VALUES (?, 'published')",
            (revision_id,),
        )
        return True


def get_publication(date: str) -> dict[str, Any] | None:
    with _lock:
        row = connect().execute(
            "SELECT * FROM publications WHERE date = ?", (date,)
        ).fetchone()
    return dict(row) if row else None


def list_publications() -> list[dict[str, Any]]:
    with _lock:
        rows = connect().execute("SELECT * FROM publications ORDER BY date").fetchall()
    return [dict(r) for r in rows]


def update_publication_path(date: str, partition_path: str) -> None:
    """Repoint a publication at its migrated partition file (startup recovery)."""
    with tx() as conn:
        conn.execute(
            "UPDATE publications SET partition_path = ? WHERE date = ?",
            (partition_path, date),
        )


def list_events(revision_id: int) -> list[dict[str, Any]]:
    with _lock:
        rows = connect().execute(
            "SELECT id, revision_id, event, detail, created_at "
            "FROM revision_events WHERE revision_id = ? ORDER BY id",
            (revision_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ---- pipeline pause switch -------------------------------------------------

def get_paused() -> bool:
    with _lock:
        row = connect().execute(
            "SELECT value FROM pipeline_state WHERE key='paused'"
        ).fetchone()
    return bool(row and row["value"] == "1")


def set_paused(paused: bool) -> None:
    with tx() as conn:
        conn.execute(
            "INSERT INTO pipeline_state(key, value) VALUES('paused', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("1" if paused else "0",),
        )


def non_terminal_revisions() -> list[dict[str, Any]]:
    terminal = ("published", "cancelled", "failed")
    with _lock:
        rows = connect().execute(
            f"SELECT * FROM revisions WHERE status NOT IN "
            f"({','.join('?' for _ in terminal)}) ORDER BY id",
            terminal,
        ).fetchall()
    return [dict(r) for r in rows]


def reset_for_recovery(revision_id: int) -> None:
    """Re-queue an unfinished revision found at startup."""
    with tx() as conn:
        conn.execute(
            "UPDATE revisions SET status='queued', stage='queued', "
            "error='recovered after API restart', updated_at=? WHERE id=?",
            (_now(), revision_id),
        )
        conn.execute(
            "INSERT INTO revision_events(revision_id, event, detail) "
            "VALUES (?, 'recovered', 'restart: revision re-queued')",
            (revision_id,),
        )
