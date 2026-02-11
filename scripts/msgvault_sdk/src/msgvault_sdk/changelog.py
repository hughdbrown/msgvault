"""Change log for tracking mutations to the msgvault database."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

from msgvault_sdk.errors import ChangeLogError
from msgvault_sdk.models import _parse_datetime

CHANGE_LOG_SCHEMA = """\
CREATE TABLE IF NOT EXISTS change_log (
    id INTEGER PRIMARY KEY,
    operation TEXT NOT NULL,
    created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    message_ids TEXT NOT NULL,
    message_count INTEGER NOT NULL,
    details TEXT,
    is_undone INTEGER DEFAULT 0,
    undone_at TEXT,
    undo_data TEXT
)"""


def ensure_changelog_table(conn: sqlite3.Connection) -> None:
    """Create the change_log table if it does not exist."""
    conn.execute(CHANGE_LOG_SCHEMA)


@dataclass(slots=True)
class ChangeEntry:
    """A single entry in the change log."""

    id: int
    operation: str
    created_at: datetime | None
    message_ids: list[int]
    message_count: int
    details: dict | None
    is_undone: bool
    undone_at: datetime | None
    undo_data: dict | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> ChangeEntry:
        return cls(
            id=row["id"],
            operation=row["operation"],
            created_at=_parse_datetime(row["created_at"]),
            message_ids=json.loads(row["message_ids"]),
            message_count=row["message_count"],
            details=json.loads(row["details"]) if row["details"] else None,
            is_undone=bool(row["is_undone"]),
            undone_at=_parse_datetime(row["undone_at"]),
            undo_data=json.loads(row["undo_data"]) if row["undo_data"] else None,
        )

    def __repr__(self) -> str:
        return (
            f"ChangeEntry(op={self.operation!r}, "
            f"count={self.message_count}, undone={self.is_undone})"
        )


class ChangeLog:
    """Interface to the change_log table."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def __iter__(self) -> Iterator[ChangeEntry]:
        ensure_changelog_table(self._conn)
        rows = self._conn.execute(
            "SELECT id, operation, created_at, message_ids, message_count, "
            "details, is_undone, undone_at, undo_data "
            "FROM change_log ORDER BY id DESC"
        ).fetchall()
        for row in rows:
            yield ChangeEntry.from_row(row)

    def __len__(self) -> int:
        ensure_changelog_table(self._conn)
        row = self._conn.execute("SELECT COUNT(*) FROM change_log").fetchone()
        return row[0]

    def last(self) -> ChangeEntry | None:
        """Return the most recent entry, or None if the log is empty."""
        ensure_changelog_table(self._conn)
        row = self._conn.execute(
            "SELECT id, operation, created_at, message_ids, message_count, "
            "details, is_undone, undone_at, undo_data "
            "FROM change_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return ChangeEntry.from_row(row)

    def undo_last(self) -> None:
        """Undo the most recent non-undone entry."""
        ensure_changelog_table(self._conn)
        row = self._conn.execute(
            "SELECT id, operation, created_at, message_ids, message_count, "
            "details, is_undone, undone_at, undo_data "
            "FROM change_log WHERE is_undone = 0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise ChangeLogError("nothing to undo")

        entry = ChangeEntry.from_row(row)
        self._execute_undo(entry)

        self._conn.execute(
            "UPDATE change_log SET is_undone = 1, "
            "undone_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
            "WHERE id = ?",
            (entry.id,),
        )
        self._conn.commit()

    def _execute_undo(self, entry: ChangeEntry) -> None:
        """Execute the reverse operation for a change log entry."""
        op = entry.operation
        ids = entry.message_ids

        if op == "delete":
            placeholders = ",".join("?" for _ in ids)
            self._conn.execute(
                f"UPDATE messages SET deleted_at = NULL "
                f"WHERE id IN ({placeholders})",
                ids,
            )

        elif op == "undelete":
            # Restore the original deleted_at values
            if entry.undo_data and "deleted_at_values" in entry.undo_data:
                for mid_str, ts in entry.undo_data["deleted_at_values"].items():
                    self._conn.execute(
                        "UPDATE messages SET deleted_at = ? WHERE id = ?",
                        (ts, int(mid_str)),
                    )

        elif op == "label_add":
            if entry.details and "label_id" in entry.details:
                label_id = entry.details["label_id"]
                placeholders = ",".join("?" for _ in ids)
                self._conn.execute(
                    f"DELETE FROM message_labels "
                    f"WHERE label_id = ? AND message_id IN ({placeholders})",
                    [label_id, *ids],
                )

        elif op == "label_remove":
            if entry.undo_data and "label_id" in entry.undo_data:
                label_id = entry.undo_data["label_id"]
                self._conn.executemany(
                    "INSERT OR IGNORE INTO message_labels (message_id, label_id) "
                    "VALUES (?, ?)",
                    [(mid, label_id) for mid in ids],
                )

        else:
            raise ChangeLogError(f"unknown operation: {op!r}")

    def export_json(self) -> str:
        """Export all entries as JSON."""
        entries = []
        for entry in self:
            entries.append({
                "id": entry.id,
                "operation": entry.operation,
                "created_at": entry.created_at.isoformat() if entry.created_at else None,
                "message_ids": entry.message_ids,
                "message_count": entry.message_count,
                "details": entry.details,
                "is_undone": entry.is_undone,
                "undone_at": entry.undone_at.isoformat() if entry.undone_at else None,
                "undo_data": entry.undo_data,
            })
        return json.dumps(entries, indent=2)

    def _record(
        self,
        operation: str,
        message_ids: list[int],
        details: dict | None = None,
        undo_data: dict | None = None,
    ) -> int:
        """Record a change log entry. Returns the entry ID."""
        ensure_changelog_table(self._conn)
        cursor = self._conn.execute(
            "INSERT INTO change_log (operation, message_ids, message_count, "
            "details, undo_data) VALUES (?, ?, ?, ?, ?)",
            (
                operation,
                json.dumps(message_ids),
                len(message_ids),
                json.dumps(details) if details else None,
                json.dumps(undo_data) if undo_data else None,
            ),
        )
        return cursor.lastrowid

    def __repr__(self) -> str:
        return f"ChangeLog(entries={len(self)})"
