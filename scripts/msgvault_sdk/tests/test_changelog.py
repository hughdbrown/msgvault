"""Tests for the change log module."""

from __future__ import annotations

import json

import pytest

from msgvault_sdk.changelog import ChangeLog, ensure_changelog_table
from msgvault_sdk.errors import ChangeLogError


@pytest.fixture()
def changelog(db_conn) -> ChangeLog:
    """A ChangeLog bound to the seeded test database."""
    return ChangeLog(db_conn)


class TestEnsureTable:
    def test_creates_table(self, db_conn) -> None:
        ensure_changelog_table(db_conn)
        row = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='change_log'"
        ).fetchone()
        assert row is not None

    def test_idempotent(self, db_conn) -> None:
        ensure_changelog_table(db_conn)
        ensure_changelog_table(db_conn)  # should not raise


class TestChangeLogRecord:
    def test_record_entry(self, changelog, db_conn) -> None:
        entry_id = changelog._record(
            "delete", [1, 2, 3], details={"reason": "cleanup"}
        )
        assert entry_id is not None
        entry = changelog.last()
        assert entry.operation == "delete"
        assert entry.message_ids == [1, 2, 3]
        assert entry.message_count == 3
        assert entry.details == {"reason": "cleanup"}
        assert entry.is_undone is False
        db_conn.commit()

    def test_record_without_details(self, changelog, db_conn) -> None:
        changelog._record("delete", [1])
        entry = changelog.last()
        assert entry.details is None
        db_conn.commit()


class TestChangeLogIteration:
    def test_empty_log(self, changelog) -> None:
        assert len(changelog) == 0
        assert list(changelog) == []

    def test_iteration_order(self, changelog, db_conn) -> None:
        changelog._record("delete", [1])
        changelog._record("label_add", [2])
        changelog._record("delete", [3])
        db_conn.commit()

        entries = list(changelog)
        assert len(entries) == 3
        # Newest first (descending ID)
        assert entries[0].message_ids == [3]
        assert entries[1].message_ids == [2]
        assert entries[2].message_ids == [1]

    def test_last(self, changelog, db_conn) -> None:
        changelog._record("delete", [1])
        changelog._record("label_add", [2, 3])
        db_conn.commit()

        last = changelog.last()
        assert last.operation == "label_add"
        assert last.message_ids == [2, 3]

    def test_last_empty(self, changelog) -> None:
        assert changelog.last() is None

    def test_len(self, changelog, db_conn) -> None:
        changelog._record("delete", [1])
        changelog._record("delete", [2])
        db_conn.commit()
        assert len(changelog) == 2


class TestChangeLogUndo:
    def test_undo_delete(self, changelog, db_conn) -> None:
        # Soft-delete messages 1 and 2
        db_conn.execute(
            "UPDATE messages SET deleted_at = '2024-01-01T00:00:00Z' "
            "WHERE id IN (1, 2)"
        )
        changelog._record("delete", [1, 2])
        db_conn.commit()

        # Verify they're deleted
        row = db_conn.execute(
            "SELECT deleted_at FROM messages WHERE id = 1"
        ).fetchone()
        assert row["deleted_at"] is not None

        # Undo
        changelog.undo_last()

        # Verify restored
        row = db_conn.execute(
            "SELECT deleted_at FROM messages WHERE id = 1"
        ).fetchone()
        assert row["deleted_at"] is None

        row = db_conn.execute(
            "SELECT deleted_at FROM messages WHERE id = 2"
        ).fetchone()
        assert row["deleted_at"] is None

        # Verify entry marked as undone
        last = changelog.last()
        assert last.is_undone is True
        assert last.undone_at is not None

    def test_undo_label_add(self, changelog, db_conn) -> None:
        # Record a label_add with label_id in details
        changelog._record(
            "label_add", [1, 2],
            details={"label": "Archive", "label_id": 3},
        )
        # Actually insert the labels
        db_conn.execute(
            "INSERT OR IGNORE INTO message_labels (message_id, label_id) VALUES (1, 3)"
        )
        db_conn.execute(
            "INSERT OR IGNORE INTO message_labels (message_id, label_id) VALUES (2, 3)"
        )
        db_conn.commit()

        # Undo should remove the label associations
        changelog.undo_last()

        rows = db_conn.execute(
            "SELECT * FROM message_labels WHERE label_id = 3 AND message_id IN (1, 2)"
        ).fetchall()
        assert len(rows) == 0

    def test_undo_label_remove(self, changelog, db_conn) -> None:
        # Message 1 had label 1 (INBOX). Record its removal.
        changelog._record(
            "label_remove", [1],
            details={"label": "INBOX"},
            undo_data={"label_id": 1},
        )
        # Actually remove it
        db_conn.execute(
            "DELETE FROM message_labels WHERE message_id = 1 AND label_id = 1"
        )
        db_conn.commit()

        # Verify removed
        row = db_conn.execute(
            "SELECT COUNT(*) FROM message_labels WHERE message_id = 1 AND label_id = 1"
        ).fetchone()
        assert row[0] == 0

        # Undo should re-add
        changelog.undo_last()

        row = db_conn.execute(
            "SELECT COUNT(*) FROM message_labels WHERE message_id = 1 AND label_id = 1"
        ).fetchone()
        assert row[0] == 1

    def test_undo_already_undone(self, changelog, db_conn) -> None:
        db_conn.execute(
            "UPDATE messages SET deleted_at = '2024-01-01T00:00:00Z' WHERE id = 1"
        )
        changelog._record("delete", [1])
        db_conn.commit()

        changelog.undo_last()

        # Second undo should fail — no non-undone entries
        with pytest.raises(ChangeLogError, match="nothing to undo"):
            changelog.undo_last()

    def test_undo_nothing(self, changelog) -> None:
        with pytest.raises(ChangeLogError, match="nothing to undo"):
            changelog.undo_last()


class TestChangeLogExport:
    def test_export_json(self, changelog, db_conn) -> None:
        changelog._record("delete", [1, 2])
        changelog._record("label_add", [3], details={"label": "Archive"})
        db_conn.commit()

        exported = changelog.export_json()
        data = json.loads(exported)
        assert len(data) == 2
        assert data[0]["operation"] == "label_add"
        assert data[1]["operation"] == "delete"

    def test_export_empty(self, changelog) -> None:
        exported = changelog.export_json()
        data = json.loads(exported)
        assert data == []
