"""Core data models for msgvault_sdk."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


def _parse_datetime(value: str | None) -> datetime | None:
    """Parse a SQLite datetime string into a Python datetime."""
    if value is None:
        return None
    # Handle multiple formats that SQLite may produce
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(value)


@dataclass(frozen=True, slots=True)
class Participant:
    """A contact (sender or recipient)."""

    id: int
    email: str | None
    phone: str | None
    display_name: str | None
    domain: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Participant:
        return cls(
            id=row["id"],
            email=row["email_address"],
            phone=row["phone_number"],
            display_name=row["display_name"],
            domain=row["domain"],
        )

    def __repr__(self) -> str:
        identifier = self.email or self.phone or f"id={self.id}"
        return f"Participant({identifier!r})"


@dataclass(frozen=True, slots=True)
class Label:
    """A Gmail label or user tag."""

    id: int
    name: str
    label_type: str | None
    source_id: int | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Label:
        return cls(
            id=row["id"],
            name=row["name"],
            label_type=row["label_type"],
            source_id=row["source_id"],
        )

    def __repr__(self) -> str:
        return f"Label({self.name!r})"


@dataclass(frozen=True, slots=True)
class Attachment:
    """An email attachment."""

    id: int
    message_id: int
    filename: str | None
    mime_type: str | None
    size: int | None
    content_hash: str | None
    media_type: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Attachment:
        return cls(
            id=row["id"],
            message_id=row["message_id"],
            filename=row["filename"],
            mime_type=row["mime_type"],
            size=row["size"],
            content_hash=row["content_hash"],
            media_type=row["media_type"],
        )

    def __repr__(self) -> str:
        name = self.filename or f"id={self.id}"
        return f"Attachment({name!r})"


@dataclass(frozen=True, slots=True)
class Conversation:
    """A thread or chat conversation."""

    id: int
    title: str | None
    conversation_type: str
    message_count: int
    last_message_at: datetime | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Conversation:
        return cls(
            id=row["id"],
            title=row["title"],
            conversation_type=row["conversation_type"],
            message_count=row["message_count"],
            last_message_at=_parse_datetime(row["last_message_at"]),
        )

    def __repr__(self) -> str:
        title = self.title or f"id={self.id}"
        return f"Conversation({title!r})"


@dataclass(frozen=True, slots=True)
class Account:
    """A message source (Gmail account, etc.)."""

    id: int
    source_type: str
    identifier: str
    display_name: str | None
    last_sync_at: datetime | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Account:
        return cls(
            id=row["id"],
            source_type=row["source_type"],
            identifier=row["identifier"],
            display_name=row["display_name"],
            last_sync_at=_parse_datetime(row["last_sync_at"]),
        )

    def __repr__(self) -> str:
        return f"Account({self.identifier!r})"


class Message:
    """An email message with lazy-loaded related data.

    Scalar fields are loaded eagerly. Related data (sender, recipients, body,
    labels, attachments, conversation) is loaded on first access.
    """

    __slots__ = (
        "id",
        "conversation_id",
        "source_id",
        "message_type",
        "sent_at",
        "subject",
        "snippet",
        "is_read",
        "is_from_me",
        "has_attachments",
        "size_estimate",
        "deleted_at",
        "sender_id",
        "_conn",
        "_sender",
        "_sender_loaded",
        "_recipients",
        "_body",
        "_body_loaded",
        "_html_body",
        "_html_body_loaded",
        "_labels",
        "_attachments",
        "_conversation_obj",
    )

    def __init__(
        self,
        *,
        id: int,
        conversation_id: int,
        source_id: int,
        message_type: str,
        sent_at: datetime | None,
        subject: str | None,
        snippet: str | None,
        is_read: bool,
        is_from_me: bool,
        has_attachments: bool,
        size_estimate: int | None,
        deleted_at: datetime | None,
        sender_id: int | None,
        conn: sqlite3.Connection,
    ) -> None:
        self.id = id
        self.conversation_id = conversation_id
        self.source_id = source_id
        self.message_type = message_type
        self.sent_at = sent_at
        self.subject = subject
        self.snippet = snippet
        self.is_read = is_read
        self.is_from_me = is_from_me
        self.has_attachments = has_attachments
        self.size_estimate = size_estimate
        self.deleted_at = deleted_at
        self.sender_id = sender_id
        self._conn = conn
        # Lazy-load sentinels
        self._sender: Participant | None = None
        self._sender_loaded = False
        self._recipients: list[Participant] | None = None
        self._body: str | None = None
        self._body_loaded = False
        self._html_body: str | None = None
        self._html_body_loaded = False
        self._labels: list[Label] | None = None
        self._attachments: list[Attachment] | None = None
        self._conversation_obj: Conversation | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row, conn: sqlite3.Connection) -> Message:
        return cls(
            id=row["id"],
            conversation_id=row["conversation_id"],
            source_id=row["source_id"],
            message_type=row["message_type"],
            sent_at=_parse_datetime(row["sent_at"]),
            subject=row["subject"],
            snippet=row["snippet"],
            is_read=bool(row["is_read"]),
            is_from_me=bool(row["is_from_me"]),
            has_attachments=bool(row["has_attachments"]),
            size_estimate=row["size_estimate"],
            deleted_at=_parse_datetime(row["deleted_at"]),
            sender_id=row["sender_id"],
            conn=conn,
        )

    @property
    def date(self) -> datetime | None:
        """Alias for sent_at."""
        return self.sent_at

    @property
    def size(self) -> int | None:
        """Alias for size_estimate."""
        return self.size_estimate

    @property
    def sender(self) -> Participant | None:
        if not self._sender_loaded:
            self._sender_loaded = True
            if self.sender_id is not None:
                row = self._conn.execute(
                    "SELECT id, email_address, phone_number, display_name, domain "
                    "FROM participants WHERE id = ?",
                    (self.sender_id,),
                ).fetchone()
                if row:
                    self._sender = Participant.from_row(row)
        return self._sender

    @property
    def recipients(self) -> list[Participant]:
        if self._recipients is None:
            rows = self._conn.execute(
                "SELECT p.id, p.email_address, p.phone_number, p.display_name, p.domain "
                "FROM message_recipients mr "
                "JOIN participants p ON p.id = mr.participant_id "
                "WHERE mr.message_id = ?",
                (self.id,),
            ).fetchall()
            self._recipients = [Participant.from_row(r) for r in rows]
        return self._recipients

    def _recipients_by_type(self, recipient_type: str) -> list[Participant]:
        rows = self._conn.execute(
            "SELECT p.id, p.email_address, p.phone_number, p.display_name, p.domain "
            "FROM message_recipients mr "
            "JOIN participants p ON p.id = mr.participant_id "
            "WHERE mr.message_id = ? AND mr.recipient_type = ?",
            (self.id, recipient_type),
        ).fetchall()
        return [Participant.from_row(r) for r in rows]

    @property
    def to(self) -> list[Participant]:
        return self._recipients_by_type("to")

    @property
    def cc(self) -> list[Participant]:
        return self._recipients_by_type("cc")

    @property
    def bcc(self) -> list[Participant]:
        return self._recipients_by_type("bcc")

    @property
    def body(self) -> str | None:
        if not self._body_loaded:
            self._body_loaded = True
            row = self._conn.execute(
                "SELECT body_text FROM message_bodies WHERE message_id = ?",
                (self.id,),
            ).fetchone()
            if row:
                self._body = row["body_text"]
        return self._body

    @property
    def html_body(self) -> str | None:
        if not self._html_body_loaded:
            self._html_body_loaded = True
            row = self._conn.execute(
                "SELECT body_html FROM message_bodies WHERE message_id = ?",
                (self.id,),
            ).fetchone()
            if row:
                self._html_body = row["body_html"]
        return self._html_body

    @property
    def labels(self) -> list[Label]:
        if self._labels is None:
            rows = self._conn.execute(
                "SELECT l.id, l.name, l.label_type, l.source_id "
                "FROM message_labels ml "
                "JOIN labels l ON l.id = ml.label_id "
                "WHERE ml.message_id = ?",
                (self.id,),
            ).fetchall()
            self._labels = [Label.from_row(r) for r in rows]
        return self._labels

    @property
    def attachments(self) -> list[Attachment]:
        if self._attachments is None:
            rows = self._conn.execute(
                "SELECT id, message_id, filename, mime_type, size, "
                "content_hash, media_type "
                "FROM attachments WHERE message_id = ?",
                (self.id,),
            ).fetchall()
            self._attachments = [Attachment.from_row(r) for r in rows]
        return self._attachments

    @property
    def conversation(self) -> Conversation | None:
        if self._conversation_obj is None:
            row = self._conn.execute(
                "SELECT id, title, conversation_type, message_count, last_message_at "
                "FROM conversations WHERE id = ?",
                (self.conversation_id,),
            ).fetchone()
            if row:
                self._conversation_obj = Conversation.from_row(row)
        return self._conversation_obj

    def __repr__(self) -> str:
        subj = self.subject or "(no subject)"
        if len(subj) > 40:
            subj = subj[:37] + "..."
        return f"Message(id={self.id}, subject={subj!r})"
