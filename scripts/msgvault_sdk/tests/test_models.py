"""Tests for msgvault_sdk data models."""

from __future__ import annotations

from datetime import datetime

from msgvault_sdk.models import (
    Account,
    Attachment,
    Conversation,
    Label,
    Message,
    Participant,
)


class TestParticipant:
    def test_from_row(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, email_address, phone_number, display_name, domain "
            "FROM participants WHERE id = 1"
        ).fetchone()
        p = Participant.from_row(row)
        assert p.id == 1
        assert p.email == "alice@example.com"
        assert p.phone is None
        assert p.display_name == "Alice Smith"
        assert p.domain == "example.com"

    def test_repr(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, email_address, phone_number, display_name, domain "
            "FROM participants WHERE id = 1"
        ).fetchone()
        p = Participant.from_row(row)
        assert "alice@example.com" in repr(p)


class TestLabel:
    def test_from_row(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, name, label_type, source_id FROM labels WHERE id = 1"
        ).fetchone()
        label = Label.from_row(row)
        assert label.id == 1
        assert label.name == "INBOX"
        assert label.label_type == "system"

    def test_repr(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, name, label_type, source_id FROM labels WHERE id = 1"
        ).fetchone()
        label = Label.from_row(row)
        assert "INBOX" in repr(label)


class TestAttachment:
    def test_from_row(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, message_id, filename, mime_type, size, content_hash, media_type "
            "FROM attachments WHERE id = 1"
        ).fetchone()
        att = Attachment.from_row(row)
        assert att.id == 1
        assert att.message_id == 4
        assert att.filename == "q3-report.pdf"
        assert att.mime_type == "application/pdf"
        assert att.size == 48000

    def test_repr(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, message_id, filename, mime_type, size, content_hash, media_type "
            "FROM attachments WHERE id = 1"
        ).fetchone()
        att = Attachment.from_row(row)
        assert "q3-report.pdf" in repr(att)


class TestConversation:
    def test_from_row(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, title, conversation_type, message_count, last_message_at "
            "FROM conversations WHERE id = 1"
        ).fetchone()
        conv = Conversation.from_row(row)
        assert conv.id == 1
        assert conv.title == "Project Discussion"
        assert conv.conversation_type == "email_thread"
        assert conv.message_count == 6
        assert isinstance(conv.last_message_at, datetime)

    def test_repr(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, title, conversation_type, message_count, last_message_at "
            "FROM conversations WHERE id = 1"
        ).fetchone()
        conv = Conversation.from_row(row)
        assert "Project Discussion" in repr(conv)


class TestAccount:
    def test_from_row(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, source_type, identifier, display_name, last_sync_at "
            "FROM sources WHERE id = 1"
        ).fetchone()
        acct = Account.from_row(row)
        assert acct.id == 1
        assert acct.source_type == "gmail"
        assert acct.identifier == "test@gmail.com"
        assert acct.display_name == "Test User"

    def test_repr(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, source_type, identifier, display_name, last_sync_at "
            "FROM sources WHERE id = 1"
        ).fetchone()
        acct = Account.from_row(row)
        assert "test@gmail.com" in repr(acct)


class TestMessage:
    def test_from_row(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 1"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert msg.id == 1
        assert msg.subject == "Hello from Alice"
        assert msg.message_type == "email"
        assert msg.is_read is True
        assert msg.is_from_me is False
        assert isinstance(msg.sent_at, datetime)
        assert msg.deleted_at is None

    def test_date_alias(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 1"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert msg.date == msg.sent_at

    def test_size_alias(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 1"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert msg.size == msg.size_estimate == 1500

    def test_lazy_sender(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 1"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert msg.sender is not None
        assert msg.sender.email == "alice@example.com"
        assert msg.sender.display_name == "Alice Smith"

    def test_lazy_sender_none(self, db_conn) -> None:
        # Insert a message with no sender
        db_conn.execute(
            "INSERT INTO messages (id, conversation_id, source_id, source_message_id, "
            "message_type, sent_at, subject, snippet, sender_id, is_read, is_from_me, "
            "has_attachments, size_estimate, deleted_at) "
            "VALUES (99, 1, 1, 'msg-99', 'email', '2024-01-01T00:00:00Z', "
            "'System', 'sys', NULL, 1, 0, 0, 100, NULL)"
        )
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 99"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert msg.sender is None

    def test_lazy_body(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 1"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert msg.body == "Hello Alice here, just saying hi!"
        assert msg.html_body == "<p>Hello Alice here</p>"

    def test_lazy_body_no_html(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 3"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert msg.body is not None
        assert msg.html_body is None

    def test_lazy_labels(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 2"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        label_names = {l.name for l in msg.labels}
        assert label_names == {"INBOX", "IMPORTANT"}

    def test_lazy_recipients(self, db_conn) -> None:
        # Message 2: bob -> alice (to), admin (cc)
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 2"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert len(msg.recipients) == 2
        to = msg.to
        assert len(to) == 1
        assert to[0].email == "alice@example.com"
        cc = msg.cc
        assert len(cc) == 1
        assert cc[0].email == "admin@example.com"

    def test_lazy_attachments(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 4"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert len(msg.attachments) == 2
        filenames = {a.filename for a in msg.attachments}
        assert filenames == {"q3-report.pdf", "chart.png"}

    def test_lazy_conversation(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 1"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        conv = msg.conversation
        assert conv is not None
        assert conv.title == "Project Discussion"

    def test_repr(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 1"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        r = repr(msg)
        assert "id=1" in r
        assert "Hello from Alice" in r

    def test_has_attachments_flag(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 4"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert msg.has_attachments is True

    def test_deleted_message(self, db_conn) -> None:
        row = db_conn.execute(
            "SELECT id, conversation_id, source_id, message_type, sent_at, "
            "subject, snippet, is_read, is_from_me, has_attachments, "
            "size_estimate, deleted_at, sender_id "
            "FROM messages WHERE id = 10"
        ).fetchone()
        msg = Message.from_row(row, db_conn)
        assert msg.deleted_at is not None
