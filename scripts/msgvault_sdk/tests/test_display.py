"""Tests for __repr__ and _repr_html_ display helpers."""

from __future__ import annotations

import pytest

from msgvault_sdk.changelog import ChangeLog
from msgvault_sdk.query import MessageQuery


@pytest.fixture()
def mq(db_conn) -> MessageQuery:
    cl = ChangeLog(db_conn)
    return MessageQuery(db_conn, changelog=cl)


class TestMessageQueryRepr:
    def test_repr_no_filters(self, mq) -> None:
        r = repr(mq)
        assert "MessageQuery" in r
        assert "filters=0" in r

    def test_repr_with_filters(self, mq) -> None:
        r = repr(mq.filter(sender="alice@example.com"))
        assert "filters=1" in r

    def test_repr_with_sort(self, mq) -> None:
        r = repr(mq.sort_by("date", desc=True))
        assert "date_desc" in r

    def test_repr_with_limit(self, mq) -> None:
        r = repr(mq.limit(5))
        assert "limit=5" in r


class TestMessageQueryReprHtml:
    def test_returns_html(self, mq) -> None:
        html = mq._repr_html_()
        assert "<table>" in html
        assert "</table>" in html

    def test_contains_headers(self, mq) -> None:
        html = mq._repr_html_()
        assert "<th>ID</th>" in html
        assert "<th>From</th>" in html
        assert "<th>Subject</th>" in html

    def test_contains_message_data(self, mq) -> None:
        html = mq._repr_html_()
        assert "alice@example.com" in html

    def test_limits_to_10_rows(self, db_conn) -> None:
        # Our test DB only has 9 non-deleted messages, so no footer
        cl = ChangeLog(db_conn)
        mq = MessageQuery(db_conn, changelog=cl)
        html = mq._repr_html_()
        assert "Showing 10 of" not in html


class TestGroupedQueryReprHtml:
    def test_returns_html(self, mq) -> None:
        html = mq.group_by("sender")._repr_html_()
        assert "<table>" in html
        assert "</table>" in html

    def test_contains_headers(self, mq) -> None:
        html = mq.group_by("sender")._repr_html_()
        assert "<th>Key</th>" in html
        assert "<th>Count</th>" in html

    def test_contains_group_data(self, mq) -> None:
        html = mq.group_by("sender")._repr_html_()
        assert "alice@example.com" in html


class TestGroupedQueryRepr:
    def test_repr(self, mq) -> None:
        r = repr(mq.group_by("sender"))
        assert "GroupedQuery" in r
        assert "sender" in r
