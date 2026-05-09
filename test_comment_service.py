"""
pytest test suite for comment_service.py
Covers HTTP endpoints and DB handler logic.
Message bus (broker/consumer) is excluded per requirements.
"""

import json
import pytest
from unittest.mock import MagicMock, patch, call
from fastapi.testclient import TestClient


# ── Patch heavy external dependencies before importing the app ────────────────

# Prevent real Azure Service Bus connections during import / lifespan startup
import unittest.mock as mock

_asb_patch   = mock.patch("comment_service._listen_client", return_value=MagicMock())
_consume_patch = mock.patch("comment_service._consume", return_value=mock.AsyncMock())

_asb_patch.start()
_consume_patch.start()

from comment_service import app, _handle_book_comment, _handle_meeting_comment  # noqa: E402

_asb_patch.stop()
_consume_patch.stop()


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def client():
    async def _noop(): pass

    with mock.patch("comment_service._consume", new=_noop):
        with TestClient(app, raise_server_exceptions=True) as c:
            yield c


def _make_cursor(rows=None, fetchone_value=None):
    """Return a mock cursor pre-configured with common return values."""
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone_value
    cursor.fetchall.return_value = rows or []
    return cursor


def _make_conn(cursor):
    """Return a mock connection that yields *cursor* from .cursor()."""
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn


# ══════════════════════════════════════════════════════════════════════════════
# _handle_book_comment
# ══════════════════════════════════════════════════════════════════════════════

class TestHandleBookComment:

    def test_inserts_comment_and_book_comment(self):
        cursor = _make_cursor(fetchone_value=(42,))
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            _handle_book_comment({"user_id": 1, "text": "Great book!", "book_id": 7})

        # First execute → INSERT into Comment
        first_call_args = cursor.execute.call_args_list[0]
        assert "INSERT INTO [dbo].Comment" in first_call_args[0][0]
        assert first_call_args[0][1:] == (1, "Great book!")

        # Second execute → INSERT into BookComment with the returned id
        second_call_args = cursor.execute.call_args_list[1]
        assert "INSERT INTO [dbo].BookComment" in second_call_args[0][0]
        assert second_call_args[0][1:] == (42, 7)

        conn.commit.assert_called_once()

    def test_commit_is_called_on_success(self):
        cursor = _make_cursor(fetchone_value=(1,))
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            _handle_book_comment({"user_id": 2, "text": "Nice", "book_id": 3})

        conn.commit.assert_called_once()

    def test_propagates_db_error(self):
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("DB unavailable")
        conn = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            with pytest.raises(Exception, match="DB unavailable"):
                _handle_book_comment({"user_id": 1, "text": "x", "book_id": 1})


# ══════════════════════════════════════════════════════════════════════════════
# _handle_meeting_comment
# ══════════════════════════════════════════════════════════════════════════════

class TestHandleMeetingComment:

    def test_inserts_comment_and_meeting_comment(self):
        cursor = _make_cursor(fetchone_value=(99,))
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            _handle_meeting_comment({"user_id": 5, "text": "Good session", "meeting_id": 10})

        first_call_args = cursor.execute.call_args_list[0]
        assert "INSERT INTO [dbo].Comment" in first_call_args[0][0]
        assert first_call_args[0][1:] == (5, "Good session")

        second_call_args = cursor.execute.call_args_list[1]
        assert "INSERT INTO [dbo].MeetingComment" in second_call_args[0][0]
        assert second_call_args[0][1:] == (99, 10)

        conn.commit.assert_called_once()

    def test_commit_is_called_on_success(self):
        cursor = _make_cursor(fetchone_value=(2,))
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            _handle_meeting_comment({"user_id": 3, "text": "OK", "meeting_id": 4})

        conn.commit.assert_called_once()

    def test_propagates_db_error(self):
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("timeout")
        conn = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            with pytest.raises(RuntimeError, match="timeout"):
                _handle_meeting_comment({"user_id": 1, "text": "x", "meeting_id": 1})


# ══════════════════════════════════════════════════════════════════════════════
# GET /comment/meeting/{meeting_id}
# ══════════════════════════════════════════════════════════════════════════════

class TestGetMeetingComments:

    def test_returns_comments_for_valid_meeting(self, client):
        cursor = _make_cursor(
            rows=[(1, 10, "Hello"), (2, 11, "World")],
            fetchone_value=(5,),   # meeting exists
        )
        conn = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            resp = client.get("/comment/meeting/5")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 2
        assert body[0] == {"id": 1, "user_id": 10, "text": "Hello"}
        assert body[1] == {"id": 2, "user_id": 11, "text": "World"}

    def test_returns_empty_list_when_no_comments(self, client):
        cursor = _make_cursor(rows=[], fetchone_value=(5,))
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            resp = client.get("/comment/meeting/5")

        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_404_when_meeting_not_found(self, client):
        cursor = _make_cursor(fetchone_value=None)   # SELECT returns nothing
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            resp = client.get("/comment/meeting/999")

        assert resp.status_code == 404
        assert resp.json()["detail"] == "Meeting not found"

    def test_queries_correct_meeting_id(self, client):
        cursor = _make_cursor(rows=[], fetchone_value=(7,))
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            client.get("/comment/meeting/7")

        # First execute must filter by meeting id 7
        first_args = cursor.execute.call_args_list[0][0]
        assert first_args[1] == 7


# ══════════════════════════════════════════════════════════════════════════════
# GET /comment/book/{book_id}
# ══════════════════════════════════════════════════════════════════════════════

class TestGetBookComments:

    def test_returns_comments_for_valid_book(self, client):
        cursor = _make_cursor(
            rows=[(3, 20, "Loved it"), (4, 21, "Meh")],
            fetchone_value=(1,),   # book exists
        )
        conn = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            resp = client.get("/comment/book/1")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 2
        assert body[0] == {"id": 3, "user_id": 20, "text": "Loved it"}

    def test_returns_empty_list_when_no_comments(self, client):
        cursor = _make_cursor(rows=[], fetchone_value=(1,))
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            resp = client.get("/comment/book/1")

        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_404_when_book_not_found(self, client):
        cursor = _make_cursor(fetchone_value=None)
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            resp = client.get("/comment/book/999")

        assert resp.status_code == 404
        assert resp.json()["detail"] == "Book not found"

    def test_queries_correct_book_id(self, client):
        cursor = _make_cursor(rows=[], fetchone_value=(8,))
        conn   = _make_conn(cursor)

        with patch("comment_service.get_connection", return_value=conn):
            client.get("/comment/book/8")

        first_args = cursor.execute.call_args_list[0][0]
        assert first_args[1] == 8


# ══════════════════════════════════════════════════════════════════════════════
# Schema validation — CommentOut
# ══════════════════════════════════════════════════════════════════════════════

class TestCommentOutSchema:
    from schemas import CommentOut

    def test_valid_comment(self):
        from schemas import CommentOut
        c = CommentOut(id=1, user_id=2, text="hello")
        assert c.id == 1
        assert c.user_id == 2
        assert c.text == "hello"

    def test_missing_field_raises(self):
        from pydantic import ValidationError
        from schemas import CommentOut
        with pytest.raises(ValidationError):
            CommentOut(id=1, text="oops")   # user_id missing