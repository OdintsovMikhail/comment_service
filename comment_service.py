from fastapi import FastAPI, HTTPException
from utility import get_connection
from schemas import CommentOut
from typing import List

app = FastAPI(
    title="BookService API",
    description="",
    version="1.0.0",
)


# ── GET /api/comment/meeting/{meeting_id} ─────────────────────────────────────

@app.get("/comment/meeting/{meeting_id}", response_model=List[CommentOut])
def get_meeting_comments(meeting_id: int):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT Id FROM dbo.Meeting WHERE Id = ?", meeting_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Meeting not found")

        cursor.execute(
            """
            SELECT c.Id, c.UserId, c.Text
            FROM   dbo.Comment c
            JOIN   dbo.MeetingComment mc ON mc.CommentId = c.Id
            WHERE  mc.MeetingId = ?
            """,
            meeting_id
        )
        rows = cursor.fetchall()

    return [CommentOut(id=r[0], user_id=r[1], text=r[2]) for r in rows]


# ── GET /api/comment/book/{book_id} ──────────────────────────────────────────

@app.get("/comment/book/{book_id}", response_model=List[CommentOut])
def get_book_comments(book_id: int):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT Id FROM dbo.book WHERE Id = ?", book_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Book not found")

        cursor.execute(
            """
            SELECT c.Id, c.UserId, c.Text
            FROM   dbo.Comment c
            JOIN   dbo.BookComment bc ON bc.CommentId = c.Id
            WHERE  bc.BookId = ?
            """,
            book_id
        )
        rows = cursor.fetchall()

    return [CommentOut(id=r[0], user_id=r[1], text=r[2]) for r in rows]