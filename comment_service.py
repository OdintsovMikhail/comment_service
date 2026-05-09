import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException

from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusReceiveMode
from azure.identity.aio import DefaultAzureCredential

from utility import get_connection, DB_SCHEMA
from broker import SOURCE_BOOK, SOURCE_MEETING, QUEUE_NAME, _listen_client
from schemas import CommentOut
from typing import List

load_dotenv()

logger = logging.getLogger("comment_service")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

S = DB_SCHEMA


# ── DB handlers ───────────────────────────────────────────────────────────────

def _handle_book_comment(data: dict) -> None:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"INSERT INTO [{S}].Comment (UserId, Text) OUTPUT INSERTED.Id VALUES (?, ?)",
            data["user_id"], data["text"],
        )
        comment_id = cursor.fetchone()[0]
        cursor.execute(
            f"INSERT INTO [{S}].BookComment (CommentId, BookId) VALUES (?, ?)",
            comment_id, data["book_id"],
        )
        conn.commit()
    logger.info("BookComment saved — comment_id=%s book_id=%s", comment_id, data["book_id"])


def _handle_meeting_comment(data: dict) -> None:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"INSERT INTO [{S}].Comment (UserId, Text) OUTPUT INSERTED.Id VALUES (?, ?)",
            data["user_id"], data["text"],
        )
        comment_id = cursor.fetchone()[0]
        cursor.execute(
            f"INSERT INTO [{S}].MeetingComment (CommentId, MeetingId) VALUES (?, ?)",
            comment_id, data["meeting_id"],
        )
        conn.commit()
    logger.info("MeetingComment saved — comment_id=%s meeting_id=%s", comment_id, data["meeting_id"])


# ── Consumer loop ─────────────────────────────────────────────────────────────

async def _consume() -> None:
    loop = asyncio.get_event_loop()

    async with _listen_client() as client:
        async with client.get_queue_receiver(
            queue_name=QUEUE_NAME,
            receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
        ) as receiver:
            logger.info("Consumer ready — queue=%s", QUEUE_NAME)

            while True:
                messages = await receiver.receive_messages(
                    max_message_count=10,
                    max_wait_time=5,
                )
                for msg in messages:
                    try:
                        data = json.loads(str(msg))
                        source = data.get("source")
                        logger.info("Received [%s]: %s", source, data)

                        if source == SOURCE_BOOK:
                            await loop.run_in_executor(None, _handle_book_comment, data)
                        elif source == SOURCE_MEETING:
                            await loop.run_in_executor(None, _handle_meeting_comment, data)
                        else:
                            logger.warning("Unknown source '%s', skipping", source)

                        await receiver.complete_message(msg)
                    except Exception as exc:
                        logger.error("Failed: %s — %s", msg, exc)
                        await receiver.abandon_message(msg)


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_consume())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


app = FastAPI(
    title="CommentService API",
    description="Consumes Azure Service Bus messages; exposes read endpoints.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── HTTP read endpoints ───────────────────────────────────────────────────────

@app.get("/comment/meeting/{meeting_id}", response_model=List[CommentOut])
def get_meeting_comments(meeting_id: int):
    logger.info("Searching for meeting with id %s", meeting_id)
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT Id FROM [{S}].Meeting WHERE Id = ?", meeting_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Meeting not found")
        cursor.execute(
            f"""
            SELECT c.Id, c.UserId, c.Text
            FROM   [{S}].Comment c
            JOIN   [{S}].MeetingComment mc ON mc.CommentId = c.Id
            WHERE  mc.MeetingId = ?
            """,
            meeting_id,
        )
        rows = cursor.fetchall()

    logger.info("Meeting with id %s found", meeting_id)
    return [CommentOut(id=r[0], user_id=r[1], text=r[2]) for r in rows]


@app.get("/comment/book/{book_id}", response_model=List[CommentOut])
def get_book_comments(book_id: int):
    logger.info("Searching for book with id %s", book_id)
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT Id FROM [{S}].book WHERE Id = ?", book_id)
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Book not found")
        cursor.execute(
            f"""
            SELECT c.Id, c.UserId, c.Text
            FROM   [{S}].Comment c
            JOIN   [{S}].BookComment bc ON bc.CommentId = c.Id
            WHERE  bc.BookId = ?
            """,
            book_id,
        )
        rows = cursor.fetchall()

    logger.info("Book with id %s found", book_id)
    return [CommentOut(id=r[0], user_id=r[1], text=r[2]) for r in rows]