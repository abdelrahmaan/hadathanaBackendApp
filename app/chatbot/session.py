import logging

from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.chatbot.models import ChatSession, Citation, SessionMessage
from app.config import settings

logger = logging.getLogger("hadathana.chatbot.session")


def _collection_name() -> str:
    """Return environment-specific collection name."""
    return "chat_sessions_dev" if settings.is_dev else "chat_sessions_prod"


async def get_or_create_session(
    db: AsyncIOMotorDatabase,
    session_id: str | None,
    user_id: str,
) -> ChatSession:
    """Load existing session (with ownership check) or create a new one."""
    col = _collection_name()
    if session_id:
        doc = await db[col].find_one({"session_id": session_id})
        if doc:
            doc.pop("_id", None)
            session = ChatSession(**doc)
            if session.user_id != user_id:
                raise HTTPException(status_code=403, detail="Session belongs to another user.")
            return session
    return ChatSession(user_id=user_id)


async def update_session_title(
    db: AsyncIOMotorDatabase,
    session_id: str,
    title: str,
) -> None:
    """Persist the auto-generated title on the session document."""
    col = _collection_name()
    await db[col].update_one(
        {"session_id": session_id},
        {"$set": {"title": title}},
    )


async def append_turn(
    db: AsyncIOMotorDatabase,
    session: ChatSession,
    user_content: str,
    assistant_content: str,
    citations: list[Citation],
) -> None:
    """Append user + assistant messages to session and upsert into Mongo."""
    col = _collection_name()
    session.messages.append(SessionMessage(role="user", content=user_content))
    session.messages.append(
        SessionMessage(role="assistant", content=assistant_content, citations=citations)
    )
    await db[col].update_one(
        {"session_id": session.session_id},
        {"$set": session.model_dump()},
        upsert=True,
    )
    logger.info(
        "session_updated",
        extra={
            "event": "session_updated",
            "collection": col,
            "session_id": session.session_id,
            "turns": len(session.messages) // 2,
        },
    )
