import asyncio
import json
import logging
import re
from datetime import date, datetime, time, timedelta
from datetime import timezone as tz

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from app.auth.config import current_active_user
from app.auth.models import User
from app.chatbot.agent import generate_title, get_agent, get_last_docs
from app.chatbot.models import (
    ChatRequest,
    ChatSession,
    ChatSessionMeta,
    Citation,
    QuotaStatus,
)
from app.chatbot.quota import check_quota, increment_quota
from app.chatbot.session import append_turn, get_or_create_session, update_session_title
from app.config import settings
from app.database import get_client, get_db, get_user_quotas_collection
from langsmith.run_helpers import tracing_context

logger = logging.getLogger("hadathana.chatbot.router")

router = APIRouter(prefix="/api/v2", tags=["chatbot"])


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _extract_token(chunk) -> str:
    """
    Extract a text token from a create_agent astream(stream_mode="messages") chunk.
    Each chunk is a tuple: (AIMessageChunk | ToolMessage | ..., metadata).
    We only want AIMessageChunk text — skip tool calls and tool results.
    """
    if not isinstance(chunk, tuple) or len(chunk) < 1:
        return ""
    msg = chunk[0]
    # Only process AI message chunks, not tool results or other message types
    from langchain_core.messages import AIMessageChunk
    if not isinstance(msg, AIMessageChunk):
        return ""
    content = getattr(msg, "content", None)
    if not content or not isinstance(content, str):
        return ""
    # Skip chunks that are purely tool-call invocations
    if getattr(msg, "tool_call_chunks", None):
        return ""
    return content


@router.post("/chat")
async def chat(
    request: ChatRequest,
    user: User = Depends(current_active_user),
    _: None = Depends(check_quota),
):
    db = get_db(get_client())
    session = await get_or_create_session(db, request.session_id, str(user.id))
    agent = get_agent()

    logger.info(
        "chat_request",
        extra={
            "event": "chat_request",
            "session_id": session.session_id,
            "question_len": len(request.question),
            "topic": request.topic,
            "book": request.book,
        },
    )

    async def event_stream():
        assembled: list[str] = []
        # Rolling tail buffer to detect the REFS line before emitting tokens
        tail: str = ""

        logger.info("-" * 50, extra={"event": "pipeline_separator"})
        logger.info(
            "pipeline_user_question",
            extra={"event": "pipeline_user_question", "session_id": session.session_id, "question": request.question[:120]},
        )

        # Start event — new sessions carry their session_id here
        start: dict = {"type": "assistant_message_start", "content": ""}
        if not request.session_id:
            start["session_id"] = session.session_id
        yield _sse(start)

        # title_task is created inside tracing_context so it appears as a child trace
        title_task = None
        try:
            with tracing_context(
                project_name=settings.get_langsmith_project(),
                metadata={
                    "session_id": session.session_id,
                    "user_id": str(user.id),
                    "app_env": settings.app_env,
                },
                tags=[settings.app_env],
            ):
                # Generate title in parallel with the main LLM stream — by the time
                # the answer finishes, the title is usually already done.
                title_task = asyncio.create_task(generate_title(request.question))
                logger.info(
                    "pipeline_llm_start",
                    extra={"event": "pipeline_llm_start", "session_id": session.session_id},
                )
                # Build history: last 3 QA pairs (up to 6 messages) from MongoDB
                history = [
                    {"role": msg.role, "content": msg.content}
                    for msg in session.messages[-6:]
                ]
                input_messages = history + [{"role": "user", "content": request.question}]

                async for chunk in agent.astream(
                    {"messages": input_messages},
                    stream_mode="messages",
                    config={"configurable": {"thread_id": session.session_id}},
                ):
                    token = _extract_token(chunk)
                    if not token:
                        continue
                    assembled.append(token)
                    tail = (tail + token)[-120:]  # keep last 120 chars to detect REFS line
                    # Suppress tokens once the REFS line has started — it's metadata, not content
                    if "\nREFS:" not in tail:
                        yield _sse({"type": "content", "content": token})

                full_content = "".join(assembled)

                # Extract REFS line written by the LLM (e.g. "REFS:[1,3]") and strip it from content.
                # refs_found=True means the LLM wrote REFS (even if empty). refs_found=False means no line
                # at all — we fall back to showing all docs so citations are never silently lost.
                refs_match = re.search(r'\nREFS:\[([^\]]*)\]\s*$', full_content)
                referenced: set[int] = set()
                refs_found: bool = refs_match is not None
                if refs_match:
                    raw = refs_match.group(1).strip()
                    if raw:
                        referenced = {int(x.strip()) for x in raw.split(",") if x.strip().isdigit()}
                    full_content = full_content[:refs_match.start()].rstrip()

                logger.info(
                    "pipeline_llm_done",
                    extra={
                        "event": "pipeline_llm_done",
                        "session_id": session.session_id,
                        "response_chars": len(full_content),
                        "refs": sorted(referenced),
                    },
                )

                # Use docs already retrieved during the tool call — no second retrieval needed.
                logger.info(
                    "pipeline_citations_start",
                    extra={"event": "pipeline_citations_start", "session_id": session.session_id},
                )
                citation_docs = get_last_docs(session.session_id)
                citations = [
                    Citation(
                        resource_id=str(d.metadata.get("_id", "")),
                        text_span=(d.page_content or "")[:200],
                        confidence=float(d.metadata.get("relevance_score", 0.0)),
                        title=d.metadata.get("title", ""),
                        hadith_url=d.metadata.get("hadith_url", ""),
                    )
                    for i, d in enumerate(citation_docs, start=1)
                    # Filter to only cited hadiths. Two fallback cases:
                    # - no REFS line at all (refs_found=False): show all docs (graceful degradation)
                    # - REFS:[] (refs_found=True, referenced={}): show nothing (LLM cited none)
                    if not refs_found or i in referenced
                ]
                logger.info(
                    "pipeline_citations_done",
                    extra={"event": "pipeline_citations_done", "session_id": session.session_id, "citations": len(citations)},
                )

                yield _sse({
                    "type": "assistant_message_complete",
                    "data": {
                        "message_type": "assistant",
                        "content": full_content,
                        "citations": [c.model_dump() for c in citations],
                    },
                })

                try:
                    title = await title_task
                except asyncio.CancelledError:
                    title = request.question[:50]
                logger.info(
                    "pipeline_done",
                    extra={"event": "pipeline_done", "session_id": session.session_id, "title": title},
                )
                yield _sse({"type": "thread_rename", "title": title})
                yield _sse({"type": "stream_end"})

        except Exception as e:
            logger.error(
                "chat_stream_error",
                extra={"event": "chat_stream_error", "session_id": session.session_id, "error": str(e)},
            )
            if title_task is not None:
                title_task.cancel()
            yield _sse({"type": "error", "content": "حدث خطأ أثناء المعالجة."})
            yield _sse({"type": "stream_end"})
            return

        # Persist conversation turn and title to Mongo after stream is fully sent
        await append_turn(db, session, request.question, full_content, citations)
        await update_session_title(db, session.session_id, title)
        await increment_quota(str(user.id), getattr(user, "tier", "free"))

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.get("/chat/quota", response_model=QuotaStatus)
async def get_quota(user: User = Depends(current_active_user)):
    """Return the authenticated user's quota usage for today."""
    tier = getattr(user, "tier", "free")
    limit = settings.get_daily_limit(tier)

    today = date.today()
    quotas = get_user_quotas_collection(get_db(get_client()))
    doc = await quotas.find_one({"user_id": str(user.id), "usage_date": today.isoformat()})
    used = int(doc["request_count"]) if doc else 0

    remaining = -1 if limit == -1 else max(0, limit - used)
    resets_at = datetime.combine(today + timedelta(days=1), time.min).replace(tzinfo=tz.utc)

    return QuotaStatus(tier=tier, limit=limit, used=used, remaining=remaining, resets_at=resets_at)


@router.get("/chat/sessions", response_model=list[ChatSessionMeta])
async def list_sessions(
    skip: int = 0,
    limit: int = 20,
    user: User = Depends(current_active_user),
):
    """List the authenticated user's chat sessions (metadata only, no messages)."""
    db = get_db(get_client())
    col = "chat_sessions_dev" if settings.is_dev else "chat_sessions_prod"
    cursor = (
        db[col]
        .find({"user_id": str(user.id)})
        .sort("created_at", -1)
        .skip(skip)
        .limit(limit)
    )
    results = []
    async for doc in cursor:
        doc.pop("_id", None)
        results.append(
            ChatSessionMeta(
                session_id=doc["session_id"],
                title=doc.get("title", ""),
                created_at=doc["created_at"],
                message_count=len(doc.get("messages", [])),
            )
        )
    return results


@router.get("/chat/sessions/{session_id}", response_model=ChatSession)
async def get_session(
    session_id: str,
    user: User = Depends(current_active_user),
):
    """Get a full chat session (with messages) — 403 if not owned by user."""
    from fastapi import HTTPException
    db = get_db(get_client())
    col = "chat_sessions_dev" if settings.is_dev else "chat_sessions_prod"
    doc = await db[col].find_one({"session_id": session_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Session not found.")
    doc.pop("_id", None)
    session = ChatSession(**doc)
    if session.user_id != str(user.id):
        raise HTTPException(status_code=403, detail="Session belongs to another user.")
    return session


@router.delete("/chat/sessions/{session_id}", status_code=204)
async def delete_session(
    session_id: str,
    user: User = Depends(current_active_user),
):
    """Delete a chat session — 403 if not owned by user, 404 if not found."""
    from fastapi import HTTPException
    db = get_db(get_client())
    col = "chat_sessions_dev" if settings.is_dev else "chat_sessions_prod"
    doc = await db[col].find_one({"session_id": session_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Session not found.")
    doc.pop("_id", None)
    session = ChatSession(**doc)
    if session.user_id != str(user.id):
        raise HTTPException(status_code=403, detail="Session belongs to another user.")
    await db[col].delete_one({"session_id": session_id})
