import json
import logging
import re

from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from langchain.chat_models import init_chat_model

from app.chatbot.agent import get_agent, get_last_docs
from app.chatbot.models import ChatRequest, Citation
from app.chatbot.prompts import THREAD_RENAME_PROMPT
from app.chatbot.session import append_turn, get_or_create_session
from app.config import settings
from app.database import get_client, get_db

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


async def _generate_title(question: str) -> str:
    """Short non-streaming call to generate a thread title."""
    try:
        model = init_chat_model(
            settings.chatbot_model,
            model_provider="openai",
            base_url="https://openrouter.ai/api/v1",
            api_key=settings.openrouter_api_key,
        )
        result = await model.ainvoke(THREAD_RENAME_PROMPT.format(question=question))
        return (result.content or "").strip()[:60]
    except Exception as e:
        logger.warning("title_generation_failed", extra={"event": "title_generation_failed", "error": str(e)})
        return question[:50]


@router.post("/chat")
async def chat(request: ChatRequest):
    db = get_db(get_client())
    session = await get_or_create_session(db, request.session_id)
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

        try:
            logger.info(
                "pipeline_llm_start",
                extra={"event": "pipeline_llm_start", "session_id": session.session_id},
            )
            async for chunk in agent.astream(
                {"messages": [{"role": "user", "content": request.question}]},
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
        except Exception as e:
            logger.error(
                "chat_stream_error",
                extra={"event": "chat_stream_error", "session_id": session.session_id, "error": str(e)},
            )
            yield _sse({"type": "error", "content": "حدث خطأ أثناء المعالجة."})
            yield _sse({"type": "stream_end"})
            return

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

        title = await _generate_title(request.question)
        logger.info(
            "pipeline_done",
            extra={"event": "pipeline_done", "session_id": session.session_id, "title": title},
        )
        yield _sse({"type": "thread_rename", "title": title})
        yield _sse({"type": "stream_end"})

        # Persist conversation turn to Mongo after stream is fully sent
        await append_turn(db, session, request.question, full_content, citations)

    return StreamingResponse(event_stream(), media_type="text/event-stream")
