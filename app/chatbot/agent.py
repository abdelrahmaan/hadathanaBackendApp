import logging

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

from app.chatbot.config import RELEVANCE_SCORE_THRESHOLD
from app.chatbot.prompts import ARABIC_SYSTEM_PROMPT, THREAD_RENAME_PROMPT
from app.chatbot.qdrant import get_qdrant_client
from app.chatbot.retriever import build_hadiths_retriever
from app.config import settings

logger = logging.getLogger("hadathana.chatbot.agent")

_agent = None       # CompiledStateGraph at runtime — main orchestrator
_retriever = None
_title_model = None  # plain chat model (not an agent) for short utility LLM calls

# Stash last retrieved docs per thread_id to avoid double retrieval for citations
_last_docs: dict[str, list] = {}


def get_agent():
    return _agent


def get_retriever():
    return _retriever


def get_last_docs(thread_id: str) -> list:
    """Pop and return docs stashed by the last search_hadiths call for this thread."""
    return _last_docs.pop(thread_id, [])


async def generate_title(question: str) -> str:
    """Generate a short Arabic title for a chat thread from the user's first question.

    Uses a module-level plain chat model singleton built in build_agent() — kept
    separate from the main agent (one create_agent per orchestrator only).
    Safe to call as a background task — failures fall back to the truncated question.
    """
    if _title_model is None:
        return question[:50]
    try:
        result = await _title_model.ainvoke(THREAD_RENAME_PROMPT.format(question=question))
        return result.content
    except Exception as e:
        logger.warning(
            "title_generation_failed",
            extra={"event": "title_generation_failed", "error": str(e)},
        )
        return question[:50]


def build_agent() -> None:
    global _agent, _retriever, _title_model

    _retriever = build_hadiths_retriever(get_qdrant_client())

    @tool
    async def search_hadiths(query: str, config: RunnableConfig) -> str:
        """
        Search Sahih al-Bukhari hadiths by meaning.
        Always call this tool before answering any Islamic question.
        Returns numbered passages with source information.
        """
        thread_id = (config.get("configurable") or {}).get("thread_id", "")
        logger.info(
            "tool_search_hadiths",
            extra={"event": "tool_search_hadiths", "query": query[:80]},
        )
        try:
            docs = await _retriever.ainvoke(query)
        except Exception as e:
            logger.error(
                "tool_retrieval_error",
                extra={"event": "tool_retrieval_error", "query": query[:80], "error": str(e)},
            )
            return "حدث خطأ أثناء البحث. يرجى المحاولة مرة أخرى."
        # Pre-filter: drop passages below relevance threshold
        pre_filter_count = len(docs)
        docs = [d for d in docs if d.metadata.get("relevance_score", 0.0) >= RELEVANCE_SCORE_THRESHOLD]

        if thread_id:
            _last_docs[thread_id] = docs   # stash filtered list for citation extraction in router

        if not docs:
            logger.info("tool_no_results", extra={"event": "tool_no_results", "query": query[:80], "filtered_out": pre_filter_count})
            return "لم يُعثر على أحاديث ذات صلة."
        logger.info(
            "tool_results_ready",
            extra={"event": "tool_results_ready", "query": query[:80], "count": len(docs), "filtered_out": pre_filter_count - len(docs)},
        )
        # Format with Arabic-Indic numerals (١, ٢, ٣, ...) for display in LLM response
        _ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"

        def _to_arabic(n: int) -> str:
            return "".join(_ARABIC_DIGITS[int(c)] for c in str(n))

        return "\n\n".join(
            f"[{_to_arabic(i + 1)}] (relevance: {d.metadata.get('relevance_score', 0.0):.2f}) "
            f"{d.metadata.get('title', '') or d.metadata.get('book', '')}\n{d.page_content}"
            for i, d in enumerate(docs)
        )

    model = init_chat_model(
        settings.chatbot_model,
        model_provider="openai",
        base_url="https://openrouter.ai/api/v1",
        api_key=settings.openrouter_api_key,
        streaming=True,
    )

    _agent = create_agent(
        model,
        [search_hadiths],
        max_tokens=750,
        system_prompt=ARABIC_SYSTEM_PROMPT,
    )

    # Plain chat model for short utility calls (titles, etc.) — not an agent.
    _title_model = init_chat_model(
        settings.chatbot_model,
        model_provider="openai",
        base_url="https://openrouter.ai/api/v1",
        api_key=settings.openrouter_api_key,
    )

    logger.info("agent_built", extra={"event": "agent_built", "model": settings.chatbot_model})
