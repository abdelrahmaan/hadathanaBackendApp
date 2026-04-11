import logging

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool
from langgraph.checkpoint.memory import InMemorySaver

from app.chatbot.config import RELEVANCE_SCORE_THRESHOLD
from app.chatbot.prompts import ARABIC_SYSTEM_PROMPT
from app.chatbot.qdrant import get_qdrant_client
from app.chatbot.retriever import build_hadiths_retriever
from app.config import settings

logger = logging.getLogger("hadathana.chatbot.agent")

_agent = None       # CompiledStateGraph at runtime
_retriever = None
_checkpointer = InMemorySaver()   # module-level, persists across requests per thread_id

# Stash last retrieved docs per thread_id to avoid double retrieval for citations
_last_docs: dict[str, list] = {}


def get_agent():
    return _agent


def get_retriever():
    return _retriever


def get_last_docs(thread_id: str) -> list:
    """Pop and return docs stashed by the last search_hadiths call for this thread."""
    return _last_docs.pop(thread_id, [])


def build_agent() -> None:
    global _agent, _retriever

    _retriever = build_hadiths_retriever(get_qdrant_client())

    @tool
    def search_hadiths(query: str, config: RunnableConfig) -> str:
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
        docs = _retriever.invoke(query)
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
        system_prompt=ARABIC_SYSTEM_PROMPT,
        checkpointer=_checkpointer,
    )

    logger.info("agent_built", extra={"event": "agent_built", "model": settings.chatbot_model})
