import logging

from langchain_classic.retrievers import ContextualCompressionRetriever
from langchain_cohere import CohereEmbeddings, CohereRerank
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from langchain_qdrant import FastEmbedSparse, QdrantVectorStore, RetrievalMode
from qdrant_client import QdrantClient

from app.chatbot.config import (
    EMBED_MODEL,
    FETCH_K,
    QDRANT_COLLECTION,
    RERANK_MODEL,
    RERANK_TOP_N,
)
from app.config import settings

logger = logging.getLogger("hadathana.chatbot.retriever")


class _LoggingRetriever(BaseRetriever):
    """Wraps the base Qdrant retriever to log candidate count before reranking."""

    base: BaseRetriever

    def _get_relevant_documents(self, query: str) -> list[Document]:
        docs = self.base.invoke(query)
        logger.info(
            "retrieval_candidates",
            extra={"event": "retrieval_candidates", "query": query[:80], "count": len(docs)},
        )
        return docs

    async def _aget_relevant_documents(self, query: str) -> list[Document]:
        docs = await self.base.ainvoke(query)
        logger.info(
            "retrieval_candidates",
            extra={"event": "retrieval_candidates", "query": query[:80], "count": len(docs)},
        )
        return docs


class _LoggingReranker(CohereRerank):
    """Wraps CohereRerank to log scores after reranking."""

    def compress_documents(self, documents, query, callbacks=None):
        result = super().compress_documents(documents, query, callbacks)
        logger.info(
            "reranking_done",
            extra={
                "event": "reranking_done",
                "query": query[:80],
                "returned": len(result),
                "scores": [round(d.metadata.get("relevance_score", 0), 4) for d in result],
                "urls": [d.metadata.get("hadith_url", "")[-40:] for d in result],
            },
        )
        return result

    async def acompress_documents(self, documents, query, callbacks=None):
        result = await super().acompress_documents(documents, query, callbacks)
        logger.info(
            "reranking_done",
            extra={
                "event": "reranking_done",
                "query": query[:80],
                "returned": len(result),
                "scores": [round(d.metadata.get("relevance_score", 0), 4) for d in result],
                "urls": [d.metadata.get("hadith_url", "")[-40:] for d in result],
            },
        )
        return result


def build_hadiths_retriever(qdrant_client: QdrantClient, k_fetch: int = FETCH_K) -> BaseRetriever:
    """
    Hybrid retriever: dense (Cohere embed-v4.0) + BM25 sparse,
    followed by Cohere multilingual reranker.
    """
    embeddings = CohereEmbeddings(
        model=EMBED_MODEL,
        cohere_api_key=settings.cohere_api_key,
    )
    sparse_embeddings = FastEmbedSparse(model_name="Qdrant/bm25")

    vector_store = QdrantVectorStore(
        client=qdrant_client,
        collection_name=QDRANT_COLLECTION,
        embedding=embeddings,
        sparse_embedding=sparse_embeddings,
        retrieval_mode=RetrievalMode.HYBRID,
        vector_name="dense",
        sparse_vector_name="text-sparse",
        content_payload_key="matn_text_plain",
    )
    base_retriever = _LoggingRetriever(base=vector_store.as_retriever(search_kwargs={"k": k_fetch}))

    reranker = _LoggingReranker(
        model=RERANK_MODEL,
        cohere_api_key=settings.cohere_api_key,
        top_n=RERANK_TOP_N,
    )

    return ContextualCompressionRetriever(
        base_compressor=reranker,
        base_retriever=base_retriever,
    )
