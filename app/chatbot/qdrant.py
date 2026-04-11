import logging

from qdrant_client import QdrantClient

from app.config import settings

logger = logging.getLogger("hadathana.chatbot.qdrant")

_qdrant_client: QdrantClient | None = None


def get_qdrant_client() -> QdrantClient:
    return _qdrant_client


async def connect_qdrant() -> None:
    global _qdrant_client
    _qdrant_client = QdrantClient(url=settings.qdrant_url)
    logger.info("qdrant_connected", extra={"event": "qdrant_connected", "url": settings.qdrant_url})


async def disconnect_qdrant() -> None:
    global _qdrant_client
    if _qdrant_client:
        _qdrant_client.close()
        _qdrant_client = None
    logger.info("qdrant_disconnected", extra={"event": "qdrant_disconnected"})
