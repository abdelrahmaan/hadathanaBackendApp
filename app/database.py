import logging

from motor.motor_asyncio import AsyncIOMotorClient

from .config import settings

logger = logging.getLogger("hadathana.database")

_client: AsyncIOMotorClient | None = None
_db_status: dict | None = None

# Minimum expected document counts per collection.
# If a collection has fewer docs than this, startup validation logs an ERROR.
EXPECTED_COLLECTIONS = {
    "processed_podia_books": 7000,
    "processed_podia_narrators": 1700,
    "processed_podia_narrator_biographies": 1700,
    "raw_shamela_books": 7000,
    "raw_shamela_narrators": 1500,
    "analytics_narrator_stats_podia": 100,
    "analytics_narrator_stats_shamela": 100,
    "chat_sessions_dev": 0,   # optional — empty at startup
    "chat_sessions_prod": 0,  # optional — empty at startup
}


def get_client() -> AsyncIOMotorClient:
    return _client


def get_db(client: AsyncIOMotorClient):
    return client[settings.get_db_name()]


def get_hadiths_collection(db):
    return db["raw_shamela_books"]


def get_narrators_collection(db):
    return db["raw_shamela_narrators"]


def get_hadith_pages_collection(db):
    return db["raw_shamela_hadith_pages"]


def get_narrator_stats_collection(db):
    return db["analytics_narrator_stats_shamela"]


def get_podia_hadiths_collection(db):
    return db["processed_podia_books"]


def get_podia_narrators_collection(db):
    return db["processed_podia_narrators"]


def get_podia_narrator_stats_collection(db):
    return db["analytics_narrator_stats_podia"]


def get_podia_raw_hadiths_collection(db):
    return db["raw_podia_books"]


def get_podia_narrators_tarajem_collection(db):
    return db["processed_podia_narrator_biographies"]


def get_chat_sessions_collection(db):
    """Returns the env-appropriate chat sessions collection (dev or prod)."""
    name = "chat_sessions_dev" if settings.is_dev else "chat_sessions_prod"
    return db[name]


def get_auth_users_collection(db):
    return db["auth_users"]


def get_bookmarks_collection(db):
    return db["user_bookmarks"]


async def connect():
    global _client
    _client = AsyncIOMotorClient(settings.get_mongodb_uri())


async def validate_connection() -> dict:
    """Ping MongoDB and verify expected collections have data.

    Stores result in module-level _db_status for use by the health endpoint.
    Logs ERROR for missing/empty collections but does NOT crash the app.
    """
    global _db_status
    status: dict = {"connected": False, "collections": {}, "empty_collections": []}

    try:
        await _client.admin.command("ping")
        status["connected"] = True
    except Exception as exc:
        logger.error("MongoDB ping failed: %s", exc)
        _db_status = status
        return status

    db = get_db(_client)
    existing = set(await db.list_collection_names())

    for col_name, min_count in EXPECTED_COLLECTIONS.items():
        if col_name not in existing:
            status["collections"][col_name] = 0
            status["empty_collections"].append(col_name)
            logger.error("Collection '%s' MISSING in database '%s'", col_name, settings.get_db_name())
            continue

        count = await db[col_name].estimated_document_count()
        status["collections"][col_name] = count
        if count < min_count:
            status["empty_collections"].append(col_name)
            logger.error(
                "Collection '%s' has %d docs (expected >= %d)",
                col_name, count, min_count,
            )

    if status["empty_collections"]:
        logger.error(
            "DATABASE IS UNHEALTHY — %d collection(s) missing or below threshold: %s",
            len(status["empty_collections"]),
            ", ".join(status["empty_collections"]),
        )
    else:
        logger.info("All %d collections verified OK", len(EXPECTED_COLLECTIONS))

    _db_status = status
    return status


def get_db_status() -> dict | None:
    return _db_status


async def disconnect():
    global _client
    if _client:
        _client.close()
        _client = None
