from motor.motor_asyncio import AsyncIOMotorClient
from .config import settings

_client: AsyncIOMotorClient | None = None


def get_client() -> AsyncIOMotorClient:
    return _client


def get_db(client: AsyncIOMotorClient):
    return client[settings.db_name]


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


async def connect():
    global _client
    _client = AsyncIOMotorClient(settings.mongodb_uri_read)


async def disconnect():
    global _client
    if _client:
        _client.close()
        _client = None
