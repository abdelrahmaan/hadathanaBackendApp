from motor.motor_asyncio import AsyncIOMotorClient
from .config import settings

_client: AsyncIOMotorClient | None = None


def get_client() -> AsyncIOMotorClient:
    return _client


def get_db(client: AsyncIOMotorClient):
    return client[settings.db_name]


def get_hadiths_collection(db):
    return db["bukhari_book"]


def get_narrators_collection(db):
    return db["narrators"]


def get_hadith_pages_collection(db):
    return db["hadith_pages"]


def get_narrator_stats_collection(db):
    return db["narrator_stats"]


async def connect():
    global _client
    _client = AsyncIOMotorClient(settings.mongodb_uri)


async def disconnect():
    global _client
    if _client:
        _client.close()
        _client = None
