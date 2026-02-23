from motor.motor_asyncio import AsyncIOMotorClient
from .config import settings

client: AsyncIOMotorClient = None


def get_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(settings.mongodb_uri)


def get_db(client: AsyncIOMotorClient):
    return client[settings.db_name]


def get_hadiths_collection(db):
    return db["bukhari_book"]


def get_narrators_collection(db):
    return db["narrators"]


def get_hadith_pages_collection(db):
    return db["hadith_pages"]
