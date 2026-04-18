"""Custom Motor (async MongoDB) adapter for fastapi-users v15.

fastapi-users v15 removed the bundled MongoDBUserDatabase adapter.
This module provides MotorUserDatabase, a custom BaseUserDatabase subclass
that stores users in a Motor (AsyncIOMotorCollection) collection.
"""

import uuid
from typing import Any, Optional

from fastapi_users.db import BaseUserDatabase

from .models import User


class MotorUserDatabase(BaseUserDatabase[User, uuid.UUID]):
    """Motor (async MongoDB) adapter for fastapi-users v15."""

    def __init__(self, collection) -> None:
        self.collection = collection

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _doc_to_user(self, doc: dict[str, Any]) -> User:
        """Convert a raw MongoDB document to a User model instance."""
        # Strip the internal _id field — not part of the User schema.
        clean = {k: v for k, v in doc.items() if k != "_id"}
        return User(**clean)

    # ------------------------------------------------------------------
    # BaseUserDatabase interface
    # ------------------------------------------------------------------

    async def get(self, id: uuid.UUID) -> Optional[User]:
        doc = await self.collection.find_one({"id": str(id)})
        return self._doc_to_user(doc) if doc else None

    async def get_by_email(self, email: str) -> Optional[User]:
        doc = await self.collection.find_one({"email": email.lower()})
        return self._doc_to_user(doc) if doc else None

    async def create(self, create_dict: dict[str, Any]) -> User:
        # Assign a new UUID id (stored as string for easy querying).
        create_dict["id"] = str(create_dict.get("id", uuid.uuid4()))
        # Normalise email to lower-case before storing.
        if "email" in create_dict:
            create_dict["email"] = create_dict["email"].lower()
        await self.collection.insert_one(create_dict)
        return self._doc_to_user(create_dict)

    async def update(self, user: User, update_dict: dict[str, Any]) -> User:
        if "email" in update_dict:
            update_dict["email"] = update_dict["email"].lower()
        await self.collection.update_one(
            {"id": str(user.id)}, {"$set": update_dict}
        )
        updated = await self.collection.find_one({"id": str(user.id)})
        return self._doc_to_user(updated)

    async def delete(self, user: User) -> None:
        await self.collection.delete_one({"id": str(user.id)})


async def get_user_db():
    """FastAPI dependency that yields a MotorUserDatabase instance."""
    from ..database import get_auth_users_collection, get_client, get_db

    client = get_client()
    db = get_db(client)
    collection = get_auth_users_collection(db)
    yield MotorUserDatabase(collection)
