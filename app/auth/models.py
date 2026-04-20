import uuid

from fastapi_users import schemas


class User(schemas.BaseUser[uuid.UUID]):
    """Internal DB user model — includes hashed_password (required by UserManager)."""

    hashed_password: str
    tier: str = "free"


class UserRead(schemas.BaseUser[uuid.UUID]):
    """Public user schema returned by API endpoints."""

    tier: str = "free"


class UserCreate(schemas.BaseUserCreate):
    pass


class UserUpdate(schemas.BaseUserUpdate):
    pass
