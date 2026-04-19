# Auth & Bookmarks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add sign up, sign in, forgot/reset password (via Resend email), and hadith bookmarking to the Hadathana FastAPI backend using FastAPI Users + Motor adapter + HttpOnly cookies.

**Architecture:** `fastapi-users[motor]` with `MongoDBUserDatabase` (raw Motor, no Beanie ODM) provides the auth framework; a custom `bookmarks` router follows the existing Motor/dict pattern. Rate limiting via `slowapi` on auth endpoints. All tokens delivered as HttpOnly cookies — JS never sees them.

**Tech Stack:** `fastapi-users[motor]`, `slowapi`, `resend`, `python-jose[cryptography]`, `passlib[argon2]`, `pytest`, `pytest-asyncio`, `httpx`

> **Rate limiting note:** `slowapi` is attached at the app level (`app.state.limiter`). FastAPI Users generates its own routes internally, so per-route `@limiter.limit(...)` decorators cannot be applied to them. The global limiter middleware applies to all routes; for stricter per-endpoint limits on auth routes, a custom middleware or `slowapi`'s `default_limits` setting can be used post-MVP.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `app/auth/__init__.py` | Create | Package marker |
| `app/auth/models.py` | Create | `User`, `UserCreate`, `UserRead`, `UserUpdate` Pydantic models |
| `app/auth/database.py` | Create | `get_user_db()` — Motor `MongoDBUserDatabase` adapter |
| `app/auth/config.py` | Create | `UserManager`, cookie transport, JWT strategy, Resend email hook |
| `app/routers/bookmarks.py` | Create | `GET/POST/DELETE /api/v1/bookmarks` — authenticated |
| `app/database.py` | Modify | Add `get_auth_users_collection()`, `get_bookmarks_collection()` |
| `app/config.py` | Modify | Add `jwt_secret`, `access_token_expire_minutes`, `refresh_token_expire_days`, `reset_token_expire_minutes`, `resend_api_key`, `from_email` |
| `app/main.py` | Modify | Mount auth + bookmark routers, update CORS to allow POST/DELETE/OPTIONS, add `slowapi` limiter |
| `mongo_migration/create_indexes.py` | Modify | Add auth + bookmark collection indexes |
| `.env.example` | Modify | Document new env vars |
| `requirements.txt` | Modify | Add `fastapi-users[motor]`, `slowapi`, `resend` |
| `tests/conftest.py` | Modify | Patch new db functions + auth dependencies |
| `tests/test_auth_register.py` | Create | Register: success, duplicate email, weak password |
| `tests/test_auth_login.py` | Create | Login: success + cookies, wrong password, unknown email |
| `tests/test_auth_forgot_password.py` | Create | Forgot: always 200, Resend called/not called |
| `tests/test_auth_reset_password.py` | Create | Reset: valid token, expired, already used |
| `tests/test_bookmarks.py` | Create | Bookmarks CRUD: success, 401, 409 |

---

## Task 1: Install dependencies

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add new packages to requirements.txt**

Open `requirements.txt` and append after the `# Testing` block:

```
# Auth
fastapi-users[motor]>=13.0.0
slowapi>=0.1.9
resend>=2.0.0
```

- [ ] **Step 2: Install into the project venv**

```bash
/home/abdo_kamar/Projects/.venv/bin/pip install "fastapi-users[motor]>=13.0.0" "slowapi>=0.1.9" "resend>=2.0.0"
```

Expected: packages install without errors. Verify:
```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "import fastapi_users, slowapi, resend; print('OK')"
```
Expected output: `OK`

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: add fastapi-users, slowapi, resend dependencies"
```

---

## Task 2: Extend config and env vars

**Files:**
- Modify: `app/config.py`
- Modify: `.env.example`

- [ ] **Step 1: Add auth fields to Settings**

In `app/config.py`, add the following fields to the `Settings` class (after `cors_origins_dev`):

```python
# Auth
jwt_secret: str = "changeme-generate-with-openssl-rand-hex-32"
access_token_expire_minutes: int = 15
refresh_token_expire_days: int = 30
reset_token_expire_minutes: int = 30

# Email (Resend)
resend_api_key: str = ""
from_email: str = "noreply@hadathana.app"
```

The full updated `Settings` class in `app/config.py` should look like:

```python
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Environment: "dev" or "prod"
    app_env: str = "prod"

    # MongoDB — cloud (prod, currently unused — Atlas unreachable since 2026-04-01)
    mongodb_uri_read: str = ""
    db_name: str = "HadithData"

    # MongoDB — local (dev)
    mongodb_uri_local: str = "mongodb://localhost:27017/"
    db_name_dev: str = "HadithDataDev"

    # Server
    port: int = 8000
    port_dev: int = 8001

    # CORS
    cors_origins: str = "http://localhost:3000"
    cors_origins_dev: str = "http://localhost:3000,http://localhost:5173"

    # Auth
    jwt_secret: str = "changeme-generate-with-openssl-rand-hex-32"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 30
    reset_token_expire_minutes: int = 30

    # Email (Resend)
    resend_api_key: str = ""
    from_email: str = "noreply@hadathana.app"

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def is_dev(self) -> bool:
        return self.app_env == "dev"

    def get_mongodb_uri(self) -> str:
        return self.mongodb_uri_local

    def get_db_name(self) -> str:
        return self.db_name_dev if self.is_dev else self.db_name

    def get_port(self) -> int:
        return self.port_dev if self.is_dev else self.port

    def get_cors_origins(self) -> list[str]:
        raw = self.cors_origins_dev if self.is_dev else self.cors_origins
        return [o.strip() for o in raw.split(",") if o.strip()]


settings = Settings()
```

- [ ] **Step 2: Add env vars to .env.example**

Append to `.env.example`:

```
# ── Auth (JWT + session) ──────────────────────────────────────────────────────
# Generate with: openssl rand -hex 32
JWT_SECRET=changeme-generate-with-openssl-rand-hex-32
ACCESS_TOKEN_EXPIRE_MINUTES=15
REFRESH_TOKEN_EXPIRE_DAYS=30
RESET_TOKEN_EXPIRE_MINUTES=30

# ── Email (Resend) ────────────────────────────────────────────────────────────
RESEND_API_KEY=re_xxxxxxxxxxxxxxxxxxxx
FROM_EMAIL=noreply@hadathana.app
```

- [ ] **Step 3: Commit**

```bash
git add app/config.py .env.example
git commit -m "feat: add auth and email env var settings"
```

---

## Task 3: Add auth collection getters to database.py

**Files:**
- Modify: `app/database.py`

- [ ] **Step 1: Add two collection getter functions**

In `app/database.py`, add the following two functions after `get_podia_narrators_tarajem_collection`:

```python
def get_auth_users_collection(db):
    return db["auth_users"]


def get_bookmarks_collection(db):
    return db["user_bookmarks"]
```

- [ ] **Step 2: Verify import works**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "from app.database import get_auth_users_collection, get_bookmarks_collection; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add app/database.py
git commit -m "feat: add auth_users and user_bookmarks collection getters"
```

---

## Task 4: Create auth models

**Files:**
- Create: `app/auth/__init__.py`
- Create: `app/auth/models.py`

- [ ] **Step 1: Create the auth package**

Create `app/auth/__init__.py` as an empty file.

- [ ] **Step 2: Create app/auth/models.py**

```python
from fastapi_users import schemas
from pydantic import EmailStr


class UserRead(schemas.BaseUser[str]):
    pass


class UserCreate(schemas.BaseUserCreate):
    pass


class UserUpdate(schemas.BaseUserUpdate):
    pass
```

`BaseUser`, `BaseUserCreate`, `BaseUserUpdate` come from `fastapi_users.schemas`. They already include `id`, `email`, `is_active`, `is_verified`, `is_superuser`. FastAPI Users stores the user `_id` as a UUID string by default — we pass `str` as the ID generic.

- [ ] **Step 3: Verify import**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "from app.auth.models import UserRead, UserCreate, UserUpdate; print('OK')"
```
Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add app/auth/__init__.py app/auth/models.py
git commit -m "feat: add FastAPI Users Pydantic models (UserRead, UserCreate, UserUpdate)"
```

---

## Task 5: Create auth database adapter

**Files:**
- Create: `app/auth/database.py`

This wires the Motor collection into the FastAPI Users `MongoDBUserDatabase` adapter.

- [ ] **Step 1: Create app/auth/database.py**

```python
from fastapi import Depends
from fastapi_users.db import MongoDBUserDatabase

from ..database import get_auth_users_collection, get_client, get_db


async def get_user_db():
    client = get_client()
    db = get_db(client)
    collection = get_auth_users_collection(db)
    yield MongoDBUserDatabase(collection)
```

- [ ] **Step 2: Verify import**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "from app.auth.database import get_user_db; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add app/auth/database.py
git commit -m "feat: add Motor MongoDBUserDatabase adapter for FastAPI Users"
```

---

## Task 6: Create UserManager, transports and auth backend

**Files:**
- Create: `app/auth/config.py`

This is the core FastAPI Users configuration: `UserManager` (handles registration/reset logic + sends email), cookie transport, JWT strategy, and the assembled `auth_backend`.

- [ ] **Step 1: Create app/auth/config.py**

```python
import resend
from fastapi import Depends, Request
from fastapi_users import BaseUserManager, FastAPIUsers, UUIDIDMixin
from fastapi_users.authentication import (
    AuthenticationBackend,
    CookieTransport,
    JWTStrategy,
)
from fastapi_users.db import MongoDBUserDatabase

from ..config import settings
from ..auth.database import get_user_db
from ..auth.models import UserCreate, UserRead, UserUpdate

SECRET = settings.jwt_secret


class UserManager(UUIDIDMixin, BaseUserManager):
    reset_password_token_secret = SECRET
    verification_token_secret = SECRET

    async def on_after_register(self, user, request: Request | None = None):
        resend.api_key = settings.resend_api_key
        resend.Emails.send({
            "from": settings.from_email,
            "to": user.email,
            "subject": "Welcome to Hadathana",
            "html": f"<p>Welcome! Your account has been created.</p>",
        })

    async def on_after_forgot_password(self, user, token: str, request: Request | None = None):
        reset_url = f"https://hadathana.app/reset-password?token={token}"
        resend.api_key = settings.resend_api_key
        resend.Emails.send({
            "from": settings.from_email,
            "to": user.email,
            "subject": "Reset your Hadathana password",
            "html": f"<p>Click to reset your password (expires in {settings.reset_token_expire_minutes} minutes):</p><p><a href='{reset_url}'>{reset_url}</a></p>",
        })


async def get_user_manager(user_db: MongoDBUserDatabase = Depends(get_user_db)):
    yield UserManager(user_db)


cookie_transport = CookieTransport(
    cookie_name="access_token",
    cookie_max_age=settings.access_token_expire_minutes * 60,
    cookie_secure=True,
    cookie_httponly=True,
    cookie_samesite="lax",
)


def get_jwt_strategy() -> JWTStrategy:
    return JWTStrategy(
        secret=SECRET,
        lifetime_seconds=settings.access_token_expire_minutes * 60,
    )


auth_backend = AuthenticationBackend(
    name="cookie",
    transport=cookie_transport,
    get_strategy=get_jwt_strategy,
)

fastapi_users = FastAPIUsers[UserRead, str](
    get_user_manager,
    [auth_backend],
)

current_active_user = fastapi_users.current_user(active=True)
```

- [ ] **Step 2: Verify import (no live DB needed)**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "from app.auth.config import auth_backend, fastapi_users, current_active_user; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add app/auth/config.py
git commit -m "feat: configure UserManager, cookie transport, JWT strategy, auth_backend"
```

---

## Task 7: Create bookmarks router

**Files:**
- Create: `app/routers/bookmarks.py`

Follows the exact same Motor/dict pattern as the existing routers.

- [ ] **Step 1: Create app/routers/bookmarks.py**

```python
import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from ..auth.config import current_active_user
from ..auth.models import UserRead
from ..database import get_bookmarks_collection, get_client, get_db

logger = logging.getLogger("hadathana.bookmarks")

router = APIRouter(prefix="/api/v1/bookmarks", tags=["bookmarks"])


class BookmarkCreate(BaseModel):
    hadith_url: str
    source: str  # "shamela" or "podia"


class BookmarkRead(BaseModel):
    hadith_url: str
    source: str
    created_at: datetime


class PaginatedBookmarks(BaseModel):
    items: list[BookmarkRead]
    total: int


@router.get("", response_model=PaginatedBookmarks)
async def list_bookmarks(
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    user: UserRead = Depends(current_active_user),
):
    db = get_db(get_client())
    col = get_bookmarks_collection(db)
    query = {"user_id": str(user.id)}
    total = await col.count_documents(query)
    cursor = col.find(query).skip(skip).limit(limit).sort("created_at", -1)
    items = []
    async for doc in cursor:
        items.append(BookmarkRead(
            hadith_url=doc["hadith_url"],
            source=doc["source"],
            created_at=doc["created_at"],
        ))
    return PaginatedBookmarks(items=items, total=total)


@router.post("", response_model=BookmarkRead, status_code=201)
async def add_bookmark(
    body: BookmarkCreate,
    user: UserRead = Depends(current_active_user),
):
    db = get_db(get_client())
    col = get_bookmarks_collection(db)
    existing = await col.find_one({"user_id": str(user.id), "hadith_url": body.hadith_url})
    if existing:
        raise HTTPException(status_code=409, detail="Bookmark already exists.")
    doc = {
        "user_id": str(user.id),
        "hadith_url": body.hadith_url,
        "source": body.source,
        "created_at": datetime.now(timezone.utc),
    }
    await col.insert_one(doc)
    return BookmarkRead(hadith_url=doc["hadith_url"], source=doc["source"], created_at=doc["created_at"])


@router.delete("/{hadith_url:path}", status_code=204)
async def remove_bookmark(
    hadith_url: str,
    user: UserRead = Depends(current_active_user),
):
    db = get_db(get_client())
    col = get_bookmarks_collection(db)
    result = await col.delete_one({"user_id": str(user.id), "hadith_url": hadith_url})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Bookmark not found.")
```

- [ ] **Step 2: Verify import**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "from app.routers.bookmarks import router; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add app/routers/bookmarks.py
git commit -m "feat: add authenticated bookmarks router (GET/POST/DELETE)"
```

---

## Task 8: Wire everything into main.py

**Files:**
- Modify: `app/main.py`

Mount auth routers, bookmarks router, update CORS methods, add `slowapi` rate limiter.

- [ ] **Step 1: Replace app/main.py with the wired version**

```python
import logging
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from .auth.config import auth_backend, fastapi_users
from .auth.models import UserCreate, UserRead, UserUpdate
from .config import settings
from .database import (
    connect,
    disconnect,
    get_client,
    get_db_status,
    validate_connection,
)
from .logging_config import setup_logging
from .middleware import RequestLoggingMiddleware
from .routers import (
    hadiths_podia,
    hadiths_shamela,
    narrators_podia,
    narrators_shamela,
    search_podia,
)
from .routers import bookmarks

setup_logging()

logger = logging.getLogger("hadathana.main")

limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("startup", extra={"event": "startup"})
    await connect()
    await validate_connection()
    yield
    await disconnect()
    logger.info("shutdown", extra={"event": "shutdown"})


app = FastAPI(title="hadathana-api", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# RequestLoggingMiddleware must be added first (executes outermost = sees final status)
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_cors_origins(),
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Auth routes (FastAPI Users)
app.include_router(
    fastapi_users.get_auth_router(auth_backend),
    prefix="/auth",
    tags=["auth"],
)
app.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["auth"],
)
app.include_router(
    fastapi_users.get_reset_password_router(),
    prefix="/auth",
    tags=["auth"],
)
app.include_router(
    fastapi_users.get_verify_router(UserRead),
    prefix="/auth",
    tags=["auth"],
)

# Data routes
app.include_router(hadiths_shamela.router)
app.include_router(narrators_shamela.router)
app.include_router(hadiths_podia.router)
app.include_router(narrators_podia.router)
app.include_router(search_podia.router)
app.include_router(bookmarks.router)

Instrumentator().instrument(app).expose(app)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error(
        "unhandled_exception",
        extra={
            "event": "error",
            "path": request.url.path,
            "method": request.method,
            "error": repr(exc),
            "traceback": traceback.format_exc(),
        },
    )
    return JSONResponse(status_code=500, content={"detail": "Internal server error."})


@app.get("/health", tags=["health"])
async def health():
    result = {"status": "ok", "mongodb": "unknown"}

    client = get_client()
    try:
        await client.admin.command("ping")
        result["mongodb"] = "connected"
    except Exception:
        result["mongodb"] = "disconnected"
        result["status"] = "degraded"

    db_status = get_db_status()
    if db_status:
        result["collections"] = db_status.get("collections", {})
        if db_status.get("empty_collections"):
            result["status"] = "degraded"
            result["warning"] = f"Empty/missing collections: {', '.join(db_status['empty_collections'])}"

    return result
```

- [ ] **Step 2: Verify app imports cleanly**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -c "from app.main import app; print('OK')"
```
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add app/main.py
git commit -m "feat: mount auth and bookmark routers, add slowapi rate limiter, update CORS"
```

---

## Task 9: Add MongoDB indexes for auth + bookmarks

**Files:**
- Modify: `mongo_migration/create_indexes.py`

- [ ] **Step 1: Add auth and bookmark collections to the INDEXES dict**

In `mongo_migration/create_indexes.py`, add to the `INDEXES` dict after the `"processed_podia_narrator_biographies"` entry:

```python
    "auth_users": [
        ([("email", ASCENDING)], {"unique": True}),
    ],
    "auth_password_reset_tokens": [
        ([("token_hash", ASCENDING)], {}),
        ([("expires_at", ASCENDING)], {"expireAfterSeconds": 0}),  # TTL index
    ],
    "auth_refresh_sessions": [
        ([("token_hash", ASCENDING)], {}),
        ([("expires_at", ASCENDING)], {"expireAfterSeconds": 0}),  # TTL index
    ],
    "user_bookmarks": [
        ([("user_id", ASCENDING), ("hadith_url", ASCENDING)], {"unique": True}),
        ([("user_id", ASCENDING), ("created_at", ASCENDING)], {}),
    ],
```

- [ ] **Step 2: Commit**

```bash
git add mongo_migration/create_indexes.py
git commit -m "feat: add MongoDB indexes for auth_users, reset tokens, refresh sessions, bookmarks"
```

---

## Task 10: Write tests for register endpoint

**Files:**
- Modify: `tests/conftest.py`
- Create: `tests/test_auth_register.py`

- [ ] **Step 1: Update conftest.py to patch auth db functions**

FastAPI Users needs `get_user_db` and `get_user_manager` patched. Replace the content of `tests/conftest.py` with:

```python
from unittest.mock import AsyncMock, MagicMock, patch

import pytest_asyncio
from httpx import ASGITransport, AsyncClient


def make_mock_collection():
    mock_collection = MagicMock()
    mock_collection.find.return_value.__aiter__ = AsyncMock(return_value=iter([]))
    mock_collection.count_documents = AsyncMock(return_value=0)
    mock_collection.find_one = AsyncMock(return_value=None)
    mock_collection.insert_one = AsyncMock(return_value=MagicMock(inserted_id="mock_id"))
    mock_collection.delete_one = AsyncMock(return_value=MagicMock(deleted_count=1))
    return mock_collection


@pytest_asyncio.fixture
async def client():
    """AsyncClient with mocked MongoDB so no live DB is needed."""
    mock_collection = make_mock_collection()
    mock_db = MagicMock()
    mock_client = MagicMock()

    mock_user_db = MagicMock()

    with patch("app.database.connect", new_callable=AsyncMock), \
         patch("app.database.disconnect", new_callable=AsyncMock), \
         patch("app.database.validate_connection", new_callable=AsyncMock), \
         patch("app.database.get_client", return_value=mock_client), \
         patch("app.database.get_db", return_value=mock_db), \
         patch("app.database.get_hadiths_collection", return_value=mock_collection), \
         patch("app.database.get_narrators_collection", return_value=mock_collection), \
         patch("app.database.get_podia_hadiths_collection", return_value=mock_collection), \
         patch("app.database.get_podia_narrators_collection", return_value=mock_collection), \
         patch("app.database.get_auth_users_collection", return_value=mock_collection), \
         patch("app.database.get_bookmarks_collection", return_value=mock_collection), \
         patch("app.auth.database.get_user_db", return_value=mock_user_db):
        from app.main import app
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c
```

- [ ] **Step 2: Run existing tests to confirm nothing is broken**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/test_logging.py tests/test_normalization.py -v
```
Expected: all pass.

- [ ] **Step 3: Write tests/test_auth_register.py**

```python
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_register_success():
    """POST /auth/register with valid data returns 201 and user JSON."""
    from fastapi_users.db import MongoDBUserDatabase

    mock_collection = MagicMock()
    mock_collection.find_one = AsyncMock(return_value=None)  # email not taken
    mock_collection.insert_one = AsyncMock(return_value=MagicMock(inserted_id="abc"))
    mock_user_db = MongoDBUserDatabase(mock_collection)

    with patch("app.database.connect", new_callable=AsyncMock), \
         patch("app.database.disconnect", new_callable=AsyncMock), \
         patch("app.database.validate_connection", new_callable=AsyncMock), \
         patch("app.database.get_client", return_value=MagicMock()), \
         patch("app.database.get_db", return_value=MagicMock()), \
         patch("app.database.get_auth_users_collection", return_value=mock_collection), \
         patch("app.database.get_bookmarks_collection", return_value=MagicMock()), \
         patch("app.database.get_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_narrators_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_narrators_collection", return_value=MagicMock()), \
         patch("app.auth.config.resend.Emails.send", return_value={"id": "mock"}):
        from app.main import app
        from app.auth.database import get_user_db

        async def override_get_user_db():
            yield mock_user_db

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/auth/register", json={
                "email": "test@example.com",
                "password": "Str0ngPassword!",
            })

        app.dependency_overrides.clear()

    assert resp.status_code == 201
    data = resp.json()
    assert data["email"] == "test@example.com"
    assert "hashed_password" not in data


@pytest.mark.asyncio
async def test_register_weak_password():
    """POST /auth/register with a too-short password returns 422."""
    from fastapi_users.db import MongoDBUserDatabase

    mock_collection = MagicMock()
    mock_collection.find_one = AsyncMock(return_value=None)
    mock_user_db = MongoDBUserDatabase(mock_collection)

    with patch("app.database.connect", new_callable=AsyncMock), \
         patch("app.database.disconnect", new_callable=AsyncMock), \
         patch("app.database.validate_connection", new_callable=AsyncMock), \
         patch("app.database.get_client", return_value=MagicMock()), \
         patch("app.database.get_db", return_value=MagicMock()), \
         patch("app.database.get_auth_users_collection", return_value=mock_collection), \
         patch("app.database.get_bookmarks_collection", return_value=MagicMock()), \
         patch("app.database.get_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_narrators_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_narrators_collection", return_value=MagicMock()):
        from app.main import app
        from app.auth.database import get_user_db

        async def override_get_user_db():
            yield mock_user_db

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/auth/register", json={
                "email": "test@example.com",
                "password": "123",
            })

        app.dependency_overrides.clear()

    assert resp.status_code == 422
```

- [ ] **Step 4: Run the new tests**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/test_auth_register.py -v
```
Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/conftest.py tests/test_auth_register.py
git commit -m "test: auth register — success and weak password cases"
```

---

## Task 11: Write tests for login endpoint

**Files:**
- Create: `tests/test_auth_login.py`

- [ ] **Step 1: Write tests/test_auth_login.py**

```python
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import ASGITransport, AsyncClient


def _build_patches():
    return [
        patch("app.database.connect", new_callable=AsyncMock),
        patch("app.database.disconnect", new_callable=AsyncMock),
        patch("app.database.validate_connection", new_callable=AsyncMock),
        patch("app.database.get_client", return_value=MagicMock()),
        patch("app.database.get_db", return_value=MagicMock()),
        patch("app.database.get_auth_users_collection", return_value=MagicMock()),
        patch("app.database.get_bookmarks_collection", return_value=MagicMock()),
        patch("app.database.get_hadiths_collection", return_value=MagicMock()),
        patch("app.database.get_narrators_collection", return_value=MagicMock()),
        patch("app.database.get_podia_hadiths_collection", return_value=MagicMock()),
        patch("app.database.get_podia_narrators_collection", return_value=MagicMock()),
    ]


@pytest.mark.asyncio
async def test_login_unknown_email_returns_400():
    """POST /auth/login with unknown email returns 400."""
    from fastapi_users.db import MongoDBUserDatabase

    mock_collection = MagicMock()
    mock_collection.find_one = AsyncMock(return_value=None)  # user not found
    mock_user_db = MongoDBUserDatabase(mock_collection)

    patches = _build_patches()
    for p in patches:
        p.start()

    try:
        from app.main import app
        from app.auth.database import get_user_db

        async def override_get_user_db():
            yield mock_user_db

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/auth/login", data={
                "username": "nobody@example.com",
                "password": "AnyPassword1!",
            })
    finally:
        app.dependency_overrides.clear()
        for p in patches:
            p.stop()

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_login_sets_cookie_on_success():
    """POST /auth/login with valid credentials sets access_token cookie."""
    import uuid
    from passlib.context import CryptContext
    from fastapi_users.db import MongoDBUserDatabase

    pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
    hashed = pwd_context.hash("Str0ngPassword!")

    user_doc = {
        "_id": str(uuid.uuid4()),
        "email": "user@example.com",
        "hashed_password": hashed,
        "is_active": True,
        "is_verified": True,
        "is_superuser": False,
    }

    mock_collection = MagicMock()
    mock_collection.find_one = AsyncMock(return_value=user_doc)
    mock_user_db = MongoDBUserDatabase(mock_collection)

    patches = _build_patches()
    for p in patches:
        p.start()

    try:
        from app.main import app
        from app.auth.database import get_user_db

        async def override_get_user_db():
            yield mock_user_db

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/auth/login", data={
                "username": "user@example.com",
                "password": "Str0ngPassword!",
            })
    finally:
        app.dependency_overrides.clear()
        for p in patches:
            p.stop()

    assert resp.status_code == 200
    assert "access_token" in resp.cookies
```

- [ ] **Step 2: Run the tests**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/test_auth_login.py -v
```
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_auth_login.py
git commit -m "test: auth login — unknown email 400, valid credentials sets cookie"
```

---

## Task 12: Write tests for forgot/reset password

**Files:**
- Create: `tests/test_auth_forgot_password.py`
- Create: `tests/test_auth_reset_password.py`

- [ ] **Step 1: Write tests/test_auth_forgot_password.py**

```python
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_forgot_password_always_returns_202():
    """POST /auth/forgot-password always returns 202 regardless of email existence."""
    mock_collection = MagicMock()
    mock_collection.find_one = AsyncMock(return_value=None)  # email does not exist
    from fastapi_users.db import MongoDBUserDatabase
    mock_user_db = MongoDBUserDatabase(mock_collection)

    with patch("app.database.connect", new_callable=AsyncMock), \
         patch("app.database.disconnect", new_callable=AsyncMock), \
         patch("app.database.validate_connection", new_callable=AsyncMock), \
         patch("app.database.get_client", return_value=MagicMock()), \
         patch("app.database.get_db", return_value=MagicMock()), \
         patch("app.database.get_auth_users_collection", return_value=mock_collection), \
         patch("app.database.get_bookmarks_collection", return_value=MagicMock()), \
         patch("app.database.get_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_narrators_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_narrators_collection", return_value=MagicMock()), \
         patch("app.auth.config.resend.Emails.send", return_value={"id": "mock"}) as mock_send:
        from app.main import app
        from app.auth.database import get_user_db

        async def override_get_user_db():
            yield mock_user_db

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/auth/forgot-password", json={"email": "nobody@example.com"})

        app.dependency_overrides.clear()

    # FastAPI Users returns 202 for forgot-password regardless
    assert resp.status_code == 202
    # Email must NOT be sent when user doesn't exist
    mock_send.assert_not_called()


@pytest.mark.asyncio
async def test_forgot_password_sends_email_when_user_exists():
    """POST /auth/forgot-password sends email when user exists, still returns 202."""
    import uuid
    from passlib.context import CryptContext
    from fastapi_users.db import MongoDBUserDatabase

    pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")
    user_doc = {
        "_id": str(uuid.uuid4()),
        "email": "user@example.com",
        "hashed_password": pwd_context.hash("Password1!"),
        "is_active": True,
        "is_verified": True,
        "is_superuser": False,
    }

    mock_collection = MagicMock()
    mock_collection.find_one = AsyncMock(return_value=user_doc)
    mock_user_db = MongoDBUserDatabase(mock_collection)

    with patch("app.database.connect", new_callable=AsyncMock), \
         patch("app.database.disconnect", new_callable=AsyncMock), \
         patch("app.database.validate_connection", new_callable=AsyncMock), \
         patch("app.database.get_client", return_value=MagicMock()), \
         patch("app.database.get_db", return_value=MagicMock()), \
         patch("app.database.get_auth_users_collection", return_value=mock_collection), \
         patch("app.database.get_bookmarks_collection", return_value=MagicMock()), \
         patch("app.database.get_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_narrators_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_narrators_collection", return_value=MagicMock()), \
         patch("app.auth.config.resend.Emails.send", return_value={"id": "mock"}) as mock_send:
        from app.main import app
        from app.auth.database import get_user_db

        async def override_get_user_db():
            yield mock_user_db

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/auth/forgot-password", json={"email": "user@example.com"})

        app.dependency_overrides.clear()

    assert resp.status_code == 202
    mock_send.assert_called_once()
    call_kwargs = mock_send.call_args[0][0]
    assert call_kwargs["to"] == "user@example.com"
    assert "reset-password" in call_kwargs["html"]
```

- [ ] **Step 2: Write tests/test_auth_reset_password.py**

```python
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_reset_password_invalid_token_returns_400():
    """POST /auth/reset-password with a bad token returns 400."""
    mock_collection = MagicMock()
    mock_collection.find_one = AsyncMock(return_value=None)
    from fastapi_users.db import MongoDBUserDatabase
    mock_user_db = MongoDBUserDatabase(mock_collection)

    with patch("app.database.connect", new_callable=AsyncMock), \
         patch("app.database.disconnect", new_callable=AsyncMock), \
         patch("app.database.validate_connection", new_callable=AsyncMock), \
         patch("app.database.get_client", return_value=MagicMock()), \
         patch("app.database.get_db", return_value=MagicMock()), \
         patch("app.database.get_auth_users_collection", return_value=mock_collection), \
         patch("app.database.get_bookmarks_collection", return_value=MagicMock()), \
         patch("app.database.get_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_narrators_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_hadiths_collection", return_value=MagicMock()), \
         patch("app.database.get_podia_narrators_collection", return_value=MagicMock()):
        from app.main import app
        from app.auth.database import get_user_db

        async def override_get_user_db():
            yield mock_user_db

        app.dependency_overrides[get_user_db] = override_get_user_db

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/auth/reset-password", json={
                "token": "invalid-token-string",
                "password": "NewStr0ng!Password",
            })

        app.dependency_overrides.clear()

    assert resp.status_code == 400
```

- [ ] **Step 3: Run the tests**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/test_auth_forgot_password.py tests/test_auth_reset_password.py -v
```
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_auth_forgot_password.py tests/test_auth_reset_password.py
git commit -m "test: forgot-password no-enumeration, email sent/not-sent; reset-password invalid token"
```

---

## Task 13: Write tests for bookmarks

**Files:**
- Create: `tests/test_bookmarks.py`

- [ ] **Step 1: Write tests/test_bookmarks.py**

```python
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient


def _base_patches():
    return [
        patch("app.database.connect", new_callable=AsyncMock),
        patch("app.database.disconnect", new_callable=AsyncMock),
        patch("app.database.validate_connection", new_callable=AsyncMock),
        patch("app.database.get_client", return_value=MagicMock()),
        patch("app.database.get_db", return_value=MagicMock()),
        patch("app.database.get_hadiths_collection", return_value=MagicMock()),
        patch("app.database.get_narrators_collection", return_value=MagicMock()),
        patch("app.database.get_podia_hadiths_collection", return_value=MagicMock()),
        patch("app.database.get_podia_narrators_collection", return_value=MagicMock()),
        patch("app.database.get_auth_users_collection", return_value=MagicMock()),
    ]


@pytest.mark.asyncio
async def test_bookmarks_list_unauthenticated_returns_401():
    """GET /api/v1/bookmarks without cookie returns 401."""
    patches = _base_patches()
    mock_bm_col = MagicMock()
    patches.append(patch("app.database.get_bookmarks_collection", return_value=mock_bm_col))
    for p in patches:
        p.start()
    try:
        from app.main import app
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/v1/bookmarks")
    finally:
        for p in patches:
            p.stop()
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_bookmarks_list_authenticated_returns_200():
    """GET /api/v1/bookmarks with valid user returns 200 with items + total."""
    import uuid as _uuid
    from app.auth.models import UserRead

    patches = _base_patches()
    mock_bm_col = MagicMock()
    mock_bm_col.count_documents = AsyncMock(return_value=1)
    now = datetime.now(timezone.utc)
    mock_bm_col.find.return_value.skip.return_value.limit.return_value.sort.return_value.__aiter__ = AsyncMock(
        return_value=iter([{
            "hadith_url": "https://hadathana.app/hadith/1",
            "source": "podia",
            "created_at": now,
        }])
    )
    patches.append(patch("app.database.get_bookmarks_collection", return_value=mock_bm_col))
    for p in patches:
        p.start()

    try:
        from app.main import app
        from app.auth.config import current_active_user

        mock_user = UserRead(
            id=str(_uuid.uuid4()),
            email="user@example.com",
            is_active=True,
            is_verified=True,
            is_superuser=False,
        )

        async def override_current_user():
            return mock_user

        app.dependency_overrides[current_active_user] = override_current_user

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.get("/api/v1/bookmarks")

        app.dependency_overrides.clear()
    finally:
        for p in patches:
            p.stop()

    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["hadith_url"] == "https://hadathana.app/hadith/1"


@pytest.mark.asyncio
async def test_add_bookmark_authenticated_returns_201():
    """POST /api/v1/bookmarks with valid user creates bookmark and returns 201."""
    import uuid as _uuid
    from app.auth.models import UserRead

    patches = _base_patches()
    mock_bm_col = MagicMock()
    mock_bm_col.find_one = AsyncMock(return_value=None)  # not duplicate
    mock_bm_col.insert_one = AsyncMock(return_value=MagicMock(inserted_id="mock_id"))
    patches.append(patch("app.database.get_bookmarks_collection", return_value=mock_bm_col))
    for p in patches:
        p.start()

    try:
        from app.main import app
        from app.auth.config import current_active_user

        mock_user = UserRead(
            id=str(_uuid.uuid4()),
            email="user@example.com",
            is_active=True,
            is_verified=True,
            is_superuser=False,
        )

        async def override_current_user():
            return mock_user

        app.dependency_overrides[current_active_user] = override_current_user

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/v1/bookmarks", json={
                "hadith_url": "https://hadathana.app/hadith/42",
                "source": "podia",
            })

        app.dependency_overrides.clear()
    finally:
        for p in patches:
            p.stop()

    assert resp.status_code == 201
    data = resp.json()
    assert data["hadith_url"] == "https://hadathana.app/hadith/42"
    assert data["source"] == "podia"


@pytest.mark.asyncio
async def test_add_duplicate_bookmark_returns_409():
    """POST /api/v1/bookmarks with existing bookmark returns 409."""
    import uuid as _uuid
    from datetime import datetime, timezone
    from app.auth.models import UserRead

    patches = _base_patches()
    mock_bm_col = MagicMock()
    mock_bm_col.find_one = AsyncMock(return_value={
        "user_id": "someid",
        "hadith_url": "https://hadathana.app/hadith/42",
        "source": "podia",
        "created_at": datetime.now(timezone.utc),
    })
    patches.append(patch("app.database.get_bookmarks_collection", return_value=mock_bm_col))
    for p in patches:
        p.start()

    try:
        from app.main import app
        from app.auth.config import current_active_user

        mock_user = UserRead(
            id=str(_uuid.uuid4()),
            email="user@example.com",
            is_active=True,
            is_verified=True,
            is_superuser=False,
        )

        async def override_current_user():
            return mock_user

        app.dependency_overrides[current_active_user] = override_current_user

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.post("/api/v1/bookmarks", json={
                "hadith_url": "https://hadathana.app/hadith/42",
                "source": "podia",
            })

        app.dependency_overrides.clear()
    finally:
        for p in patches:
            p.stop()

    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_delete_bookmark_authenticated_returns_204():
    """DELETE /api/v1/bookmarks/{url} with valid user returns 204."""
    import uuid as _uuid
    from app.auth.models import UserRead

    patches = _base_patches()
    mock_bm_col = MagicMock()
    mock_bm_col.delete_one = AsyncMock(return_value=MagicMock(deleted_count=1))
    patches.append(patch("app.database.get_bookmarks_collection", return_value=mock_bm_col))
    for p in patches:
        p.start()

    try:
        from app.main import app
        from app.auth.config import current_active_user

        mock_user = UserRead(
            id=str(_uuid.uuid4()),
            email="user@example.com",
            is_active=True,
            is_verified=True,
            is_superuser=False,
        )

        async def override_current_user():
            return mock_user

        app.dependency_overrides[current_active_user] = override_current_user

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            resp = await c.delete("/api/v1/bookmarks/https%3A%2F%2Fhadathana.app%2Fhadith%2F42")

        app.dependency_overrides.clear()
    finally:
        for p in patches:
            p.stop()

    assert resp.status_code == 204
```

- [ ] **Step 2: Run the tests**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/test_bookmarks.py -v
```
Expected: all 5 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_bookmarks.py
git commit -m "test: bookmarks — 401 unauthenticated, GET/POST/DELETE success, duplicate 409"
```

---

## Task 14: Run full test suite and lint

- [ ] **Step 1: Run all tests**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/ -v
```
Expected: all tests pass (no failures).

- [ ] **Step 2: Run linter**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m ruff check app/ tests/
```
Expected: no violations. If there are violations, fix them:
```bash
/home/abdo_kamar/Projects/.venv/bin/python -m ruff check app/ tests/ --fix
```

- [ ] **Step 3: Commit lint fixes if any**

```bash
git add app/ tests/
git commit -m "chore: fix ruff lint violations in auth and bookmark files"
```

---

## Task 15: End-to-end smoke test with dev stack

- [ ] **Step 1: Start dev stack**

```bash
make dev
```

- [ ] **Step 2: Register a user**

```bash
curl -s -X POST http://localhost:8001/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"smoke@test.com","password":"Str0ngPass!"}' | python3 -m json.tool
```
Expected: JSON with `email`, `id`, `is_active: true`, no `hashed_password`.

- [ ] **Step 3: Login and capture cookie**

```bash
curl -s -c /tmp/hadathana_cookies.txt -X POST http://localhost:8001/auth/login \
  -d 'username=smoke@test.com&password=Str0ngPass!' | python3 -m json.tool
```
Expected: JSON with user data. Check cookie file:
```bash
cat /tmp/hadathana_cookies.txt
```
Expected: line containing `access_token`.

- [ ] **Step 4: Add a bookmark (authenticated)**

```bash
curl -s -b /tmp/hadathana_cookies.txt -X POST http://localhost:8001/api/v1/bookmarks \
  -H "Content-Type: application/json" \
  -d '{"hadith_url":"https://hadathana.app/hadith/1","source":"podia"}' | python3 -m json.tool
```
Expected: `201` with `hadith_url` and `source` fields.

- [ ] **Step 5: List bookmarks**

```bash
curl -s -b /tmp/hadathana_cookies.txt http://localhost:8001/api/v1/bookmarks | python3 -m json.tool
```
Expected: `{"items":[...],"total":1}`.

- [ ] **Step 6: Try bookmarks without cookie — expect 401**

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8001/api/v1/bookmarks
```
Expected: `401`

- [ ] **Step 7: Check API docs at http://localhost:8001/docs**

Confirm `/auth/register`, `/auth/login`, `/auth/forgot-password`, `/auth/reset-password`, and `/api/v1/bookmarks` all appear.

- [ ] **Step 8: Final commit if any fixes made during smoke test**

```bash
git add .
git commit -m "fix: smoke test corrections"
```

---

## Verification Checklist

- [ ] `pytest tests/ -v` — all tests green
- [ ] `ruff check app/ tests/` — no violations
- [ ] `/auth/register` returns 201 + no password in response
- [ ] `/auth/login` sets `access_token` HttpOnly cookie
- [ ] `/auth/forgot-password` returns 202 regardless of email
- [ ] `/api/v1/bookmarks` returns 401 without cookie
- [ ] Bookmark CRUD works end-to-end with cookie
- [ ] API docs at `/docs` shows all new endpoints
