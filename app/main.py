import logging
import os
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
from .auth.models import UserCreate, UserRead
from .config import settings

# Wire LangSmith tracing — must happen before any LangChain import
if settings.get_langsmith_api_key():
    os.environ["LANGSMITH_TRACING"] = "true"
    os.environ["LANGSMITH_API_KEY"] = settings.get_langsmith_api_key()
    os.environ["LANGSMITH_PROJECT"] = settings.get_langsmith_project()
from .database import (
    connect,
    disconnect,
    get_client,
    get_db_status,
    validate_connection,
)

if settings.chatbot_enabled:
    from .chatbot.agent import build_agent
    from .chatbot.qdrant import connect_qdrant, disconnect_qdrant
    from .chatbot.router import router as chatbot_router
from .logging_config import setup_logging
from .middleware import RequestLoggingMiddleware
from .routers import (
    admin,
    bookmarks,
    hadiths_podia,
    hadiths_shamela,
    narrators_podia,
    narrators_shamela,
    search_podia,
)

setup_logging()

logger = logging.getLogger("hadathana.main")

limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("startup", extra={"event": "startup"})
    await connect()
    await validate_connection()
    if settings.chatbot_enabled:
        await connect_qdrant()
        build_agent()
    yield
    if settings.chatbot_enabled:
        await disconnect_qdrant()
    await disconnect()
    logger.info("shutdown", extra={"event": "shutdown"})


app = FastAPI(title="hadathana-api", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# RequestLoggingMiddleware must be added first (executes outermost = sees final status)
# Starlette middleware stack is LIFO: last add_middleware call = innermost wrapper
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_cors_origins(),
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------
# Auth routes
# ------------------------------------------------------------------

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

# ------------------------------------------------------------------
# Data routes
# ------------------------------------------------------------------

app.include_router(hadiths_shamela.router)
app.include_router(narrators_shamela.router)
app.include_router(hadiths_podia.router)
app.include_router(narrators_podia.router)
app.include_router(search_podia.router)
if settings.chatbot_enabled:
    app.include_router(chatbot_router)
app.include_router(bookmarks.router)
app.include_router(admin.router)

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

    result["chatbot"] = settings.chatbot_enabled

    return result
