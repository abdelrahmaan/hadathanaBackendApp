import logging
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .config import settings
from .database import connect, disconnect, validate_connection, get_client, get_db_status
from .logging_config import setup_logging
from .middleware import RequestLoggingMiddleware
from .routers import hadiths_shamela, narrators_shamela, hadiths_podia, narrators_podia, search_podia

setup_logging()

logger = logging.getLogger("hadathana.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("startup", extra={"event": "startup"})
    await connect()
    await validate_connection()
    yield
    await disconnect()
    logger.info("shutdown", extra={"event": "shutdown"})


app = FastAPI(title="hadathana-api", lifespan=lifespan)

# RequestLoggingMiddleware must be added first (executes outermost = sees final status)
# Starlette middleware stack is LIFO: last add_middleware call = innermost wrapper
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_cors_origins(),
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(hadiths_shamela.router)
app.include_router(narrators_shamela.router)
app.include_router(hadiths_podia.router)
app.include_router(narrators_podia.router)
app.include_router(search_podia.router)


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
