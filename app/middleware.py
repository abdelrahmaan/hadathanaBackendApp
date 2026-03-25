import logging
import time
import traceback

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

request_logger = logging.getLogger("hadathana.request")
error_logger = logging.getLogger("hadathana.main")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            error_logger.error(
                "unhandled_exception",
                extra={
                    "event": "error",
                    "path": request.url.path,
                    "method": request.method,
                    "error": repr(exc),
                    "traceback": traceback.format_exc(),
                },
            )
            request_logger.info(
                "request",
                extra={
                    "event": "request",
                    "method": request.method,
                    "path": request.url.path,
                    "query": str(request.url.query),
                    "status": 500,
                    "duration_ms": duration_ms,
                    "ip": request.client.host if request.client else None,
                },
            )
            return JSONResponse(status_code=500, content={"detail": "Internal server error."})

        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        request_logger.info(
            "request",
            extra={
                "event": "request",
                "method": request.method,
                "path": request.url.path,
                "query": str(request.url.query),
                "status": response.status_code,
                "duration_ms": duration_ms,
                "ip": request.client.host if request.client else None,
            },
        )
        return response
