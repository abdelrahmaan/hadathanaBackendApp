import logging
from unittest.mock import patch

import pytest


@pytest.mark.asyncio
async def test_request_log_emitted_on_health(client):
    """Every request should emit a structured log with event=request."""
    request_logger = logging.getLogger("hadathana.request")
    with patch.object(request_logger, "info") as mock_info:
        resp = await client.get("/health")

    assert resp.status_code == 200
    assert mock_info.called, "Request logger should have been called"
    call_kwargs = mock_info.call_args[1]
    extra = call_kwargs["extra"]
    assert extra["event"] == "request"
    assert extra["path"] == "/health"
    assert extra["method"] == "GET"
    assert extra["status"] == 200
    assert "duration_ms" in extra


@pytest.mark.asyncio
async def test_search_log_emitted_on_list_hadiths(client):
    """List hadiths with a filter should emit event=search with active filters."""
    search_logger = logging.getLogger("app.routers.hadiths_shamela")
    with patch.object(search_logger, "info") as mock_info:
        resp = await client.get("/api/v1/hadiths?hadith_plain=test")

    assert resp.status_code == 200
    search_calls = [c for c in mock_info.call_args_list if c.args and c.args[0] == "search"]
    assert len(search_calls) == 1, "Exactly one search log expected"
    extra = search_calls[0].kwargs["extra"]
    assert extra["event"] == "search"
    assert extra["endpoint"] == "/api/v1/hadiths"
    assert extra["filters"].get("hadith_plain") == "test"
    assert "total" in extra


@pytest.mark.asyncio
async def test_search_log_empty_filters_when_no_params(client):
    """List hadiths with no filters should log empty filters dict."""
    search_logger = logging.getLogger("app.routers.hadiths_shamela")
    with patch.object(search_logger, "info") as mock_info:
        resp = await client.get("/api/v1/hadiths")

    assert resp.status_code == 200
    search_calls = [c for c in mock_info.call_args_list if c.args and c.args[0] == "search"]
    assert len(search_calls) == 1
    extra = search_calls[0].kwargs["extra"]
    assert extra["filters"] == {}


@pytest.mark.asyncio
async def test_exception_handler_returns_500_json(client):
    """Unhandled exceptions should return 500 JSON with detail key and log event=error."""
    from fastapi import Request

    from app.main import app

    @app.get("/test-error-unique")
    async def boom(request: Request):
        raise RuntimeError("intentional test error")

    error_logger = logging.getLogger("hadathana.main")
    with patch.object(error_logger, "error") as mock_error:
        resp = await client.get("/test-error-unique")

    assert resp.status_code == 500
    assert resp.json() == {"detail": "Internal server error."}
    assert mock_error.called
    extra = mock_error.call_args[1]["extra"]
    assert extra["event"] == "error"
    assert "traceback" in extra
