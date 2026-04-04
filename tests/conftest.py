import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport


@pytest_asyncio.fixture
async def client():
    """AsyncClient with mocked MongoDB so no live DB is needed."""
    mock_collection = MagicMock()
    mock_collection.find.return_value.__aiter__ = AsyncMock(return_value=iter([]))
    mock_collection.count_documents = AsyncMock(return_value=0)
    mock_collection.find_one = AsyncMock(return_value=None)

    mock_db = MagicMock()
    mock_client = MagicMock()

    with patch("app.database.connect", new_callable=AsyncMock), \
         patch("app.database.disconnect", new_callable=AsyncMock), \
         patch("app.database.validate_connection", new_callable=AsyncMock), \
         patch("app.database.get_client", return_value=mock_client), \
         patch("app.database.get_db", return_value=mock_db), \
         patch("app.database.get_hadiths_collection", return_value=mock_collection), \
         patch("app.database.get_narrators_collection", return_value=mock_collection), \
         patch("app.database.get_podia_hadiths_collection", return_value=mock_collection), \
         patch("app.database.get_podia_narrators_collection", return_value=mock_collection):
        from app.main import app
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c
