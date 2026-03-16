from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .database import connect, disconnect
from .routers import hadiths_shamela, narrators_shamela, hadiths_podia, narrators_podia


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect()
    yield
    await disconnect()


app = FastAPI(title="hadathana-api", lifespan=lifespan)

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


@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}
