from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import hadiths, narrators

app = FastAPI(title="hadathana-api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(hadiths.router)
app.include_router(narrators.router)
