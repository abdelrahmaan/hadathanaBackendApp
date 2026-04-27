from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str
    session_id: str | None = None  # None → server generates new UUID
    topic: str | None = None        # optional Qdrant payload filter
    book: str | None = None         # optional Qdrant payload filter


class Citation(BaseModel):
    resource_id: str   # internal hadith _id (Qdrant point id = hadith_indices[0])
    text_span: str     # matn_text_plain[:200]
    confidence: float  # reranker relevance_score, 0.0–1.0
    title: str = ""    # short Arabic headline (≤150 chars), from hadith title field
    hadith_url: str = ""  # source URL for frontend deep-linking


class SessionMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    citations: list[Citation] = []
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ChatSession(BaseModel):
    session_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str                           # UUID string from auth User.id
    title: str = ""                        # populated by thread_rename event
    created_at: datetime = Field(default_factory=datetime.utcnow)
    messages: list[SessionMessage] = []


class ChatSessionMeta(BaseModel):
    session_id: str
    title: str
    created_at: datetime
    message_count: int


class QuotaStatus(BaseModel):
    tier: str        # "free" | "supporter" | "unlimited"
    limit: int       # daily cap; -1 = unlimited
    used: int        # requests made today (0 if none yet)
    remaining: int   # limit - used; -1 if unlimited
    resets_at: datetime  # UTC midnight tonight — when today's counter expires
