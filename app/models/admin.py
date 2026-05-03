from typing import Literal

from pydantic import BaseModel


class SystemHealth(BaseModel):
    status: Literal["ok", "degraded"]
    mongodb: Literal["connected", "disconnected"]
    qdrant: Literal["connected", "disconnected"]
    chatbot_enabled: bool


class UsersByTier(BaseModel):
    free: int
    supporter: int
    unlimited: int


class UserStats(BaseModel):
    total: int
    by_tier: UsersByTier
    quota_used_today: int
    users_at_limit_today: int


class ChatbotStats(BaseModel):
    total_sessions: int
    total_messages: int
    messages_today: int
    avg_messages_per_session: float


class DataStats(BaseModel):
    podia_hadiths: int
    shamela_hadiths: int
    podia_narrators: int
    topics: int
    qdrant_points: int


class AdminStats(BaseModel):
    system: SystemHealth
    users: UserStats
    chatbot: ChatbotStats
    data: DataStats


class TierUpdate(BaseModel):
    tier: Literal["free", "supporter", "unlimited"]


class UserTierResponse(BaseModel):
    id: str
    email: str
    tier: str
    is_active: bool
    is_superuser: bool
