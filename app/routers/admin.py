from datetime import UTC, date, datetime

from fastapi import APIRouter, Depends, HTTPException

from ..auth.config import current_active_user
from ..auth.models import User
from ..chatbot.qdrant import get_qdrant_client
from ..config import settings
from ..database import (
    get_auth_users_collection,
    get_chat_sessions_collection,
    get_client,
    get_db,
    get_hadiths_collection,
    get_podia_hadiths_collection,
    get_podia_narrators_collection,
    get_user_quotas_collection,
)
from ..models.admin import (
    AdminStats,
    ChatbotStats,
    DataStats,
    SystemHealth,
    TierUpdate,
    UsersByTier,
    UserStats,
    UserTierResponse,
)

router = APIRouter(prefix="/api/v2/admin", tags=["admin"])


def _require_superuser(user: User = Depends(current_active_user)) -> User:
    if not user.is_superuser:
        raise HTTPException(status_code=403, detail="Superuser access required.")
    return user


async def _get_system_health() -> SystemHealth:
    client = get_client()
    try:
        await client.admin.command("ping")
        mongodb_status = "connected"
        db_ok = True
    except Exception:
        mongodb_status = "disconnected"
        db_ok = False

    qdrant_client = get_qdrant_client()
    try:
        if qdrant_client:
            qdrant_client.get_collections()
            qdrant_status = "connected"
        else:
            qdrant_status = "disconnected"
    except Exception:
        qdrant_status = "disconnected"

    return SystemHealth(
        status="ok" if db_ok else "degraded",
        mongodb=mongodb_status,
        qdrant=qdrant_status,
        chatbot_enabled=settings.chatbot_enabled,
    )


async def _get_user_stats() -> UserStats:
    db = get_db(get_client())
    users_col = get_auth_users_collection(db)
    quotas_col = get_user_quotas_collection(db)

    pipeline = [{"$group": {"_id": "$tier", "count": {"$sum": 1}}}]
    tier_counts = {"free": 0, "supporter": 0, "unlimited": 0}
    total = 0
    async for doc in users_col.aggregate(pipeline):
        tier = doc["_id"] or "free"
        count = doc["count"]
        if tier in tier_counts:
            tier_counts[tier] = count
        total += count

    today_str = date.today().isoformat()
    quota_pipeline = [
        {"$match": {"usage_date": today_str}},
        {"$group": {"_id": None, "total_used": {"$sum": "$count"}}},
    ]
    quota_used_today = 0
    async for doc in quotas_col.aggregate(quota_pipeline):
        quota_used_today = doc["total_used"]

    at_limit_pipeline = [
        {"$match": {"usage_date": today_str, "count": {"$gte": settings.quota_free_daily}}},
        {"$count": "total"},
    ]
    users_at_limit = 0
    async for doc in quotas_col.aggregate(at_limit_pipeline):
        users_at_limit = doc["total"]

    return UserStats(
        total=total,
        by_tier=UsersByTier(**tier_counts),
        quota_used_today=quota_used_today,
        users_at_limit_today=users_at_limit,
    )


async def _get_chatbot_stats() -> ChatbotStats:
    db = get_db(get_client())
    sessions_col = get_chat_sessions_collection(db)

    total_sessions = await sessions_col.count_documents({})

    today_midnight = datetime.combine(date.today(), datetime.min.time(), tzinfo=UTC)
    pipeline = [
        {
            "$facet": {
                "total_msgs": [
                    {"$project": {"msg_count": {"$size": "$messages"}}},
                    {"$group": {"_id": None, "total": {"$sum": "$msg_count"}}},
                ],
                "today_msgs": [
                    {"$unwind": "$messages"},
                    {"$match": {"messages.timestamp": {"$gte": today_midnight}}},
                    {"$count": "total"},
                ],
            }
        }
    ]

    total_messages = 0
    messages_today = 0
    async for doc in sessions_col.aggregate(pipeline):
        if doc["total_msgs"]:
            total_messages = doc["total_msgs"][0].get("total", 0)
        if doc["today_msgs"]:
            messages_today = doc["today_msgs"][0].get("total", 0)

    avg = round(total_messages / total_sessions, 2) if total_sessions > 0 else 0.0

    return ChatbotStats(
        total_sessions=total_sessions,
        total_messages=total_messages,
        messages_today=messages_today,
        avg_messages_per_session=avg,
    )


async def _get_data_stats() -> DataStats:
    db = get_db(get_client())

    podia_col = get_podia_hadiths_collection(db)
    shamela_col = get_hadiths_collection(db)
    narrators_col = get_podia_narrators_collection(db)

    podia_hadiths = await podia_col.count_documents({})
    shamela_hadiths = await shamela_col.count_documents({})
    podia_narrators = await narrators_col.count_documents({})
    topic_list = await podia_col.distinct("topics")
    topics = len(topic_list)

    qdrant_points = 0
    qdrant_client = get_qdrant_client()
    if qdrant_client:
        try:
            info = qdrant_client.get_collection("hadiths_matn")
            qdrant_points = info.points_count or 0
        except Exception:
            qdrant_points = 0

    return DataStats(
        podia_hadiths=podia_hadiths,
        shamela_hadiths=shamela_hadiths,
        podia_narrators=podia_narrators,
        topics=topics,
        qdrant_points=qdrant_points,
    )


@router.get("/stats", response_model=AdminStats)
async def admin_stats(_user: User = Depends(_require_superuser)) -> AdminStats:
    system = await _get_system_health()
    users = await _get_user_stats()
    chatbot = await _get_chatbot_stats()
    data = await _get_data_stats()
    return AdminStats(system=system, users=users, chatbot=chatbot, data=data)


@router.patch("/users/{user_id}/tier", response_model=UserTierResponse)
async def update_user_tier(
    user_id: str,
    body: TierUpdate,
    _user: User = Depends(_require_superuser),
) -> UserTierResponse:
    db = get_db(get_client())
    users_col = get_auth_users_collection(db)

    result = await users_col.find_one_and_update(
        {"id": user_id},
        {"$set": {"tier": body.tier}},
        return_document=True,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="User not found.")

    return UserTierResponse(
        id=str(result["id"]),
        email=result["email"],
        tier=result["tier"],
        is_active=result.get("is_active", True),
        is_superuser=result.get("is_superuser", False),
    )
