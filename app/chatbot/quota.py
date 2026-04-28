from datetime import date

from fastapi import Depends, HTTPException

from app import database
from app.auth.config import current_active_user
from app.auth.models import User
from app.config import settings


async def check_quota(user: User = Depends(current_active_user)) -> None:
    tier = getattr(user, "tier", "free")
    limit = settings.get_daily_limit(tier)
    if limit == -1:
        return

    today = date.today()
    quotas = database.get_user_quotas_collection(
        database.get_db(database.get_client())
    )
    doc = await quotas.find_one(
        {"user_id": str(user.id), "usage_date": today.isoformat()}
    )
    count = int(doc["request_count"]) if doc else 0
    if count >= limit:
        raise HTTPException(
            status_code=429,
            detail={
                "ar": "لقد وصلت إلى الحد اليومي. ادعم المشروع للحصول على المزيد.",
                "limit": limit,
                "used": count,
                "upgrade_hint": "supporter",
            },
        )


async def increment_quota(user_id: str, tier: str) -> None:
    today = date.today()
    quotas = database.get_user_quotas_collection(
        database.get_db(database.get_client())
    )
    await quotas.find_one_and_update(
        {"user_id": user_id, "usage_date": today.isoformat()},
        {
            "$inc": {"request_count": 1},
            "$setOnInsert": {
                "tier": tier,
                "expires_at": database.get_quota_expiry(today),
            },
        },
        upsert=True,
    )
