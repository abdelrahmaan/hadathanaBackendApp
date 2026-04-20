# Design: Per-User Daily Quota System

**Date:** 2026-04-19
**Branch:** feat-chatbot_v1
**Status:** Approved

---

## Context

The chatbot (`POST /api/v2/chat`) currently has no usage limits. Every request invokes an LLM (OpenRouter) and a Cohere reranker — both cost money. The project is pre-monetization but donation-funded (~$1/donation at current rates). We need to:

1. Cap daily requests per user to protect costs now
2. Lay down tier-ready infrastructure for future paid plans
3. Keep the architecture clean — quota logic must not bleed into agent code

---

## Goals

- Free users: **3 requests/day** (tunable via env var)
- Supporter users: **10 requests/day** (tunable via env var)
- Unlimited users: **no cap** (for admins / future paid tier)
- 429 response with Arabic message + upgrade hint when limit reached
- No Redis — MongoDB-only, atomic `$inc` is sufficient at current scale

---

## Data Model

### New collection: `user_quotas`

```json
{
  "user_id": "uuid-string",
  "usage_date": "2026-04-19",
  "request_count": 2,
  "tier": "free",
  "expires_at": "2026-04-21T00:00:00Z"
}
```

- Compound unique index: `{ user_id: 1, usage_date: 1 }`
- TTL index on `expires_at` (BSON datetime, 2 days after creation): old counters auto-deleted, no manual cleanup
- `usage_date` kept as string for human-readable querying; `expires_at` is a proper datetime for TTL

### User tier field

Add `tier: str = "free"` to `User` model (`app/auth/models.py`).  
Stored in `auth_users` collection. Absence of field = `"free"` (backward-compatible).  
Valid values: `"free"` | `"supporter"` | `"unlimited"`

Tier is **not self-served** — set manually (or via future admin endpoint) when a user donates.

---

## Configuration

Added to `app/config.py` (all overridable via env vars):

```python
quota_free_daily: int = 3        # QUOTA_FREE_DAILY
quota_supporter_daily: int = 10  # QUOTA_SUPPORTER_DAILY
quota_unlimited_daily: int = -1  # -1 = no limit
```

Helper method:
```python
def get_daily_limit(self, tier: str) -> int:
    return {
        "free": self.quota_free_daily,
        "supporter": self.quota_supporter_daily,
        "unlimited": self.quota_unlimited_daily,
    }.get(tier, self.quota_free_daily)
```

---

## Implementation: FastAPI Dependency

New file: `app/chatbot/quota.py`

```python
from datetime import date
from fastapi import Depends, HTTPException
from app.auth.config import current_active_user
from app.auth.models import User
from app.config import settings
from app.database import get_client, get_db

async def check_quota(user: User = Depends(current_active_user)):
    tier = getattr(user, "tier", "free")
    limit = settings.get_daily_limit(tier)
    if limit == -1:
        return  # unlimited tier

    today = date.today().isoformat()
    db = get_db(get_client())
    result = await db["user_quotas"].find_one_and_update(
        {"user_id": str(user.id), "usage_date": today},
        {
            "$inc": {"request_count": 1},
            "$setOnInsert": {"tier": tier},
        },
        upsert=True,
        return_document=True,  # ReturnDocument.AFTER
    )
    count = result["request_count"]
    if count > limit:
        raise HTTPException(
            status_code=429,
            detail={
                "ar": "لقد وصلت إلى الحد اليومي. ادعم المشروع للحصول على المزيد.",
                "limit": limit,
                "used": count,
                "upgrade_hint": "supporter",
            },
        )
```

**Why `find_one_and_update` with `$inc`:** MongoDB's findAndModify is atomic at the document level — increment and read happen in one operation, preventing race conditions under concurrent requests.

### Injection into chat endpoint (`app/chatbot/router.py`)

```python
from app.chatbot.quota import check_quota

@router.post("/chat")
async def chat(
    request: ChatRequest,
    user: User = Depends(current_active_user),
    _: None = Depends(check_quota),
):
```

The `check_quota` dependency runs **before** the agent is invoked. If the user is over limit, FastAPI raises 429 and the agent is never called — no LLM cost incurred.

---

## Files Changed

| Action | File | What changes |
|---|---|---|
| Create | `app/chatbot/quota.py` | `check_quota` dependency |
| Modify | `app/config.py` | Add 3 quota fields + `get_daily_limit()` |
| Modify | `app/auth/models.py` | Add `tier: str = "free"` to `User` and `UserRead` |
| Modify | `app/auth/database.py` | `create()` sets `tier="free"` default explicitly |
| Modify | `app/chatbot/router.py` | Add `_: None = Depends(check_quota)` to `POST /chat` |
| Modify | `app/database.py` | Register `user_quotas` collection + create indexes |
| Modify | `.env.example` | Add `QUOTA_FREE_DAILY`, `QUOTA_SUPPORTER_DAILY` |
| Create | `tests/test_quota.py` | Unit + integration tests for quota logic |

---

## Indexes (MongoDB)

```python
# In create_indexes() or database startup
await db["user_quotas"].create_index(
    [("user_id", 1), ("usage_date", 1)],
    unique=True,
)
await db["user_quotas"].create_index(
    "expires_at",
    expireAfterSeconds=0,  # TTL: delete when expires_at is reached
)
```

`expires_at` is set to `datetime(today) + timedelta(days=2)` on document creation via `$setOnInsert`.

---

## Testing Plan

1. **Unit test** `check_quota` with a mocked collection:
   - First request: count=1, no 429
   - Request at limit: count=limit, no 429
   - Request over limit: count=limit+1, raises 429
   - Unlimited tier: never raises regardless of count

2. **Integration test** against real dev MongoDB:
   - POST `/api/v2/chat` 3 times as free user → 3rd succeeds
   - 4th request → 429 with Arabic message and `upgrade_hint`
   - Supporter user → gets 10 requests before 429

3. **Verify 429 response shape** matches what frontend expects

---

## Future Extensions (not in scope now)

- Admin endpoint `PATCH /api/v1/admin/users/{user_id}/tier` to upgrade users after donation
- Usage stats endpoint `GET /api/v2/chat/quota` returning `{used, limit, resets_at}`
- Webhook from payment processor to auto-upgrade tier
