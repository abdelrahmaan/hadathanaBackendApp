# Auth & Bookmarks Design Spec
**Date:** 2026-04-18
**Status:** Approved

## Context

Hadathana is a read-only FastAPI + Motor (async MongoDB) hadith search API. Currently all endpoints are public GET-only with no authentication. This spec adds sign up, sign in, forgot/reset password, and hadith bookmarking — the foundational user layer for v2.0.

**Why now:** Bookmarks require a user identity. Auth is the prerequisite. The frontend is Next.js (browser-only), so HttpOnly cookies are the correct session mechanism.

---

## Approach

Use **`fastapi-users[motor]`** with the `MongoDBUserDatabase` adapter (raw Motor, no Beanie ODM) — consistent with existing Motor dict-based patterns. Add **`slowapi`** for rate limiting on sensitive endpoints. Use **Resend** for transactional email.

---

## MongoDB Collections

All collections live in the same database (`HadithData` / `HadithDataDev`):

| Collection | Purpose | Key indexes |
|---|---|---|
| `auth_users` | User accounts | unique on `email` |
| `auth_password_reset_tokens` | Reset tokens (hashed, single-use) | TTL on `expires_at`, index on `token_hash` |
| `auth_refresh_sessions` | Refresh token store (hashed) | TTL on `expires_at`, index on `token_hash` |
| `user_bookmarks` | Per-user hadith bookmarks | compound unique on `(user_id, hadith_url)` |

### `auth_users` document shape
```json
{
  "_id": "ObjectId",
  "email": "string (lowercased, unique)",
  "hashed_password": "string (Argon2id)",
  "is_active": true,
  "is_verified": false,
  "is_superuser": false,
  "created_at": "datetime",
  "last_login_at": "datetime | null",
  "roles": ["user"]
}
```

### `auth_password_reset_tokens` document shape
```json
{
  "_id": "ObjectId",
  "user_id": "ObjectId",
  "token_hash": "string (SHA-256 of raw token)",
  "expires_at": "datetime (30 min TTL)",
  "used_at": "datetime | null"
}
```

### `user_bookmarks` document shape
```json
{
  "_id": "ObjectId",
  "user_id": "ObjectId",
  "hadith_url": "string",
  "source": "shamela | podia",
  "created_at": "datetime"
}
```

---

## New Files

```
app/
├── auth/
│   ├── __init__.py
│   ├── config.py        # FastAPI Users setup: UserManager, cookie transport, JWT strategy
│   ├── models.py        # UserCreate, UserRead, UserUpdate Pydantic models
│   └── database.py      # get_user_db() — Motor MongoDBUserDatabase adapter
└── routers/
    └── bookmarks.py     # Authenticated bookmark CRUD endpoints
```

**Modified files:**
- `app/main.py` — mount auth router, bookmarks router, add slowapi limiter, update CORS methods
- `app/database.py` — add `get_auth_users_collection()`, `get_bookmarks_collection()`
- `app/config.py` — add JWT_SECRET, token TTL settings, RESEND_API_KEY, FROM_EMAIL
- `.env.example` — document new env vars
- `scripts/bootstrap_local_db.py` — create TTL indexes for token collections
- `mongo_migration/create_indexes.py` — add auth + bookmark indexes

---

## Endpoints

### Auth (FastAPI Users, mounted at `/auth`)

| Method | Path | Purpose | Rate limit |
|---|---|---|---|
| `POST` | `/auth/register` | Create account | 20/min |
| `POST` | `/auth/login` | Set HttpOnly cookies | 10/min |
| `POST` | `/auth/logout` | Clear cookies | — |
| `POST` | `/auth/forgot-password` | Send reset email | 5/min |
| `POST` | `/auth/reset-password` | Consume token, set new password | 10/min |

### Bookmarks (custom router, `/api/v1/bookmarks`)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/bookmarks` | List current user's bookmarks (paginated, skip/limit) |
| `POST` | `/api/v1/bookmarks` | Add bookmark (`hadith_url`, `source`) |
| `DELETE` | `/api/v1/bookmarks/{hadith_url}` | Remove bookmark |

---

## Session Strategy

- **Access token:** JWT, 15-min TTL, set as `access_token` HttpOnly Secure SameSite=Lax cookie
- **Refresh token:** opaque token, 30-day TTL, set as `refresh_token` HttpOnly Secure SameSite=Lax cookie
- FastAPI Users handles cookie set/clear on login/logout automatically
- `current_user` FastAPI dependency injected into all bookmark endpoints

---

## Data Flows

### Registration
1. `POST /auth/register` — FastAPI Users validates email uniqueness + password min length
2. Password hashed with Argon2id
3. User document created in `auth_users`
4. Verification email sent via Resend (non-blocking — account usable immediately)
5. Returns `UserRead` (id, email, is_verified)

### Login
1. `POST /auth/login` — FastAPI Users checks email exists + verifies Argon2id hash
2. Issues JWT access token + refresh token
3. Both set as HttpOnly cookies — JS never reads them
4. Returns `UserRead`

### Forgot Password (OWASP-compliant)
1. `POST /auth/forgot-password` — always returns `200 OK` (prevents user enumeration)
2. If email exists: generate cryptographically random token, store SHA-256 hash in `auth_password_reset_tokens` with 30-min `expires_at`
3. Resend delivers: `https://hadathana.app/reset-password?token=<raw_token>`
4. `POST /auth/reset-password` — validate token hash, check not expired and not used, set new password, mark `used_at`
5. MongoDB TTL index auto-deletes expired tokens

### Bookmarks
- All routes require valid access token cookie (`current_user` dependency — 401 if missing/invalid)
- `user_id` always sourced from verified JWT, never from request body

---

## Security Notes

- Passwords: Argon2id via `fastapi-users` default (OWASP-recommended)
- Reset tokens: stored as SHA-256 hash only — raw token only ever in the email link
- No user enumeration: forgot-password always returns same response
- CORS: update `allow_methods` to include `POST`, `DELETE`, `OPTIONS`
- Rate limiting: `slowapi` on auth endpoints (limits above)
- Cookies: HttpOnly, Secure, SameSite=Lax — JS cannot read tokens

---

## New Environment Variables

```bash
# Auth
JWT_SECRET=<random 32-byte hex — generate with: openssl rand -hex 32>
ACCESS_TOKEN_EXPIRE_MINUTES=15
REFRESH_TOKEN_EXPIRE_DAYS=30
RESET_TOKEN_EXPIRE_MINUTES=30

# Email (Resend)
RESEND_API_KEY=<your resend api key>
FROM_EMAIL=noreply@hadathana.app
```

---

## Testing Plan

**New test files:**

| File | Scenarios |
|---|---|
| `tests/test_auth_register.py` | success 201, duplicate email 400, weak password 422 |
| `tests/test_auth_login.py` | success + cookies set, wrong password 400, unknown email 400 |
| `tests/test_auth_forgot_password.py` | always 200, Resend called when email exists, not called when unknown |
| `tests/test_auth_reset_password.py` | valid token 200, expired token 400, already-used token 400 |
| `tests/test_bookmarks.py` | GET/POST/DELETE success, unauthenticated 401, duplicate bookmark 409 |

**Mock approach:** same pattern as existing tests — patch `get_user_db()`, `get_bookmarks_collection()` via `unittest.mock.patch`. Mock `resend.Emails.send()` to assert calls without sending real email. Inject mock `current_user` via FastAPI dependency override.

---

## Dependencies to Add

```
fastapi-users[motor]   # auth framework + Motor adapter
slowapi                # rate limiting
resend                 # email delivery
```
