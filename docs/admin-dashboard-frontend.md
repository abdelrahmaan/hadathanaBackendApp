# Admin Dashboard — Frontend Implementation Guide

This document describes what the FE team needs to build: a `/admin` page in the Next.js frontend that displays a live ops panel for the project owner.

---

## Overview

- **Route:** `/admin`
- **Access:** Superuser only — redirect to `/` if `is_superuser !== true`
- **Data:** `GET /api/v2/admin/stats` + `GET /api/v2/admin/users` on page load, with manual refresh and tier update action
- **Layout:** 4 stats sections + users table

---

## API Endpoints

```
GET /api/v2/admin/stats
GET /api/v2/admin/users?skip=0&limit=20
PATCH /api/v2/admin/users/{user_id}/tier
```

**Auth:** Requires authenticated session (HttpOnly cookie). Returns `401` if not logged in, `403` if not superuser.

**Base URL:** `NEXT_PUBLIC_API_URL` (already in your env config)

### `GET /api/v2/admin/stats`

Use this for the stat cards at the top of the page.

#### Response Shape

```ts
type AdminStats = {
  system: {
    status: "ok" | "degraded"
    mongodb: "connected" | "disconnected"
    qdrant: "connected" | "disconnected"
    chatbot_enabled: boolean
  }
  users: {
    total: number
    by_tier: {
      free: number
      supporter: number
      unlimited: number
    }
    quota_used_today: number       // total requests made today across all users
    users_at_limit_today: number   // users who have hit their daily quota cap
  }
  chatbot: {
    total_sessions: number
    total_messages: number
    messages_today: number
    avg_messages_per_session: number
  }
  data: {
    podia_hadiths: number
    shamela_hadiths: number
    podia_narrators: number
    topics: number
    qdrant_points: number
  }
}
```

### `GET /api/v2/admin/users?skip=0&limit=20`

Use this for the users management table.

#### Query Params

- `skip`: default `0`
- `limit`: default `20`

#### Response Shape

```ts
type AdminUser = {
  id: string
  email: string
  tier: "free" | "supporter" | "unlimited"
  is_active: boolean
  is_superuser: boolean
}

type PaginatedUsers = {
  items: AdminUser[]
  total: number
}
```

#### Example Response

```json
{
  "items": [
    {
      "id": "uuid-1",
      "email": "a@example.com",
      "tier": "free",
      "is_active": true,
      "is_superuser": false
    },
    {
      "id": "uuid-2",
      "email": "b@example.com",
      "tier": "supporter",
      "is_active": true,
      "is_superuser": false
    }
  ],
  "total": 2
}
```

### `PATCH /api/v2/admin/users/{user_id}/tier`

Use this from a tier dropdown or segmented control in the users table.

#### Body

```ts
type TierUpdate = {
  tier: "free" | "supporter" | "unlimited"
}
```

#### Example Request

```json
{ "tier": "supporter" }
```

#### Response Shape

Returns the updated `AdminUser` object.

---

## Page Structure

### Auth Guard (`app/admin/layout.tsx`)

- Call `GET /auth/me` server-side
- If response is `401` / `403` or `is_superuser !== true` → `redirect("/")`
- Wrap the page in a minimal admin shell (no public nav needed)

### Dashboard Page (`app/admin/page.tsx`)

Server component. Fetch `GET /api/v2/admin/stats` and first-page `GET /api/v2/admin/users?skip=0&limit=20` with `cache: "no-store"`.

**Sections (top to bottom):**

#### 1. System Health
4 stat cards in a row:

| Label | Value | Green if | Red if |
|---|---|---|---|
| API Status | `system.status` | `"ok"` | `"degraded"` |
| MongoDB | `system.mongodb` | `"connected"` | `"disconnected"` |
| Qdrant | `system.qdrant` | `"connected"` | `"disconnected"` |
| Chatbot | `system.chatbot_enabled` → `"enabled"` / `"disabled"` | `true` | `false` |

#### 2. Users & Quotas
Row 1 — 4 cards: Total Users · Free · Supporter · Unlimited
Row 2 — 2 cards: Quota Used Today · Users at Limit Today (orange if > 0)

#### 3. Chatbot Activity
4 cards in a row: Total Sessions · Total Messages · Messages Today · Avg / Session

#### 4. Data Overview
Row 1 — 4 cards: Podia Hadiths · Shamela Hadiths · Narrators · Topics
Row 2 — 2 cards: Qdrant Points · (empty or reuse total users)

#### 5. Users Management

Render a table below the stat sections.

**Columns:**

- Email
- Tier
- Active
- Superuser
- Actions

**Tier control:**

- Use a `<select>` or segmented control with `free`, `supporter`, `unlimited`
- On change, call `PATCH /api/v2/admin/users/{user_id}/tier`
- Optimistically disable the control while request is in flight
- After success, either update the row locally or call `router.refresh()`

**Pagination:**

- Basic next/prev buttons are enough
- Keep first version simple: page size `20`
- Use `skip = page * 20`

### Refresh Button
A client-side `<RefreshButton />` component that calls `router.refresh()` (Next.js App Router). Shown top-right of the page. No polling.

---

## Components to Build

```
app/admin/page.tsx              ← server component, fetches stats, renders sections
app/admin/layout.tsx            ← superuser guard + minimal shell
components/admin/UsersTable.tsx ← tier controls + pagination UI
components/admin/StatsCard.tsx  ← props: label, value, color? ("green"|"red"|"orange")
components/admin/SectionGrid.tsx← 4-col (or 2-col) responsive grid wrapper
components/admin/RefreshButton.tsx ← client component, calls router.refresh()
lib/api/admin.ts                ← getAdminStats(), getAdminUsers(), updateAdminUserTier()
```

---

## Error States

| Scenario | Behaviour |
|---|---|
| Not logged in | `layout.tsx` redirects to `/login` |
| Logged in but not superuser | `layout.tsx` redirects to `/` |
| API returns error | Show an inline error banner, not a full crash |
| Qdrant disconnected | `system.qdrant = "disconnected"` — show red card, page still loads |
| Tier update fails | Revert the control to previous value and show inline toast/banner |

---

## Design Notes

- No charts needed — stat cards only
- Color coding: green = healthy, red = down/error, orange = warning (users at limit)
- RTL not required for this page (internal tool, English labels are fine)
- Mobile-friendly is nice-to-have, not required
- No sidebar nav needed — this is a standalone ops page
- Sort newest UX work later if needed; first pass can use backend default ordering
