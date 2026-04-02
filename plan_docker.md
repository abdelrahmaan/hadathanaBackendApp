# Plan: Docker + docker-compose Setup

## Context

The project currently has a `Dockerfile` for the FastAPI app only — no `docker-compose.yml` exists. The user wants to clarify the right Docker strategy and add a `docker-compose.yml`.

Two environments exist, controlled by `APP_ENV`:
- **`dev`** → local Docker MongoDB (`mongodb-hadathana` container) + `HadithDataDev` + port 8001
- **`prod`** → Atlas MongoDB + `HadithData` + port 8000

---

## Recommended Strategy

| Environment | App | MongoDB | How to run |
|---|---|---|---|
| **dev** | `uvicorn` directly (or docker-compose) | Docker container (`mongodb-hadathana`) | `docker compose up` or manual uvicorn |
| **prod** | Docker image | Atlas (cloud) | `docker run` with env vars from platform |

**docker-compose**: Dev-only. Spins up `api` + `mongo` together, replacing the manual `docker run` + `uvicorn` steps. Reuses the existing `mongodb_hadathana_data` Docker volume (already has data).

**No docker-compose for prod**: Atlas is cloud — nothing to compose locally. Prod is deployed on Railway/Render which runs the Docker image directly with env vars from the platform dashboard.

---

## Changes

### 1. Fix `Dockerfile` — dynamic port via env var

Currently hardcodes port `8000`. Should respect `PORT` env var:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

EXPOSE ${PORT:-8000}

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
```

### 2. New `docker-compose.yml` (dev only)

```yaml
# docker-compose.yml — DEV only
# Usage: APP_ENV=dev docker compose up
# Reuses existing mongodb_hadathana_data volume (already has data imported)

services:
  api:
    build: .
    ports:
      - "8001:8001"
    environment:
      - PORT=8001
    env_file:
      - .env                  # APP_ENV=dev must be set here
    depends_on:
      mongo:
        condition: service_healthy
    volumes:
      - ./app:/app/app        # live reload in dev

  mongo:
    image: mongo:8
    ports:
      - "27017:27017"
    volumes:
      - mongodb_hadathana_data:/data/db
    healthcheck:
      test: ["CMD", "mongosh", "--eval", "db.adminCommand('ping')"]
      interval: 5s
      timeout: 5s
      retries: 5

volumes:
  mongodb_hadathana_data:
    external: true            # reuse existing volume — already has data
```

### 3. `.env.example` — add `PORT`

```bash
# ── Server port (optional, default: 8000 prod / 8001 dev) ──
PORT=8000
```

### 4. Update `CLAUDE.md` + `README.md`

Add `docker compose up` as the preferred dev startup option alongside the existing manual uvicorn command.

---

## Dev Workflow After This Change

```bash
# Option A — docker compose (new, recommended for dev)
# Make sure APP_ENV=dev is in .env first
docker compose up

# Option B — manual (still works, same as today)
docker start mongodb-hadathana
/home/abdo_kamar/Projects/.venv/bin/python -m uvicorn app.main:app \
  --host 0.0.0.0 --port 8001 --reload
```

## Prod Workflow (unchanged)

```bash
# Build image
docker build -t hadathna-api .

# Run (pass env vars at runtime — never bake into image)
docker run -p 8000:8000 \
  -e APP_ENV=prod \
  -e MONGODB_URI_READ="mongodb+srv://..." \
  -e DB_NAME=HadithData \
  -e CORS_ORIGINS="https://hadathana.app,https://www.hadathana.app" \
  hadathna-api
```

---

## Files to Create/Modify

| File | Change |
|---|---|
| `Dockerfile` | Replace hardcoded `8000` with `${PORT:-8000}` |
| `docker-compose.yml` | New — dev-only, `api` + `mongo` services |
| `.env.example` | Add `PORT` variable |
| `CLAUDE.md` | Add `docker compose up` to dev commands |
| `README.md` | Add docker-compose dev section |

---

## Verification

```bash
# Dev via compose
docker compose up
curl http://localhost:8001/health
curl http://localhost:8001/api/v2/topics

# Prod Docker build test
docker build -t hadathna-api .
docker run -p 8000:8000 -e APP_ENV=prod -e MONGODB_URI_READ="..." hadathna-api
curl http://localhost:8000/health
```
