# Plan: GitHub Actions CI/CD → GHCR → Dokploy

## Context

**Problem:** Building Docker images directly on the VPS (via Dokploy) consumes high RAM/CPU and can freeze the server.

**Solution:** Offload the build to GitHub Actions (free GitHub infrastructure). Actions builds the image, pushes it to GHCR (GitHub Container Registry), then sends a webhook to Dokploy to pull the ready image and redeploy — the VPS never runs a build again.

**Flow:**
```
git push main → GitHub Actions → build image → push to GHCR → webhook → Dokploy pull & redeploy
```

---

## Files to Create/Modify

### New files:
- `.github/workflows/deploy.yml` — main CI/CD workflow

### Modified files:
- `docker-compose.prod.yml` — change `api` service image from build context to GHCR image reference
- `.env.example` — add `GHCR_IMAGE` comment/variable

---

## Implementation Steps

### Step 1: Update `docker-compose.prod.yml`

Instead of `build: .` (which builds on the VPS), use the pre-built GHCR image:

```yaml
# api service
api:
  image: ghcr.io/<github-username>/hadathana-backend:latest
  # remove or comment out: build: .
```

Note: `<github-username>` must be lowercase — GHCR is case-sensitive.

### Step 2: Create `.github/workflows/deploy.yml`

```yaml
name: Build & Deploy

on:
  push:
    branches: [main]

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}   # e.g. username/hadathanaBackendApp

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write   # required for GHCR push

    outputs:
      image_digest: ${{ steps.push.outputs.digest }}

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}   # built-in, no manual secret needed

      - name: Extract metadata (tags, labels)
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
          tags: |
            type=sha,prefix=sha-
            type=raw,value=latest,enable={{is_default_branch}}

      - name: Build and push
        id: push
        uses: docker/build-push-action@v5
        with:
          context: .
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=gha        # GitHub Actions cache — speeds up repeat builds
          cache-to: type=gha,mode=max

  deploy:
    needs: build-and-push
    runs-on: ubuntu-latest

    steps:
      - name: Trigger Dokploy redeploy
        run: |
          curl -s -o /dev/null -w "%{http_code}" \
            -X POST "${{ secrets.DOKPLOY_WEBHOOK_URL }}" \
            -H "Content-Type: application/json"
        # DOKPLOY_WEBHOOK_URL = Dokploy service webhook (Dokploy UI → service → Deployments → Webhook)
```

### Step 3: Add GitHub Secrets

Go to **GitHub repo → Settings → Secrets and variables → Actions** and add:

| Secret | Value | Note |
|--------|-------|------|
| `DOKPLOY_WEBHOOK_URL` | Webhook URL from Dokploy UI | Only manual secret needed |

> `GITHUB_TOKEN` is built-in automatically — no need to add it.

### Step 4: Configure Dokploy

In Dokploy UI for the service:
1. **Source** → select **Docker Image** (not GitHub/Dockerfile)
2. **Image**: `ghcr.io/<username>/hadathanabackendapp:latest`
3. **Registry credentials**: add GHCR credentials (GitHub username + PAT with `read:packages` scope)
4. **Deployments tab** → **Webhook URL** → copy the URL → paste into GitHub secret as `DOKPLOY_WEBHOOK_URL`

### Step 5: GitHub PAT for Dokploy (to read from GHCR)

Dokploy needs a token to pull private images from GHCR:
- GitHub → Settings → Developer Settings → Personal Access Tokens → Fine-grained
- Scope: `read:packages` only
- Add the PAT in Dokploy → Registry credentials

---

## Build Optimizations

- **BuildKit cache** (`cache-from: type=gha`) — subsequent builds are significantly faster
- **sha tag** (`sha-abc1234`) — every push gets a unique tag, easy rollback
- **`latest` tag** — Dokploy automatically pulls this on every deploy

---

## Verification

After setup is complete:
```bash
# 1. Push a commit to main and watch Actions run
git push origin main

# 2. Confirm image was built in GHCR
# GitHub → repo → Packages → hadathanabackendapp

# 3. Confirm Dokploy received the webhook and redeployed
# Dokploy UI → service → Deployments → check latest deployment

# 4. Test the live API
curl https://api.hadathana.app/health
```

---

## File Structure After Implementation

```
.github/
  workflows/
    deploy.yml          ← NEW
docker-compose.prod.yml ← MODIFIED (image: ghcr.io/... instead of build: .)
.env.example            ← MODIFIED (add GHCR_IMAGE comment)
```
