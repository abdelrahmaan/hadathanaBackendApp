# CI/CD Pipeline Design — hadathanaBackendApp

**Date:** 2026-04-14  
**Scope:** GitHub Actions CI pipeline (lint + test) on PRs to main  
**Goal:** Learn CI/CD concepts by building a real pipeline step by step

---

## Context

The project has no CI/CD configured. `.github/workflows/` exists but is empty. Tests exist in `tests/` using pytest, but pytest is missing from `requirements.txt`. No linting tools are configured. This design sets up the foundational CI pipeline: catch bugs automatically on every PR before code reaches `main`.

---

## Core Concepts

GitHub Actions spins up a fresh Ubuntu VM when a PR is opened or updated. It runs your defined steps in order, then reports pass/fail on the PR. The key guarantee: if it passes on CI, it passes on a clean system — not just your machine.

**Mental model:**
```
PR opened/updated targeting main
  └── Job: lint (~5-10s)
        ├── checkout code
        ├── install Python 3.11
        ├── install ruff
        └── ruff check .         ← fail = PR blocked
  └── Job: test (~30-60s)  [only runs if lint passes]
        ├── checkout code
        ├── install Python 3.11
        ├── install all dependencies
        └── pytest tests/ -v     ← fail = PR blocked
```

`needs: lint` is the dependency keyword — test waits for lint to succeed before starting.

---

## Files to Create

### `.github/workflows/ci.yml`

```yaml
name: CI

on:
  pull_request:
    branches:
      - main

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install ruff
        run: pip install ruff

      - name: Run ruff
        run: ruff check .

  test:
    needs: lint
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run tests
        run: pytest tests/ -v
        env:
          APP_ENV: dev
          MONGODB_URI_LOCAL: mongodb://localhost:27017/
          DB_NAME_DEV: HadithDataDev
```

### `pyproject.toml`

```toml
[tool.ruff]
target-version = "py311"
line-length = 88

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors (indentation, whitespace)
    "F",   # pyflakes (undefined names, unused imports — real bugs)
    "I",   # isort (import ordering)
]
ignore = [
    "E501",  # line too long — formatter handles this, not linter
]
```

**Rule rationale:**
- `E` — basic style correctness
- `F` — catches real bugs: undefined variables (F821), unused imports (F401)
- `I` — consistent import ordering, auto-fixable
- Excluded: `N` (naming), `ANN` (annotations), `S` (security) — too noisy on existing code for day one

---

## Files to Modify

### `requirements.txt`

Add at the end:
```
pytest>=7.0
pytest-asyncio>=0.23
```

These are needed because CI runs `pip install -r requirements.txt` before running `pytest`. Without them, the test job fails immediately.

---

## What You Will Learn

1. **Triggers** — `on: pull_request: branches: [main]` — how GitHub decides when to run CI
2. **Jobs vs Steps** — jobs are isolated VMs; steps are commands within one VM
3. **Pre-built Actions** — `uses: actions/checkout@v4` — reusable units of CI logic, versioned
4. **Job dependencies** — `needs: lint` — how to build sequential pipelines (DAGs)
5. **Environment variables in CI** — `env:` block — how secrets and config reach your code
6. **Reading CI logs** — when a job fails, GitHub shows the exact step and output

---

## What Is Out of Scope (Intentionally)

- Docker build verification — next natural step after this works
- Deployment steps — comes after Docker CI is stable
- Dependency caching (`actions/cache`) — speeds up CI, good follow-up
- Branch protection rules — set manually in GitHub repo settings (Settings → Branches → Add rule → Require status checks)

---

## Verification

After implementation:

1. Push this branch to GitHub and open a PR against `main`
2. Go to the PR → "Checks" tab — you should see the `CI` workflow running
3. Click into the lint job — verify ruff runs and reports any issues
4. Click into the test job — verify pytest output matches local `pytest tests/ -v`
5. Intentionally introduce a ruff violation (e.g., unused import) → confirm CI fails on lint
6. Fix it → confirm CI goes green

**Local pre-check before pushing:**
```bash
# Lint
pip install ruff
ruff check .

# Tests
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/ -v
```
