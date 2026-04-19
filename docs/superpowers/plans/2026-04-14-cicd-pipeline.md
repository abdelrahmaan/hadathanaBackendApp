# CI/CD Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set up a GitHub Actions CI pipeline that runs ruff linting and pytest on every PR to `main`, blocking merge on failure.

**Architecture:** Single workflow file (`.github/workflows/ci.yml`) with two sequential jobs — `lint` (ruff) then `test` (pytest). Ruff config lives in `pyproject.toml`. Pytest and pytest-asyncio added to `requirements.txt`.

**Tech Stack:** GitHub Actions, ruff (linting), pytest + pytest-asyncio (testing), Python 3.11

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `.github/workflows/ci.yml` | Pipeline definition — triggers, jobs, steps |
| Create | `pyproject.toml` | Ruff linting configuration |
| Modify | `requirements.txt` | Add pytest and pytest-asyncio |

---

## Task 1: Add test dependencies to `requirements.txt`

**Files:**
- Modify: `requirements.txt`

The CI test job runs `pip install -r requirements.txt` then `pytest`. Without pytest in the file, the job crashes immediately with "pytest: command not found".

- [ ] **Step 1: Add pytest dependencies**

Open `requirements.txt` and append these two lines at the end (after the `tqdm` line):

```
# Testing
pytest>=7.0
pytest-asyncio>=0.23
httpx>=0.27.0
```

(`httpx` is needed by the existing `conftest.py` which imports `AsyncClient` from `httpx`.)

- [ ] **Step 2: Verify tests run locally**

```bash
/home/abdo_kamar/Projects/.venv/bin/python -m pytest tests/ -v
```

Expected output: all tests pass (look for green dots or `PASSED` next to each test name). If any fail, fix before continuing — CI must start from a green baseline.

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: add pytest, pytest-asyncio, httpx to requirements"
```

---

## Task 2: Create `pyproject.toml` with ruff configuration

**Files:**
- Create: `pyproject.toml`

Ruff reads its configuration from `pyproject.toml` automatically. Without this file, ruff uses defaults which may flag issues in existing code we don't want to fix right now.

- [ ] **Step 1: Verify ruff is installed locally**

```bash
pip install ruff
ruff --version
```

Expected: prints something like `ruff 0.x.x`

- [ ] **Step 2: Create `pyproject.toml`**

Create `/home/abdo_kamar/Projects/hadathanaBackendApp/pyproject.toml` with this exact content:

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

- [ ] **Step 3: Run ruff locally and see what it reports**

```bash
ruff check .
```

This will likely report some violations in existing code. **Do not fix them all blindly.** Instead, note how many there are. If there are more than ~20, we will add path exclusions in the next step to focus ruff on `app/` and `tests/` only (the code we own and maintain).

- [ ] **Step 4: If ruff reports violations in generated/migration/script files, scope ruff to app code**

If `ruff check .` reports many errors in files like `mongo_migration/`, `extract_data_v2/`, `scripts/`, `Hadith_split/`, add an `exclude` list to `pyproject.toml`:

```toml
[tool.ruff]
target-version = "py311"
line-length = 88
exclude = [
    "mongo_migration/",
    "extract_data_v2/",
    "scripts/",
    "Hadith_split/",
    "tarajm/",
    "*.py",           # root-level one-off scripts
]
# Keep linting app/ and tests/ — the code we actively maintain

[tool.ruff.lint]
select = [
    "E",
    "F",
    "I",
]
ignore = [
    "E501",
]
```

Then re-run `ruff check app/ tests/` — this should be a manageable set of violations.

- [ ] **Step 5: Fix any ruff violations in `app/` and `tests/`**

Run:
```bash
ruff check app/ tests/ --fix
```

Ruff will auto-fix import ordering (`I` rules) and some style issues. For any remaining violations it cannot auto-fix, open the file and fix manually. Re-run until clean:

```bash
ruff check app/ tests/
# Expected: no output (exit code 0 means clean)
```

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml app/ tests/
git commit -m "chore: add ruff config and fix existing lint violations"
```

---

## Task 3: Create the GitHub Actions workflow

**Files:**
- Create: `.github/workflows/ci.yml`

This is the pipeline itself. GitHub reads any `.yml` file in `.github/workflows/` automatically.

- [ ] **Step 1: Create `.github/workflows/ci.yml`**

Create the file with this exact content:

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
        run: ruff check app/ tests/

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

**What each part does (learning notes):**

| Line | What it means |
|------|---------------|
| `on: pull_request: branches: [main]` | Trigger: runs when a PR targets `main` (opened, updated, or reopened) |
| `runs-on: ubuntu-latest` | GitHub spins up a fresh Ubuntu VM for this job |
| `uses: actions/checkout@v4` | Pre-built action: clones your repo onto the VM. `@v4` = pinned version |
| `uses: actions/setup-python@v5` | Pre-built action: installs Python 3.11 on the VM |
| `run: pip install ruff` | Shell command — just like your terminal |
| `needs: lint` | Job dependency: `test` waits for `lint` to succeed before starting |
| `env:` | Environment variables injected into that step — tests read these via `app/config.py` |

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "feat: add GitHub Actions CI pipeline (lint + test)"
```

---

## Task 4: Trigger and verify the pipeline on GitHub

**Files:** none — this is verification only

- [ ] **Step 1: Push your branch to GitHub**

```bash
git push origin main
# or if on a feature branch:
git push origin <your-branch-name>
```

- [ ] **Step 2: Open a Pull Request to `main` on GitHub**

Go to your repo on GitHub → "Pull requests" → "New pull request". If you pushed directly to main, create a test branch first:

```bash
git checkout -b test/ci-verification
git push origin test/ci-verification
```

Then open a PR from `test/ci-verification` → `main`.

- [ ] **Step 3: Watch the CI run**

On the PR page, click the "Checks" tab. You should see:
- `CI / lint` — running or queued
- `CI / test` — waiting (grey) until lint passes

Click into the `lint` job → expand each step → read the output. This is how you debug CI failures.

- [ ] **Step 4: Verify both jobs pass**

Both `lint` and `test` should show a green checkmark. The PR will show "All checks passed".

- [ ] **Step 5: Intentionally break it to learn failure mode**

On your branch, add an unused import to `app/main.py`:

```python
import os  # add this line if it's not already there and unused
```

Push the change:
```bash
git add app/main.py
git commit -m "test: intentional lint violation"
git push origin test/ci-verification
```

Watch CI fail on the `lint` job. Read the ruff error in the logs. Then revert:
```bash
git revert HEAD
git push origin test/ci-verification
```

Watch CI go green again. This is the core CI feedback loop you'll use every day.

- [ ] **Step 6: (Optional) Enable branch protection**

In GitHub → your repo → Settings → Branches → "Add branch protection rule":
- Branch name pattern: `main`
- Check: "Require status checks to pass before merging"
- Search for and add: `lint` and `test`
- Check: "Require branches to be up to date before merging"
- Save

This makes CI a hard gate — you physically cannot merge a PR with failing checks.

---

## Self-Review Notes

**Spec coverage check:**
- ✅ `.github/workflows/ci.yml` — Task 3
- ✅ `pyproject.toml` ruff config — Task 2
- ✅ `requirements.txt` pytest deps — Task 1
- ✅ Trigger on PR to main — Task 3 Step 1 YAML
- ✅ Sequential lint → test — `needs: lint` in Task 3
- ✅ Verification steps — Task 4
- ✅ Learning explanations — inline in each task

**No placeholders:** All steps have exact commands, exact file content, and expected output.

**Type consistency:** No function signatures — N/A for this plan.
