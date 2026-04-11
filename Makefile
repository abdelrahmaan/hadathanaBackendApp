# Makefile — Shorthand commands for dev/prod Docker stacks
#
# Both stacks can run simultaneously on the same machine:
#   Dev:  port 8001 (API), port 27017 (MongoDB)
#   Prod: port 8000 (API), MongoDB internal-only

PROD_COMPOSE = docker compose -p hadathana-prod -f docker-compose.yml -f docker-compose.prod.yml

# ── Dev (auto-loads docker-compose.override.yml) ──────────────

.PHONY: dev dev-logs dev-down dev-ps

dev:
	docker compose up -d

dev-logs:
	docker compose logs -f api

dev-down:
	docker compose down

dev-ps:
	docker compose ps

# ── Prod (explicit compose files + project name) ─────────────

.PHONY: prod prod-logs prod-down prod-ps prod-restart

prod:
	$(PROD_COMPOSE) up -d --build

prod-logs:
	$(PROD_COMPOSE) logs -f api

prod-down:
	$(PROD_COMPOSE) down

prod-ps:
	$(PROD_COMPOSE) ps

prod-restart:
	$(PROD_COMPOSE) restart api

# ── Tests ─────────────────────────────────────────────────────
#
# Unit tests (mocked — no server needed):
#   make test-chatbot
#
# Integration / smoke tests (require a live stack):
#   make test-chatbot-dev    → APP_ENV=dev  hits localhost:8001
#   make test-chatbot-prod   → APP_ENV=prod hits localhost:8000

PYTHON = /home/abdo_kamar/Projects/.venv/bin/python

.PHONY: test test-chatbot test-chatbot-dev test-chatbot-prod test-db-dev test-db-prod

test:
	$(PYTHON) -m pytest tests/ -v

test-chatbot:
	$(PYTHON) -m pytest tests/test_chatbot_v1.py -v

test-chatbot-dev:
	APP_ENV=dev $(PYTHON) -m pytest tests/test_chatbot_smoke.py -v

test-chatbot-prod:
	APP_ENV=prod $(PYTHON) -m pytest tests/test_chatbot_smoke.py -v

test-db-dev:
	APP_ENV=dev $(PYTHON) -m pytest tests/test_data_presence.py -v

test-db-prod:
	APP_ENV=prod $(PYTHON) -m pytest tests/test_data_presence.py -v

# ── Utilities ─────────────────────────────────────────────────

.PHONY: status health

status:
	@echo "=== Dev ===" && docker compose ps 2>/dev/null || true
	@echo ""
	@echo "=== Prod ===" && $(PROD_COMPOSE) ps 2>/dev/null || true

health:
	@echo "Dev  (8001):" && curl -s http://localhost:8001/health | python3 -m json.tool 2>/dev/null || echo "  not running"
	@echo ""
	@echo "Prod (8000):" && curl -s http://localhost:8000/health | python3 -m json.tool 2>/dev/null || echo "  not running"
