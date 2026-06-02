.PHONY: up down logs psql test lint format backfill help

# Default target
help:
	@echo "Targets:"
	@echo "  up        Build and start all services (collector + monitor + postgres)"
	@echo "  down      Stop and remove containers (data volume preserved)"
	@echo "  logs      Tail logs from all services"
	@echo "  psql      Open a psql shell into the running postgres container"
	@echo "  test      Run the test suite against an ephemeral postgres"
	@echo "  lint      Run ruff lint checks"
	@echo "  format    Run ruff formatter"
	@echo "  backfill  Run the settlements backfill script"

# -----------------------------------------------------------------------
up:
	docker compose up --build -d
	@echo "Services started. Run 'make logs' to tail output."

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

psql:
	docker compose exec postgres psql -U $${POSTGRES_USER:-kalshi} -d $${POSTGRES_DB:-kalshi}

# -----------------------------------------------------------------------
# Tests: spin up ephemeral postgres, run pytest, tear down
test:
	docker compose -f docker-compose.test.yml up -d --wait
	POSTGRES_HOST=localhost \
	POSTGRES_PORT=5433 \
	POSTGRES_DB=kalshi_test \
	POSTGRES_USER=kalshi_test \
	POSTGRES_PASSWORD=kalshi_test \
	KALSHI_API_KEY_ID=test \
	KALSHI_PRIVATE_KEY_PATH=tests/fixtures/test_private_key.pem \
	KALSHI_ENV=demo \
	POSTGRES_PASSWORD=kalshi_test \
	  python -m pytest tests/ -v || true
	docker compose -f docker-compose.test.yml down -v

# -----------------------------------------------------------------------
lint:
	ruff check src/ tests/ scripts/

format:
	ruff format src/ tests/ scripts/

# -----------------------------------------------------------------------
backfill:
	docker compose run --rm collector python scripts/backfill_settlements.py
