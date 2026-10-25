COMPOSE ?= docker compose
PROFILES ?= all
PROFILE_FLAGS := $(foreach p,$(PROFILES),--profile $(p))

.DEFAULT_GOAL := help
.PHONY: help up down ps logs reset config lint fmt test

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-10s %s\n", $$1, $$2}'

up: ## Start services. PROFILES=ingest|lake|stream|orchestration|all (default: all)
	$(COMPOSE) $(PROFILE_FLAGS) up -d --build

down: ## Stop all services (keeps volumes)
	$(COMPOSE) --profile all down --remove-orphans

ps: ## List running services
	$(COMPOSE) --profile all ps

logs: ## Follow logs. SERVICE=<name> to filter
	$(COMPOSE) --profile all logs -f --tail=100 $(SERVICE)

reset: ## Stop everything and delete all volumes
	$(COMPOSE) --profile all down -v --remove-orphans

config: ## Validate the Compose file for every profile
	$(COMPOSE) --profile all config --quiet

lint: ## Lint Python code
	ruff check .

fmt: ## Auto-format Python code
	ruff format .

test: ## Run unit tests
	pytest -q
