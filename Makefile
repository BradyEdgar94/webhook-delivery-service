# Hooklane developer commands. `make help` lists them.
# Only Docker is needed for up/down/logs/demo/load*/chaos*.
# `test` and `lint` also need a Rust toolchain (see README).

SHELL := /bin/bash
COMPOSE := docker compose
LOAD_EVENTS ?= 100000
LOAD_ENDPOINTS ?= 100
LOAD_RATE ?= 2000      # steady target; ~1750/s is this laptop's measured ceiling
SPIKE_RATE ?= 200      # spike base rate; the spike is 10x this
CHAOS_RATE ?= 1500     # chaos tests run below the ceiling
WORKERS ?= 2

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- stack ----

.env:
	@cp .env.example .env
	@KEY=$$( (openssl rand -base64 32 2>/dev/null) || (head -c 32 /dev/urandom | base64) ); \
	  sed -i.bak "s|^SECRET_ENCRYPTION_KEY=.*|SECRET_ENCRYPTION_KEY=$$KEY|" .env && rm -f .env.bak
	@echo "created .env with a fresh SECRET_ENCRYPTION_KEY"

# PREBUILT=1 pulls the image published by CI instead of compiling it locally
# (the first local build takes ~10 minutes).
PREBUILT_IMAGE ?= ghcr.io/bradyedgar94/webhook-delivery-service:latest

.PHONY: up
up: .env ## Build and start the whole stack. PREBUILT=1 pulls the published image instead of building
ifdef PREBUILT
	docker pull $(PREBUILT_IMAGE)
	HOOKLANE_IMAGE=$(PREBUILT_IMAGE) $(COMPOSE) up -d --no-build --wait --scale worker=$(WORKERS)
else
	$(COMPOSE) up -d --build --wait --scale worker=$(WORKERS)
endif
	@$(MAKE) --no-print-directory urls

.PHONY: down
down: ## Stop the stack and delete its volumes
	$(COMPOSE) down -v --remove-orphans

.PHONY: logs
logs: ## Tail logs of every service
	$(COMPOSE) logs -f --tail=100

.PHONY: urls
urls:
	@echo ""
	@echo "  API               http://localhost:8080   (healthz: /healthz)"
	@echo "  Grafana           http://localhost:3000   (dashboard: Hooklane)"
	@echo "  Jaeger            http://localhost:16686"
	@echo "  Redpanda Console  http://localhost:8081"
	@echo "  Prometheus        http://localhost:9090"
	@echo "  Sink stats        http://localhost:8090/_admin/stats"
	@echo ""

.PHONY: demo
demo: up ## Start the stack, create a tenant + 3 endpoints, send 100 events, show a summary (PREBUILT=1 to skip the build)
	./scripts/demo.sh

# ---------------------------------------------------------------- quality ----

.PHONY: lint
lint: ## cargo fmt --check and clippy with warnings as errors
	cargo fmt --all -- --check
	cargo clippy --all-targets -- -D warnings

.PHONY: test
test: test-unit test-integration ## Unit tests, then integration tests (starts Postgres + Redpanda itself)

.PHONY: test-unit
test-unit: ## Unit tests only (no external services)
	cargo test --workspace --lib --bins

.PHONY: test-deps
test-deps: .env
	$(COMPOSE) up -d --wait postgres redpanda

.PHONY: test-integration
test-integration: test-deps ## Integration tests against real Postgres and Redpanda
	TEST_DATABASE_URL=postgres://hooklane:hooklane@localhost:55432/postgres \
	TEST_KAFKA_BROKERS=localhost:19092 \
	cargo test -p hooklane --test '*' -- --test-threads=4

# ---------------------------------------------------------------- load ----

.PHONY: load
load: ## Steady load test (default ~100k events) + loadcheck report
	./loadtest/run.sh steady --events $(LOAD_EVENTS) --endpoints $(LOAD_ENDPOINTS) --rate $(LOAD_RATE)

.PHONY: load-full
load-full: ## Steady load test at 1M events
	./loadtest/run.sh steady --events 1000000 --endpoints 1000 --rate $(LOAD_RATE)

.PHONY: load-spike
load-spike: ## 10x spike, then check consumer lag drains
	./loadtest/run.sh spike --events $(LOAD_EVENTS) --endpoints $(LOAD_ENDPOINTS) --rate $(SPIKE_RATE)

.PHONY: chaos-worker
chaos-worker: ## SIGKILL worker containers during a run
	./loadtest/run.sh chaos-worker --events $(LOAD_EVENTS) --endpoints $(LOAD_ENDPOINTS) --rate $(CHAOS_RATE)

.PHONY: chaos-endpoints
chaos-endpoints: ## 30% of endpoints fail (5xx / timeout / 429)
	./loadtest/run.sh chaos-endpoints --events $(LOAD_EVENTS) --endpoints $(LOAD_ENDPOINTS) --rate $(CHAOS_RATE)

.PHONY: chaos-db
chaos-db: ## Restart Postgres during a run
	./loadtest/run.sh chaos-db --events $(LOAD_EVENTS) --endpoints $(LOAD_ENDPOINTS) --rate $(CHAOS_RATE)

.PHONY: load-scale
load-scale: ## Scale workers up and down mid-run (docker compose --scale)
	./loadtest/run.sh scale --events 40000 --endpoints $(LOAD_ENDPOINTS) --rate 1000

.PHONY: load-compare
load-compare: ## Same steady load in postgres mode and kafka mode
	./loadtest/run.sh compare --events $(LOAD_EVENTS) --endpoints $(LOAD_ENDPOINTS) --rate $(CHAOS_RATE)

# ---------------------------------------------------------------- k8s ----

.PHONY: k8s-up
k8s-up: ## kind cluster + KEDA + metrics-server + Hooklane
	./deploy/k8s/kind-up.sh

.PHONY: k8s-demo
k8s-demo: ## Burst of events; watch workers scale on consumer lag
	./deploy/k8s/kind-demo.sh

.PHONY: k8s-down
k8s-down: ## Delete the kind cluster
	kind delete cluster --name hooklane
