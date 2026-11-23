.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: tidy
tidy: ## Sync Go workspace and tidy all modules
	go work sync
	@for s in pkg services/order-service services/inventory-service services/payment-service services/shipping-service services/notification-service tests; do \
		(cd $$s && go mod tidy) || exit $$?; \
	done

.PHONY: lint
lint: ## Run golangci-lint across all modules
	golangci-lint run ./pkg/... ./services/order-service/... ./services/inventory-service/... ./services/payment-service/... ./services/shipping-service/... ./services/notification-service/... ./tests/...

.PHONY: test
test: ## Run unit tests across all modules
	go test -v -race ./pkg/... ./services/order-service/... ./services/inventory-service/... ./services/payment-service/... ./services/shipping-service/... ./services/notification-service/... ./tests/...

.PHONY: sqlc
sqlc: ## Generate type-safe Go code from SQL queries
	sqlc generate

.PHONY: build
build: ## Build all service binaries
	@mkdir -p bin
	@for s in order-service inventory-service payment-service shipping-service notification-service; do \
		if [ -d "services/$$s" ]; then \
			echo "Building $$s..."; \
			go build -o bin/$$s ./services/$$s/cmd/server || exit $$?; \
		fi \
	done

.PHONY: run
run: ## Start local infrastructure and services via Docker Compose
	docker compose up --build
