# Event-Driven Order & Fulfillment System (Go Architecture)

A high-performance, distributed event-driven order-processing platform modeling production
e-commerce backends: independent Go microservices communicating **asynchronously** over RabbitMQ,
coordinating multi-step workflows with the **saga pattern**, and ensuring correctness under failures,
retries, and duplicate deliveries — complete with an operations dashboard and Prometheus/Grafana observability.

---

## Architecture & System Design Highlights

- **Choreographed Microservices** — 5 independent Go services communicating via RabbitMQ topic exchange (`orders.topic`).
- **Saga Pattern & Compensating Transactions** — Order service acts as coordinator; failure in any leg (inventory/payment) triggers automatic compensating rollbacks (stock release / refund).
- **Transactional Outbox Pattern** — Dual-writes avoided via atomic SQL transactions (`orders`, `order_items`, `order_events`, `outbox`) and background relay with `SELECT ... FOR UPDATE SKIP LOCKED`.
- **Idempotent Consumers** — PostgreSQL `processed_messages` ledger guarantees exactly-once processing semantics over at-least-once delivery.
- **Reliability & Dead-Letter Queue (DLQ)** — Exponential backoff with jitter, automated retry queues (`<queue>.retry`), dead-letter queues (`<queue>.dlq`), and DLQ replay endpoint.
- **Type-Safe Persistence** — `sqlc` generates type-safe Go database client code with `pgx/v5` connection pooling.
- **Observability** — Native Prometheus `/metrics` endpoints, Grafana dashboards, structured JSON logging with correlation ID propagation.
- **Containerization & Orchestration** — Multi-stage Alpine Dockerfiles (< 15MB images), Docker Compose local stack, and Kubernetes manifests with HPA.

---

## Microservices Architecture

```
        ┌────────────────────┐
        │  Next.js Dashboard │  (orders, timeline, health, DLQ replay)
        └─────────┬──────────┘
                  │ HTTP (BFF)
        ┌─────────▼──────────┐   order.created
        │   Order Service    │──────────────┬──────────────┐
        │  · REST API        │              ▼              ▼
        │  · Saga Coordinator│      ┌──────────────┐ ┌──────────────┐
        │  · Outbox Relay    │      │  Inventory   │ │   Payment    │
        └────▲───────────────┘      │   Service    │ │   Service    │
             │ inventory.reserved   └──────┬───────┘ └──────┬───────┘
             │ payment.succeeded           │                │
             │ inventory.rejected   ┌──────▼────────────────▼──────┐
             │ payment.failed       │  RabbitMQ Topic Exchange     │
             │ shipment.*           │  "orders.topic" (+retry/DLQ) │
             └──────────────────────┴──────┬────────────────┬──────┘
                                            ▼                ▼
                                   ┌──────────────┐ ┌────────────────┐
                                   │  Shipping    │ │  Notification  │
                                   │  Service     │ │    Service     │
                                   └──────────────┘ └────────────────┘

  PostgreSQL (one DB per service) · Redis · Prometheus + Grafana
```

**Order Lifecycle:** `PENDING → CONFIRMED → SHIPPED → DELIVERED`, or `PENDING → CANCELLED` (with compensation). The Order service confirms only once **both** the inventory and payment legs succeed; if either fails it emits `order.cancelled`, triggering downstream compensations.

---

## Tech Stack

| Concern | Technology |
|---|---|
| Microservices | Go (1.25+), Chi v5 HTTP router, slog structured logging |
| Messaging | RabbitMQ — durable topic exchange, per-queue retry + DLQ |
| Data Layer | PostgreSQL 16 (isolated DB per service), `sqlc` + `pgx/v5` |
| Idempotency | Redis distributed locking + SQL `processed_messages` ledger |
| Operations Dashboard | Next.js (App Router), Tailwind CSS, TanStack Query |
| Observability | Prometheus + Grafana, `/metrics` endpoints, correlation ID tracing |
| Tooling | Go Workspaces (`go.work`), `golangci-lint`, Testcontainers-Go, Make |

---

## Quickstart (Docker Compose)

**Prerequisites:** Docker Engine 24+ & Docker Compose v2.

```bash
# Build and run the entire stack (services, databases, queue, dashboard, monitoring)
docker compose up --build
```

| Service | Port / URL | Description |
|---|---|---|
| Operations Dashboard | `http://localhost:3000` | Real-time order monitoring, catalog, ops health, DLQ replay |
| Order Service | `http://localhost:4001` | REST API, Saga Coordinator, Outbox Relay, Ops BFF |
| Inventory Service | `http://localhost:4002` | Catalog & reservation management |
| Payment Service | `http://localhost:4003` | Payment processing simulation & refund compensation |
| Shipping Service | `http://localhost:4004` | Shipment dispatch and delivery simulation |
| Notification Service | `http://localhost:4005` | Event consumer and multi-channel dispatch |
| RabbitMQ Management | `http://localhost:15672` | RabbitMQ UI (`guest` / `guest`) |
| Prometheus | `http://localhost:9090` | Metrics scraping and query console |
| Grafana | `http://localhost:3001` | Dashboards (`admin` / `admin`) |

---

## Testing Scenarios

### 1. Happy Path Saga
Submit a new order via the dashboard or `curl`:
```bash
curl -s -X POST http://localhost:4001/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust_01","items":[{"productId":"prod-espresso","sku":"ESP-01","quantity":1,"unitPrice":24.0}]}'
```
Inspect status progression:
```bash
curl -s http://localhost:4001/api/orders/<ORDER_ID>
```
Progression: `PENDING` → `CONFIRMED` → `SHIPPED` → `DELIVERED`.

### 2. Simulated Out-of-Stock Compensation
```bash
curl -s -X POST http://localhost:4001/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust_02","items":[{"productId":"prod-espresso","sku":"ESP-01","quantity":1,"unitPrice":24.0}],"simulate":{"outOfStock":true}}'
```
Result: Order transitions to `CANCELLED` (`"cancellationReason": "inventory rejected: simulated out-of-stock"`), and payment is automatically refunded/compensated.

### 3. Simulated Payment Failure Compensation
```bash
curl -s -X POST http://localhost:4001/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust_03","items":[{"productId":"prod-espresso","sku":"ESP-01","quantity":1,"unitPrice":24.0}],"simulate":{"paymentFailure":true}}'
```
Result: Order transitions to `CANCELLED` (`"cancellationReason": "payment failed: card declined"`), and held inventory is automatically released.

---

## Local Development & Tooling

```bash
# Code generation for SQL queries
make sqlc

# Format & lint all modules
make lint

# Run all unit tests with Go race detector
make test

# Build all Go service binaries into bin/
make build

# Run integration tests against real RabbitMQ via Testcontainers
cd tests && RUN_INTEGRATION=1 go test -v ./...
```

---

## Repository Layout

```
├── go.work                         # Go multi-module workspace
├── Makefile                        # Build, test, lint, and run orchestration
├── pkg/                            # Shared core library
│   ├── bus/                        # RabbitMQ MessageBus (retry, DLQ, replay)
│   ├── events/                     # Strongly-typed generic event envelopes
│   ├── idempotency/                # Distributed locks (Redis) & SQL ledger
│   ├── logger/                     # Slog structured logging with correlation IDs
│   └── metrics/                    # Prometheus metrics collectors & handler
├── services/
│   ├── order-service/              # REST API, Saga Coordinator, Outbox Relay
│   ├── inventory-service/          # Catalog & stock reservation
│   ├── payment-service/            # Payment simulation & refund compensation
│   ├── shipping-service/           # Dispatch & delayed delivery simulation
│   └── notification-service/       # Multi-event consumer & notification dispatch
├── dashboard/                      # Next.js operations console
├── infra/                          # PostgreSQL init, Prometheus & Grafana configs
├── k8s/                            # Kubernetes deployment & HPA manifests
└── tests/                          # Testcontainers-Go integration tests
```
