# Real-Time Log Analytics Platform

> A backend-only, event-driven log analytics pipeline built with Go, Kafka, Redis, Elasticsearch, and PostgreSQL.

[![Go Version](https://img.shields.io/badge/go-1.22+-00ADD8?style=flat&logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

---

## What is this?

A microservices-based log analytics platform that ingests application logs via HTTP, processes them through an asynchronous streaming pipeline, and serves real-time metrics and searchable historical data through a REST API.

**Key features:**
- ⚡ **Sub-10ms real-time metrics** via Redis data structures
- 🔍 **Full-text search** across millions of logs via Elasticsearch
- 📊 **Event-driven architecture** with Kafka as the central nervous system
- 🗄️ **Polyglot persistence** — PostgreSQL for metadata, ES for search, Redis for speed
- 🛠️ **Admin CLI** for service management and benchmarking
- 📈 **Load generator** with measured throughput and latency benchmarks

---

## Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────────────┐
│   Client    │─────▶│  INGESTOR   │─────▶│   KAFKA (KRaft)     │
│ (curl/k6/   │      │   :8081     │      │   app-logs topic    │
│  logctl)    │      │  (Go API)   │      │   3 partitions      │
└─────────────┘      └─────────────┘      └──────────┬──────────┘
                                                      │
                              ┌───────────────────────┘
                              ▼
                     ┌─────────────────┐
                     │ STREAM PROCESSOR│
                     │     (Go)        │
                     │                 │
                     │ • Consume Kafka │
                     │ • Bulk Index ES │
                     │ • Update Redis  │
                     │ • DLQ on fail   │
                     └────────┬────────┘
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
            ┌──────────────┐    ┌──────────────┐
            │Elasticsearch │    │    Redis     │
            │ logs-v1-*    │    │ • Counters   │
            │              │    │ • ZSets      │
            └──────┬───────┘    │ • Lists      │
                   │            │ • Sets       │
                   │            └──────┬───────┘
                   │                   │
                   └─────────┬─────────┘
                             ▼
                    ┌─────────────────┐
                    │  ANALYTICS API  │
                    │     :8082       │
                    │   (Go API)      │
                    └─────────────────┘
                             ▲
                    ┌────────┘
                    │
             ┌─────────────────┐
             │   PostgreSQL    │
             │  (Metadata)     │
             └─────────────────┘
```

**Read the full architecture decisions →** [`docs/02-architecture.md`](docs/02-architecture.md)

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Language | **Go 1.22+** | Services and CLI tools |
| Message Queue | **Apache Kafka (KRaft)** | Event streaming, decoupling |
| Search | **Elasticsearch 8.11** | Full-text log search |
| Cache / Real-time | **Redis 7** | Counters, leaderboards, TTL windows |
| Metadata | **PostgreSQL 15** | Relational config (apps, services, rules) |
| Visualization | **Kibana** | Debug and inspect indexed logs |
| Orchestration | **Docker Compose** | Local infrastructure |

---

## Quick Start

### Prerequisites

- [Go 1.22+](https://go.dev/dl)
- [Docker & Docker Compose](https://docs.docker.com/get-docker)

### 1. Clone & Start Infrastructure

```bash
git clone https://github.com/yourusername/log-platform.git
cd log-platform

docker compose -f deployments/docker-compose.yml up -d
```

### 2. Run Migrations & Seed Data

```bash
go run ./cmd/migrate
go run ./cmd/seed
```

### 3. Start Services (3 terminals)

```bash
# Terminal 1: Stream Processor
go run ./cmd/processor

# Terminal 2: Analytics API
go run ./cmd/analytics

# Terminal 3: Log Ingestor
go run ./cmd/ingestor
```

### 4. Send Your First Log

```bash
curl -X POST http://localhost:8081/api/v1/logs   -H "Content-Type: application/json"   -d '{
    "timestamp": "2026-08-06T10:00:00Z",
    "level": "ERROR",
    "service": "payment-api",
    "message": "DB connection timeout",
    "trace_id": "abc-123",
    "ip": "192.168.1.5"
  }'
```

### 5. Query Metrics

```bash
# Real-time metrics (sub-10ms)
curl http://localhost:8082/metrics/live | jq

# Search logs
curl "http://localhost:8082/search?q=timeout&service=payment-api" | jq

# View in Kibana: http://localhost:5601
```

**→ Full setup guide:** [`docs/06-runbook.md`](docs/06-runbook.md)

---

## Project Structure

```
log-platform/
├── cmd/
│   ├── ingestor/          # HTTP API :8081
│   ├── processor/         # Kafka consumer worker
│   ├── analytics/         # HTTP API :8082
│   ├── logctl/            # Admin CLI
│   ├── loadgen/           # Benchmark tool
│   ├── migrate/           # DB migrations
│   └── seed/              # Seed data
│
├── internal/
│   ├── config/            # 12-factor env config
│   ├── models/            # Go structs
│   ├── postgres/          # Connection, migrations, queries
│   ├── kafka/             # Producer, consumer, DLQ
│   ├── redis/             # Schema helpers
│   ├── elastic/           # Bulk indexer, search builder
│   └── telemetry/         # slog, request ID middleware
│
├── migrations/            # PostgreSQL .sql files
├── deployments/
│   └── docker-compose.yml
├── docs/                  # Architecture docs, ADRs, API spec
├── scripts/               # Helper scripts
└── README.md
```

---

## API Reference

### Ingestor (`:8081`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/logs` | Ingest a log (returns 202 Accepted) |
| `GET`  | `/health` | Health check |

### Analytics (`:8082`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/metrics/live` | Real-time metrics from Redis |
| `GET` | `/metrics/top-errors?n=5` | Top error messages |
| `GET` | `/search?q=&service=&level=` | Full-text search in ES |
| `GET` | `/services` | List services from PostgreSQL |

**→ Full API spec:** [`docs/03-api-spec.md`](docs/03-api-spec.md)

---

## CLI Tools

### `logctl` — Admin CLI

```bash
# Build
go build -o bin/logctl ./cmd/logctl

# Usage
./bin/logctl app create ecommerce "E-commerce Platform"
./bin/logctl service create ecommerce production payment-api
./bin/logctl service list
./bin/logctl search --service=payment-api --level=ERROR --last=1h
./bin/logctl benchmark --rate=1000 --duration=5m
./bin/logctl dlq inspect
```

### `loadgen` — Load Generator

```bash
go build -o bin/loadgen ./cmd/loadgen

# Basic run
./bin/loadgen --rate=500 --duration=60s

# Stress test
./bin/loadgen --rate=5000 --duration=10m --level=mixed
```

**→ Full CLI spec:** [`docs/03-api-spec.md`](docs/03-api-spec.md)

---

## Benchmarks

> ⚠️ **Measured on:** MacBook Air M2, 16GB RAM, Docker Compose (single node)

| Metric | Value |
|--------|-------|
| Sustained ingestion | _ logs/sec *(measure after building)* |
| p50 ingestion latency | _ ms |
| p99 ingestion latency | _ ms |
| Metrics API latency | _ ms |
| Search API latency (last 1h) | _ ms |
| ES bulk flush size | 100 docs |
| ES bulk flush interval | 5s |
| Test duration | 5 minutes |

**Run benchmarks:**
```bash
./bin/loadgen --rate=1000 --duration=300s
```

**→ Benchmark methodology:** [`docs/06-runbook.md`](docs/06-runbook.md)

---

## Key Design Decisions

| Decision | Why |
|----------|-----|
| **Kafka KRaft** (no ZooKeeper) | One less moving part; modern default |
| **Polyglot persistence** | Right database for each access pattern |
| **Redis data structures** | Sorted Sets for rankings, Lists for queues, Sets for dedup |
| **ES versioned indices** | `logs-v1-*` enables schema evolution without reindexing |
| **Bulk indexing** | 100 docs / 5s flush = ~100x throughput vs single-document |
| **DLQ as Kafka topic** | Same infrastructure, replayable, no head-of-line blocking |
| **No frontend** | 100% backend focus; CLI tools demonstrate API design |
| **No Kubernetes** | Docker Compose is sufficient for learning; K8s is Phase 2 |

**→ Full ADRs:** [`docs/02-architecture.md`](docs/02-architecture.md)

---

## Documentation

| Document | What's Inside |
|----------|---------------|
| [`docs/01-scope.md`](docs/01-scope.md) | Project charter, timeline, success criteria |
| [`docs/02-architecture.md`](docs/02-architecture.md) | 13 ADRs with trade-offs and consequences |
| [`docs/03-api-spec.md`](docs/03-api-spec.md) | HTTP + CLI contracts, request/response examples |
| [`docs/04-data-model.md`](docs/04-data-model.md) | PostgreSQL schema, Redis keys, ES mapping, Kafka topics |
| [`docs/05-sequence-diagrams.md`](docs/05-sequence-diagrams.md) | Data flows: happy path, failures, graceful shutdown |
| [`docs/06-runbook.md`](docs/06-runbook.md) | Setup, common commands, troubleshooting |
| [`docs/07-development-log.md`](docs/07-development-log.md) | Weekly journal (maintained during development) |
| [`docs/08-benchmarks.md`](docs/08-benchmarks.md) | Empirical benchmark results & bottleneck analysis |

---

## Roadmap

### ✅ Phase 1 — MVP (Current)
- [x] Log ingestion via HTTP API
- [x] Kafka event streaming
- [x] Elasticsearch bulk indexing
- [x] Redis real-time metrics
- [x] PostgreSQL metadata
- [x] Dead Letter Queue
- [x] Admin CLI & load generator
- [x] Docker Compose local deployment

### 🔮 Phase 2 — Production Hardening
- [ ] Index Lifecycle Management (ILM) for Elasticsearch
- [ ] Alert evaluation engine
- [ ] Kafka replay from DLQ
- [ ] Structured log enrichment (IP → geo)

### 🔮 Phase 3 — Observability
- [ ] Prometheus metrics export
- [ ] Grafana dashboards
- [ ] OpenTelemetry distributed tracing

### 🔮 Phase 4 — Scale
- [ ] Kubernetes manifests
- [ ] Horizontal Pod Autoscaling
- [ ] Multi-tenant support

---

## Contributing

This is a personal learning project. However, if you spot bugs or have suggestions:

1. Open an issue describing the problem
2. Fork the repo and create a branch
3. Make your changes with tests
4. Submit a pull request

---

## License

[MIT](LICENSE)

---

> *Built to learn event-driven architecture, polyglot persistence, and Go microservices — one log at a time.*
