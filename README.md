# LogLogger — Distributed Real-Time Log Analytics Platform

[![CI Pipeline](https://github.com/Rzmy7/logLogger/actions/workflows/ci.yml/badge.svg)](https://github.com/Rzmy7/logLogger/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/Go-1.24%2B-blue.svg)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED.svg?logo=docker&logoColor=white)](deployments/docker-compose.yml)

**LogLogger** is an end-to-end, high-throughput distributed log analytics and stream processing platform written in Go. Designed to ingest, process, store, and query high-volume application logs in real-time, the platform decouples HTTP ingestion from storage through Apache Kafka, indexes logs into Elasticsearch for full-text search, maintains sub-10ms real-time metric aggregations in Redis, isolates poison messages using a Kafka Dead Letter Queue (DLQ), and exposes unified REST query endpoints alongside full Prometheus and Grafana observability.

---

## 🏗️ Architecture & Data Flow

```mermaid
flowchart TD
    Client["Client / Log Producer"] -- "HTTP POST /api/v1/logs" --> Ingestor["Ingestor API (:8081)"]
    Ingestor -- "Publish to topic 'app-logs'" --> Kafka[("Apache Kafka (KRaft)")]
    
    Kafka -- "Consume 'app-logs'" --> Processor["Stream Processor (:8083)"]
    
    Processor -- "Valid Document" --> ES[("Elasticsearch\n(logs-v1-YYYY.MM.DD)")]
    Processor -- "Real-time Metrics" --> Redis[("Redis Metrics Store")]
    Processor -- "Malformed / Invalid" --> DLQ[("Kafka DLQ Topic\n(app-logs-dlq)")]
    
    Retention["Retention Service (:8084)\n(Periodic Runner)"] -- "Index Lifecycle\n(Delete Expired Indices)" --> ES
    
    Analytics["Analytics API (:8082)\n(/search, /metrics, /admin)"] -- "Full-Text Search" --> ES
    Analytics -- "Sub-10ms Aggregations" --> Redis
    Analytics -- "Admin Index Lifecycle" --> ES
    
    Prometheus[("Prometheus (:9090)")] -- "Scrape :8081, :8082, :8083, :8084" --> Ingestor
    Prometheus --> Processor
    Prometheus --> Analytics
    Prometheus --> Retention
    
    Grafana["Grafana Dashboard (:3000)"] -- "Visualize Telemetry" --> Prometheus
```

### Core Architecture Principles

1. **Storage Boundaries & Source of Truth:**
   - **Elasticsearch** is the sole source of truth for log documents, historical queries, and storage lifecycle.
   - **Redis** maintains derived real-time metrics (counters, 5m sliding error windows, leaderboards). Redis is **not** responsible for raw log lifecycle, and cumulative counters are not decremented when historical ES indices expire.
   - **PostgreSQL** is reserved strictly for future platform metadata (tenants, applications, alert rules) and is **not** in the log ingestion or processing data path.
2. **Lifecycle Safety & Interface Decoupling:**
   - Log lifecycle business logic is strictly encapsulated behind the `retention.Manager` interface.
   - Administrative deletions only operate on `logs-v1-YYYY.MM.DD` formatted indices and strictly reject non-log or system indices (`.kibana`, `.security`).
   - Today's active write index (`logs-v1-<today>`) is permanently protected from retention and manual deletion.
3. **No Unnecessary Dependencies:** The ingestion pipeline maintains direct, lightweight streaming via Kafka without heavy unneeded dependencies.

---

### End-to-End Pipeline Stages

1. **Ingestion Layer (`cmd/ingestor` on `:8081`)**:
   - Validates JSON payload schemas, timestamps (RFC3339), log levels, and IP addresses.
   - Publishes valid log events asynchronously to Kafka topic `app-logs` using trace/service partitioning keys.
   - Returns immediate `202 Accepted` with request metadata.
2. **Buffering & Decoupling (Apache Kafka)**:
   - Topic `app-logs` buffers bursts durably across partitions.
   - Consumer group `log-processors` tracks consumer offset advancement.
3. **Stream Processing Layer (`cmd/processor` on `:8083`)**:
   - Consumes events with strict manual offset commit semantics (offset committed only after sink writes succeed).
   - Routes permanently invalid/malformed payloads to `app-logs-dlq` with failure diagnostics.
   - Indexes valid documents into Elasticsearch daily indices (`logs-v1-YYYY.MM.DD`).
   - Atomically updates real-time counters, sliding 5-minute error windows, and sorted-set leaderboards in Redis.
4. **Analytics & Admin API (`cmd/analytics` on `:8082`)**:
   - Serves sub-10ms real-time metric aggregations and leaderboards from Redis.
   - Executes full-text search with boolean match filters and timestamp range filtering in Elasticsearch.
   - Exposes administrative log lifecycle endpoints (`/admin/logs/*`) for on-demand deletion and storage telemetry.
5. **Retention Management Service (`cmd/retention` on `:8084`)**:
   - Periodically identifies Elasticsearch log indices matching `logs-v1-YYYY.MM.DD` older than `LOG_RETENTION_DAYS`.
   - Safely deletes expired indices while strictly protecting today's active write index.
   - Exposes Prometheus metrics and structured execution telemetry.
6. **Observability (Prometheus `:9090` & Grafana `:3000`)**:
   - Pulls metrics every 2s across Ingestor, Processor, Analytics, and Retention services.
   - Visualizes ingestion rate, consumer lag, DLQ events, and storage latencies in a provisioned dashboard.

---

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| **Backend Language** | Go (Golang 1.24+) | High-performance, concurrent service binaries |
| **HTTP Routing** | Chi v5 | Lightweight, idiomatic HTTP routing & middleware |
| **Message Broker** | Apache Kafka 3.7 (KRaft) | Distributed log streaming, consumer groups, DLQ |
| **Search Engine** | Elasticsearch 8.11 | Full-text log search with strict index templates & lifecycle |
| **Retention Manager**| Dedicated Go daemon (`cmd/retention`) | Automated daily index retention & safe cleanup |
| **Metrics Cache** | Redis 7 / Upstash TLS | Real-time counters, sliding error windows, leaderboards |
| **Relational Store** | PostgreSQL 15 | Metadata, applications, services, alert rules |
| **Metrics Scraping** | Prometheus 2.51 | Time-series metric scraping and aggregation |
| **Dashboards** | Grafana 10.4 | Pre-provisioned visual operational telemetry |
| **Load Testing** | Custom Go CLI (`cmd/loadgen`) | Configurable rate-limiting, workers, and latency percentiles |

---

## 📂 Repository Structure

```text
logLogger/
├── .github/
│   └── workflows/
│       └── ci.yml                     # GitHub Actions CI workflow
├── cmd/
│   ├── analytics/                     # Analytics REST API (:8082)
│   ├── ingestor/                      # HTTP Ingestion Service (:8081)
│   ├── loadgen/                       # Benchmarking and Load Generator CLI
│   └── processor/                     # Stream Processor & DLQ Manager (:8083)
├── deployments/
│   ├── docker-compose.yml             # Kafka, ES, Redis, Postgres, Prometheus, Grafana
│   ├── grafana/                       # Auto-provisioned Grafana datasources & dashboards
│   └── prometheus/                    # Prometheus scrape targets config
├── docs/
│   ├── 01-scope.md                    # Project requirements & scope
│   ├── 02-architecture.md             # Architectural decisions & design
│   ├── 03-api-spec.md                 # REST API contracts & CLI commands
│   ├── 04-data-model.md               # Redis keys, Kafka topics, ES mappings
│   ├── 05-sequence-diagrams.md        # Pipeline execution sequence flows
│   ├── 06-runbook.md                  # Operations & deployment runbook
│   └── 08-benchmarks.md               # Empirical benchmark results & bottleneck analysis
├── internal/
│   ├── config/                        # Environment configuration loader
│   ├── elastic/                       # Elasticsearch v8 client & searcher
│   ├── kafka/                         # Kafka producer, consumer, & DLQ
│   ├── metrics/                       # Prometheus collectors & middleware
│   ├── models/                        # Core data models (LogMessage, DLQMessage)
│   └── redis/                         # Redis real-time metrics client
├── migrations/
│   └── 001_initial_schema.sql         # PostgreSQL schema migration
├── .env.example                       # Documented environment template
├── .gitignore                         # Build and secret ignore rules
├── go.mod                             # Go dependency module definition
├── go.sum                             # Go checksums
├── LICENSE                            # MIT License
└── README.md                          # Main project documentation
```

---

## 🚀 Getting Started

### 1. Prerequisites
- **Go**: Version `1.24` or later installed ([Download Go](https://go.dev/dl/))
- **Docker & Docker Compose**: Installed and running ([Download Docker Desktop](https://www.docker.com/products/docker-desktop/))
- **curl** or **Postman**: For testing API endpoints

### 2. Clone the Repository
```bash
git clone https://github.com/Rzmy7/logLogger.git
cd logLogger
```

### 3. Environment Configuration
Copy the example environment file and adjust if necessary:
```bash
cp .env.example .env
```

Default local `.env` values:
```env
POSTGRES_URL=postgresql://postgres:password@localhost:5432/log_analytics?sslmode=disable
REDIS_URL=redis://localhost:6379
KAFKA_BROKERS=localhost:9092
ELASTICSEARCH_URL=http://localhost:9200
HTTP_PORT=8081
ANALYTICS_PORT=8082
PROCESSOR_METRICS_PORT=8083
LOG_LEVEL=info
```

### 4. Start Infrastructure with Docker Compose
Start Kafka, Elasticsearch, Redis, PostgreSQL, Prometheus, and Grafana:
```bash
docker compose -f deployments/docker-compose.yml up -d
```

Verify running containers:
```bash
docker ps
```

| Service | Port | Health Check URL |
|---|---|---|
| **Kafka Broker** | `9092` | `localhost:9092` |
| **Elasticsearch** | `9200` | `http://localhost:9200` |
| **Redis** | `6379` | `localhost:6379` |
| **PostgreSQL** | `5432` | `localhost:5432` |
| **Prometheus** | `9090` | `http://localhost:9090/-/healthy` |
| **Grafana** | `3000` | `http://localhost:3000` (`admin`/`admin`) |

### 5. Run the Go Services
Open separate terminals to run each service:

```bash
# Terminal 1: Ingestor Service (:8081)
go run ./cmd/ingestor

# Terminal 2: Stream Processor (:8083 metrics)
go run ./cmd/processor

# Terminal 3: Analytics API (:8082)
go run ./cmd/analytics
```

---

## 📡 API Reference & `curl` Examples

### 1. Health Checks
```bash
# Ingestor Health Check
curl -s http://localhost:8081/health

# Analytics API Health Check (verifies ES & Redis connectivity)
curl -s http://localhost:8082/health
```

### 2. Ingest a Log Message
```bash
curl -X POST http://localhost:8081/api/v1/logs \
  -H "Content-Type: application/json" \
  -d '{
    "timestamp": "2026-08-24T12:00:00Z",
    "level": "ERROR",
    "service": "payment-api",
    "message": "Stripe gateway timeout after 30s",
    "trace_id": "trace-pay-9921",
    "ip": "192.168.1.55"
  }'
```

**Response (`202 Accepted`):**
```json
{
  "data": {
    "status": "queued",
    "trace_id": "trace-pay-9921",
    "request_id": "req_1724500000"
  },
  "meta": {
    "request_id": "req_1724500000",
    "timestamp": "2026-08-24T12:00:00.012Z"
  }
}
```

### 3. Query Real-Time Live Metrics (Sub-10ms)
```bash
curl -s "http://localhost:8082/metrics/live?services=all"
```

**Response (`200 OK`):**
```json
{
  "data": {
    "total_logs": 10420,
    "services": {
      "payment-api": {
        "total_logs": 5210,
        "total_errors": 42,
        "errors_last_5m": 3
      },
      "auth-service": {
        "total_logs": 3810,
        "total_errors": 5,
        "errors_last_5m": 0
      }
    }
  },
  "meta": {
    "request_id": "req_xyz",
    "timestamp": "2026-08-24T12:00:05Z"
  }
}
```

### 4. Query Top Error Leaderboard
```bash
curl -s "http://localhost:8082/metrics/top-errors?n=5"
```

### 5. Full-Text Log Search
```bash
curl -s "http://localhost:8082/search?q=Stripe&service=payment-api&level=ERROR&page=1&size=10"
```

**Response (`200 OK`):**
```json
{
  "data": {
    "total": 1,
    "page": 1,
    "size": 10,
    "pages": 1,
    "logs": [
      {
        "timestamp": "2026-08-24T12:00:00Z",
        "level": "ERROR",
        "service": "payment-api",
        "message": "Stripe gateway timeout after 30s",
        "trace_id": "trace-pay-9921",
        "ip": "192.168.1.55",
        "ingested_at": "2026-08-24T12:00:00.054Z"
      }
    ]
  },
  "meta": {
    "request_id": "req_abc",
    "timestamp": "2026-08-24T12:00:06Z"
  }
}
```

> [!WARNING]
> **Production Security & Authorization**: Administrative endpoints (`/admin/*`) are enabled for local operational management. In production environments, these endpoints MUST be protected behind an authentication and authorization layer (e.g., API Gateway, mTLS, JWT, or RBAC).

### 6. Storage Statistics & Index Information
```bash
curl -s http://localhost:8082/admin/logs/stats
```

### 7. Trigger Manual Retention Cycle
```bash
curl -X POST "http://localhost:8082/admin/logs/retention/run?days=30"
```

### 8. Safely Delete Historical Index by Name
```bash
curl -X DELETE http://localhost:8082/admin/logs/indices/logs-v1-2026.08.01
```

### 9. Delete Historical Indices Before Date
```bash
curl -X DELETE "http://localhost:8082/admin/logs?before=2026-08-01T00:00:00Z"
```

---

## 📊 Observability & Dashboards

- **Prometheus UI**: [`http://localhost:9090`](http://localhost:9090) — Scrapes endpoints `/metrics` across all running services.
- **Grafana UI**: [`http://localhost:3000`](http://localhost:3000) (Login: `admin` / `admin`).

The **Log Platform Overview** dashboard is pre-provisioned on startup and visualizes:
1. **Pipeline Throughput**: HTTP Ingestion rate vs. Processor consumption rate.
2. **Kafka Consumer Lag**: Real-time unprocessed messages in Kafka partition queues.
3. **DLQ Message Counter**: Total poison messages routed to `app-logs-dlq`.
4. **Latency Profiles**: Ingestion & processing p50, p95, and p99 histograms.
5. **Storage Metrics**: Elasticsearch indexing ops/sec and Redis real-time write rates.

---

## ⚡ Performance Benchmarks & Load Testing

The platform includes a CLI Load Generator ([`cmd/loadgen`](cmd/loadgen)) to simulate realistic production traffic and calculate latency percentiles.

### Running Load Generator
```bash
# Basic test: 500 logs/sec for 60s
go run ./cmd/loadgen --rate=500 --duration=60s

# Targeted batch: 1,000 logs with 20 workers
go run ./cmd/loadgen -n=1000 --workers=20 --service=payment-api,auth-service

# Unthrottled stress test
go run ./cmd/loadgen -n=10000 --rate=0 --workers=50
```

### Empirical Baseline Results *(Local Environment)*

> [!NOTE]
> Benchmark results were collected on a single local development machine (AMD64, Go 1.24, Docker Compose). They reflect baseline single-node performance and not universal cluster capacity.

| Test Profile | Target Rate | Workers | Logs Sent | Duration | Achieved Rate | Success Rate | p50 Latency | p95 Latency | p99 Latency |
|---|---|---|---|---|---|---|---|---|---|
| **Moderate Rate** | 500 logs/s | 15 | 5,000 | 10.31s | **485.1 logs/s** | 100% | 10.89ms | 13.18ms | 56.55ms |
| **High Load** | 1,000 logs/s | 20 | 10,000 | 11.26s | **887.8 logs/s** | 100% | 8.50ms | 29.35ms | 79.98ms |
| **Burst Ingestion** | 2,000 logs/s | 30 | 10,000 | 6.78s | **1,475.7 logs/s** | 100% | 8.15ms | 30.78ms | 83.08ms |
| **Peak Unthrottled** | Max | 80 | 25,000 | 15.43s | **1,620.4 logs/s** | 100% | 48.56ms | 78.78ms | 100.74ms |

*For the complete breakdown and metric analysis, see [docs/08-benchmarks.md](docs/08-benchmarks.md).*

### 🔍 Identified Bottleneck & Planned Optimization
- **Observation:** Ingestor scales to **1,620+ req/s** with sub-50ms latency because Kafka writes are buffered. However, the Stream Processor consumes at ~80–120 msg/s when indexing synchronously per-document into Elasticsearch.
- **Cause:** Each log requires a separate synchronous HTTP request (`IndexLog`), making network latency the primary constraint.
- **Remediation Roadmap:** Implementation of Elasticsearch micro-batching / bulk indexing (`_bulk` API buffering up to 200 documents or 100ms) as designed in ADR-005.

---

---

## 🛠️ Administrative CLI (`cmd/logctl`)

`logctl` is the operator command-line interface for managing and inspecting the Log Platform. It interacts exclusively with the Analytics/Admin API (`http://localhost:8082` by default, configurable via `LOGCTL_API_URL` or `--api-url`) and preserves all server-side safety protections.

### Command Reference & Examples

```bash
# 1. Check API and Dependency Health
go run ./cmd/logctl health
go run ./cmd/logctl health --json

# 2. Inspect Storage and Index Statistics
go run ./cmd/logctl logs stats
go run ./cmd/logctl logs stats --json

# 3. Search and Filter Logs
go run ./cmd/logctl logs search --service payment-api --level ERROR
go run ./cmd/logctl logs search --trace-id trace-12345 --json
go run ./cmd/logctl logs search --tenant-id tenant-a --query "timeout" --size 20

# 4. View Retention & Lifecycle Status
go run ./cmd/logctl retention status
go run ./cmd/logctl retention status --json

# 5. Trigger Manual Retention Run
go run ./cmd/logctl retention run --days 30
go run ./cmd/logctl retention run --days 14 --json

# 6. Delete Expired Index by Name (Requires confirmation unless --yes is supplied)
go run ./cmd/logctl logs delete-index logs-v1-2026.08.01
go run ./cmd/logctl logs delete-index logs-v1-2026.08.01 --yes

# 7. Delete Indices Older than Timestamp (Requires confirmation unless --yes is supplied)
go run ./cmd/logctl logs delete-before 2026-08-01T00:00:00Z --yes
```

---

## 🧪 Testing & Code Quality

Run all unit and integration tests:
```bash
# Run unit tests
go test -v ./...

# Run code formatting check
gofmt -s -l .

# Run static analysis
go vet ./...

# Build all binaries
go build ./...
```

---

## 🗺️ Roadmap & Future Work

- [x] High-throughput HTTP Ingestor with Kafka producer
- [x] Stream Processor with dual Elasticsearch & Redis sinks
- [x] Kafka Dead Letter Queue (DLQ) poison-pill isolation
- [x] Analytics REST API (`:8082`) with search and real-time metrics
- [x] Prometheus instrumentation & provisioned Grafana dashboards
- [x] Configurable Load Generator CLI (`cmd/loadgen`)
- [x] Elasticsearch micro-batching / Bulk indexing (`_bulk` API)
- [x] Partition-level parallel consumer worker pools
- [x] Admin management CLI (`cmd/logctl`)
- [ ] Next.js web dashboard frontend

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
