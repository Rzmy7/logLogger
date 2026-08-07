# Project Scope: Real-Time Log Analytics Platform

> **Version:** 1.0  
> **Author:** [Your Name]  
> **Date:** 2026-08-06  
> **Status:** Draft

---

## 1. Elevator Pitch

A backend-only, event-driven log analytics platform that ingests application logs via HTTP, processes them through an asynchronous streaming pipeline, and serves real-time metrics and searchable historical data through a REST API. Built to demonstrate proficiency in distributed messaging (Kafka), in-memory data structures (Redis), full-text search (Elasticsearch), relational metadata (PostgreSQL), and Go microservices.

---

## 2. Goals

1. **Learn by building:** Gain hands-on experience with Kafka, Redis, Elasticsearch, and PostgreSQL in a unified system.
2. **Event-driven architecture:** Decouple log producers from consumers using Kafka as the central nervous system.
3. **Polyglot persistence:** Use the right database for each access pattern — PostgreSQL for relational metadata, Elasticsearch for search, Redis for real-time aggregations.
4. **Production patterns:** Implement batch processing, dead letter queues, structured logging, graceful shutdowns, and measurable benchmarks.
5. **Portfolio value:** Ship a documented, runnable, demoable system that can be explained in a 5-minute interview walkthrough.

---

## 3. Non-Goals

The following are explicitly out of scope for the MVP. They may be added in future phases.

- Frontend dashboard (React/Vue/HTML)
- Kubernetes or container orchestration
- Authentication, authorization, or multi-tenancy
- Alert engine (rules exist in DB; no evaluation worker yet)
- Log enrichment (IP geolocation, user-agent parsing)
- Prometheus / Grafana monitoring
- OpenTelemetry distributed tracing
- Kafka replay engine
- CI/CD pipelines

---

## 4. Architecture Overview

```
                    Clients
                      │
                      ▼
              ┌───────────────┐
              │   INGESTOR    │
              │   (Go API)    │
              │    :8081      │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │ KAFKA (KRaft) │
              │  app-logs     │
              │  3 partitions │
              └───────┬───────┘
                      │
                      ▼
              ┌───────────────┐
              │   PROCESSOR   │
              │    (Go)       │
              │  • Consume    │
              │  • Validate   │
              │  • Bulk Index │
              │  • Update     │
              │    Redis      │
              │  • DLQ on fail│
              └───────┬───────┘
                      │
         ┌────────────┼────────────┐
         ▼            ▼            ▼
   ┌──────────┐ ┌──────────┐ ┌──────────┐
   │    ES    │ │  Redis   │ │   DLQ    │
   │(logs-v1-*)│ │ • Counters│ │  Topic   │
   │          │ │ • ZSets    │ │          │
   └────┬─────┘ │ • Lists    │ └──────────┘
        │      │ • Sets     │
        │      └─────┬──────┘
        │            │
        └─────┬──────┘
              ▼
       ┌───────────────┐
       │  ANALYTICS    │
       │    API        │
       │   (Go)        │
       │   :8082       │
       └───────────────┘
              ▲
              │
       ┌──────┴──────┐
       │ PostgreSQL   │
       │  (Metadata)  │
       │              │
       │ • Applications│
       │ • Services    │
       │ • Environments│
       │ • Alert Rules │
       │ • Saved Searches│
       └──────────────┘
```

**Data flow:**
1. Client sends log to Ingestor.
2. Ingestor validates schema and checks service exists in PostgreSQL.
3. Ingestor publishes to Kafka `app-logs` topic.
4. Processor consumes, bulk-indexes to Elasticsearch, updates Redis data structures.
5. Invalid messages go to `app-logs-dlq` Kafka topic.
6. Analytics API serves metrics from Redis and search from Elasticsearch.

---

## 5. Technology Stack

| Layer | Technology | Version / Mode | Justification |
|-------|-----------|----------------|---------------|
| Language | Go | 1.22+ | Native concurrency, static binaries, excellent client libraries |
| HTTP Router | chi | v5 | Lightweight, idiomatic middleware |
| Kafka Client | segmentio/kafka-go | latest | Pure Go, no CGO, simple API |
| Redis Client | redis/go-redis | v9 | Official, type-safe, cluster-aware |
| ES Client | olivere/elastic | v7 | Mature, fluent query DSL |
| PostgreSQL | Postgres | 15-alpine | ACID metadata, foreign keys, JSONB |
| Kafka Broker | Apache Kafka | KRaft mode | No ZooKeeper — one less moving part |
| Search Engine | Elasticsearch | 8.11.0 | Inverted indices, aggregations, Kibana |
| Cache / Real-time | Redis | 7-alpine | Data structures (ZSet, List, Set, String) |
| Visualization | Kibana | 8.11.0 | Debug and verify ES indexing without code |
| Orchestration | Docker Compose | v2 | Single-command local deployment |
| Logging | slog | Go stdlib | Structured, no external dependency |
| Config | caarlos0/env | latest | 12-factor config from environment |

---

## 6. Service Breakdown

### 6.1 Log Ingestor (`:8081`)
- **Type:** HTTP API
- **Responsibility:** Single entry point for all log ingestion.
- **Endpoints:**
  - `POST /api/v1/logs` — validate, check PG, publish to Kafka
  - `GET /health` — Kafka + PostgreSQL connectivity check
- **Key behavior:** Returns `202 Accepted` immediately. Ingestion is asynchronous.

### 6.2 Stream Processor (background worker)
- **Type:** Background worker (no exposed port)
- **Responsibility:** The brain of the pipeline.
- **Key behaviors:**
  - Consumes from Kafka consumer group `log-processors`
  - Buffers 100 logs or flushes every 5 seconds (whichever comes first)
  - Bulk-indexes to Elasticsearch via Bulk API
  - Updates Redis counters, sorted sets, lists, and sets
  - Writes parse failures to `app-logs-dlq` topic
  - Commits Kafka offset only after successful ES + Redis writes
  - Graceful shutdown on SIGTERM (finish current batch, then exit)

### 6.3 Analytics API (`:8082`)
- **Type:** HTTP API
- **Responsibility:** Serve queries to downstream consumers.
- **Endpoints:**
  - `GET /health` — Redis + ES + PG connectivity
  - `GET /metrics/live` — real-time counters from Redis
  - `GET /metrics/top-errors?n=5` — ranked error types from Redis ZSet
  - `GET /search?q=&service=&level=&from=&to=&page=&size=` — full-text search in ES
  - `GET /services` — list services from PostgreSQL (with app/env joins)

### 6.4 Admin CLI (`logctl`)
- **Type:** Go CLI tool
- **Commands:**
  - `logctl app create <name> [description]`
  - `logctl env create <name>`
  - `logctl service create <app> <env> <name>`
  - `logctl service list`
  - `logctl search --service= --level= --last=`
  - `logctl benchmark --rate= --duration=`
  - `logctl dlq inspect`

### 6.5 Load Generator (`loadgen`)
- **Type:** Go CLI tool
- **Purpose:** Generate realistic logs at configurable rates for benchmarking.
- **Example:** `go run ./cmd/loadgen --rate=1000 --duration=300s --service=payment-api`

---

## 7. Data Stores & Access Patterns

| Store | Data | Access Pattern | Why This Store |
|-------|------|----------------|----------------|
| **PostgreSQL** | Applications, Services, Environments, Alert Rules, Saved Searches | Relational queries, joins, foreign key validation | ACID, schema enforcement, operational metadata |
| **Elasticsearch** | Log messages (timestamp, level, service, message, trace_id, ip) | Full-text search, time-range filters, aggregations | Inverted index, tokenization, horizontal scaling |
| **Redis** | Counters, leaderboards, recent errors, unique IPs, rate-limit windows | Sub-millisecond reads, ranked queries, TTL expiration | In-memory data structures (String, ZSet, List, Set) |
| **Kafka** | Raw log events | Durable log, consumer groups, replay, DLQ | Decoupling, backpressure handling, fault tolerance |

---

## 8. In-Scope Features (MVP)

- [ ] Kafka KRaft deployment (no ZooKeeper)
- [ ] PostgreSQL schema + migrations (applications, services, environments, alert_rules, saved_searches)
- [ ] Log Ingestor with schema validation and PG service lookup
- [ ] Kafka producer with partitioning by trace_id
- [ ] Stream Processor with consumer groups
- [ ] Elasticsearch bulk indexing (100 docs / 5s flush)
- [ ] Versioned ES indices (`logs-v1-YYYY.MM.DD`)
- [ ] Redis multi-structure schema (counters, ZSets, Lists, Sets, TTL)
- [ ] Dead Letter Queue (`app-logs-dlq` topic)
- [ ] Analytics API (metrics, search, service metadata)
- [ ] Admin CLI (`logctl`)
- [ ] Load generator with configurable rate
- [ ] Docker Compose local deployment
- [ ] Structured logging (`slog`) + request ID middleware
- [ ] Graceful shutdown handling
- [ ] Measured benchmarks (throughput, latency)
- [ ] Architecture documentation + runbook

---

## 9. Out-of-Scope Features (Phase 2+)

- [ ] Frontend dashboard
- [ ] Kubernetes deployment
- [ ] Authentication / authorization
- [ ] Alert evaluation engine
- [ ] Log enrichment (IP → geo, user-agent parsing)
- [ ] Prometheus / Grafana metrics
- [ ] OpenTelemetry tracing
- [ ] Kafka replay engine
- [ ] Index Lifecycle Management (ILM) automation
- [ ] CI/CD pipeline

---

## 10. Success Criteria

This project is considered complete when ALL of the following are true:

1. `docker-compose up -d` starts all infrastructure cleanly.
2. `logctl app create` + `logctl service create` populate PostgreSQL.
3. `curl` to `POST /api/v1/logs` returns `202` and the log appears in Kafka.
4. Stream Processor writes the log to Elasticsearch (visible in Kibana) and Redis.
5. `GET /metrics/live` returns accurate real-time counts from Redis (< 10ms).
6. `GET /search?q=timeout` returns relevant logs from Elasticsearch.
7. Invalid logs (bad JSON, unknown service) land in `app-logs-dlq`.
8. Load generator runs for 5 minutes without crashing the system.
9. Benchmarks are measured and documented (not estimated).
10. A stranger can clone the repo, follow the runbook, and reproduce the demo in under 10 minutes.

---

## 11. Timeline

| Week | Focus | Key Deliverable |
|------|-------|-----------------|
| **Week 1** | Infrastructure + Ingestor | Docker Compose (KRaft, Redis, ES, PG, Kibana). PG migrations. Ingestor API with validation. Kafka producer. |
| **Week 2** | Stream Processor | Consumer groups. Bulk ES indexing. Redis multi-structure updates. DLQ topic. Kibana verification. |
| **Week 3** | Analytics + CLI | Analytics API endpoints. `logctl` CLI. Integration testing. |
| **Week 4** | Benchmark + Polish | Load generator. Measure throughput & latency. README, architecture diagram, demo video. |

**Daily commitment:** 2–3 hours weekdays, 5–6 hours weekends.

---

## 12. Folder Structure

```
log-platform/
├── cmd/
│   ├── ingestor/          # HTTP API :8081
│   ├── processor/         # Kafka consumer worker
│   ├── analytics/         # HTTP API :8082
│   ├── logctl/            # Admin CLI
│   └── loadgen/           # Benchmark tool
├── internal/
│   ├── config/            # 12-factor env config
│   ├── models/            # Go structs
│   ├── postgres/          # Connection, migrations, queries
│   ├── kafka/             # Producer, consumer, DLQ
│   ├── redis/             # Schema helpers
│   ├── elastic/           # Bulk indexer, search builder
│   └── telemetry/         # slog, request ID middleware
├── migrations/            # PostgreSQL .sql files
├── deployments/
│   └── docker-compose.yml
├── docs/
│   ├── 01-scope.md
│   ├── 02-architecture.md
│   ├── 03-api-spec.md
│   ├── 04-data-model.md
│   ├── 05-sequence-diagrams.md
│   ├── 06-runbook.md
│   └── 07-development-log.md
├── scripts/
├── benchmarks/
└── README.md
```

---

## 12. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Kafka KRaft unfamiliar | High | Spend Day 1 on docs + Docker verification before writing code |
| ES mapping errors | Medium | Define mapping in `04-data-model.md` before indexing |
| Redis key sprawl | Low | Document every key pattern in `04-data-model.md` |
| Scope creep (K8s, frontend) | High | Re-read this document. Out-of-scope list is final. |
| Over-optimizing throughput | Medium | Measure first, tune second. Don't guess numbers. |

---

## 13. Glossary

| Term | Definition |
|------|------------|
| **DLQ** | Dead Letter Queue — a separate Kafka topic for messages that fail processing |
| **KRaft** | Kafka Raft — ZooKeeper-less Kafka mode using an internal Raft consensus protocol |
| **Bulk API** | Elasticsearch API for indexing multiple documents in a single HTTP request |
| **ZSet** | Redis Sorted Set — a collection of unique elements ordered by a score |
| **Consumer Group** | A set of Kafka consumers that cooperate to read from a topic in parallel |
| **Offset** | Kafka's record of how far a consumer has read in a partition |

---

*This document is the single source of truth for project scope. Any change requires updating this file and recording the reason in `07-development-log.md`.*
