# Architecture Decision Records (ADRs)

> **Version:** 1.0  
> **Date:** 2026-08-06  
> **Status:** Draft  
> **Related:** `01-scope.md`, `04-data-model.md`

---

## What is an ADR?

An Architecture Decision Record captures a significant architectural decision, the context in which it was made, the options considered, and the consequences of the choice.

**Format per decision:**
- **Context:** What problem were we solving?
- **Decision:** What did we choose?
- **Alternatives Considered:** What else could we have done?
- **Consequences:** What do we gain? What do we lose?

---

## ADR-001: Language — Go

### Context
We need a language for building concurrent, network-heavy microservices that interact with Kafka, Redis, Elasticsearch, and PostgreSQL. The project is a solo effort with a 4-week timeline.

### Decision
Use **Go 1.22+** for all services and CLI tools.

### Alternatives Considered

| Language | Pros | Cons | Verdict |
|----------|------|------|---------|
| **Python** | Fast prototyping, huge ecosystem | GIL limits concurrency; dynamic typing catches errors at runtime | Rejected — Kafka consumer throughput matters |
| **Java** | Mature Kafka clients (Spring Kafka), strong ecosystem | Heavy JVM, verbose boilerplate, slower startup | Rejected — too heavy for a learning project |
| **Node.js** | Fast to write, great for APIs | Single-threaded event loop struggles with CPU-heavy batch processing | Rejected — bulk ES indexing is CPU-intensive |
| **Rust** | Performance, safety | Steep learning curve, async ecosystem still maturing | Rejected — would spend 2 weeks fighting the borrow checker |
| **.NET/C#** | Great tooling, familiar to the developer | Heavier runtime, less native Kafka/Redis library maturity in Go's ecosystem | Rejected — Go's concurrency model is better for this domain |

### Consequences

**Positive:**
- Native goroutines make Kafka consumers trivial to parallelize.
- Static binaries simplify Docker images (`FROM scratch` or `FROM alpine`).
- Excellent standard library (`net/http`, `database/sql`, `log/slog`).
- `segmentio/kafka-go` and `go-redis` are pure Go — no CGO complexity.

**Negative:**
- Verbose error handling (`if err != nil` everywhere).
- No generics-based ORM maturity (we use raw SQL + sqlc/manual mapping).
- Less "batteries included" than Java Spring or Python Django.

---

## ADR-002: Kafka in KRaft Mode (No ZooKeeper)

### Context
Kafka traditionally requires ZooKeeper for cluster coordination. Apache Kafka 3.3+ introduced KRaft (Kafka Raft), a self-managed quorum protocol that removes the ZooKeeper dependency.

### Decision
Use **Kafka in KRaft mode** — no ZooKeeper container.

### Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Kafka + ZooKeeper** | Battle-tested, vast documentation | Two services to manage, ZK is a separate failure domain | Rejected — unnecessary complexity for local dev |
| **Redpanda** | Kafka-compatible, no ZK, single binary | Different operational model, smaller community | Rejected — learning Kafka itself is a goal |
| **NATS Streaming** | Lightweight, simple | Not Kafka; different semantics, smaller ecosystem | Rejected — Kafka is the explicit learning target |
| **RabbitMQ** | Simple queues, easy setup | Not a log; no consumer groups, no replay, no partition scaling | Rejected — doesn't teach event streaming patterns |

### Consequences

**Positive:**
- One less Docker container (`zookeeper` removed).
- Simpler `docker-compose.yml`.
- KRaft is the future of Kafka; learning it now is forward-compatible.

**Negative:**
- KRaft is newer; some Stack Overflow answers still assume ZooKeeper.
- KRaft cluster bootstrapping requires explicit `KAFKA_NODE_ID` and quorum voters config.

---

## ADR-003: Polyglot Persistence (Four Data Stores)

### Context
The system needs to store relational metadata, searchable logs, real-time metrics, and an event stream. No single database handles all four access patterns well.

### Decision
Use **four specialized stores**, each handling one access pattern:

| Store | Data | Access Pattern |
|-------|------|----------------|
| **PostgreSQL** | Applications, services, environments, alert rules | Relational queries, joins, foreign key validation |
| **Elasticsearch** | Log messages (timestamp, level, service, message, trace_id, ip) | Full-text search, time-range filters, aggregations |
| **Redis** | Counters, leaderboards, recent errors, unique IPs, rate limits | Sub-millisecond reads, ranked queries, TTL expiration |
| **Kafka** | Raw log events | Durable log, consumer groups, replay, decoupling |

### Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **PostgreSQL for everything** | One database to manage | Full-text search is slow at scale; `COUNT(*)` + `ORDER BY` for leaderboards is O(N); no TTL | Rejected — wrong tool for search and real-time aggregations |
| **MongoDB for logs + metadata** | Flexible schema, JSON-native | No true full-text search (until Atlas Search); no consumer groups for event streaming | Rejected — doesn't teach Kafka or ES |
| **Single Redis instance** | Blazing fast | No persistence guarantee; no text search; no relational integrity | Rejected — Redis is a cache/performance layer, not source of truth |
| **Elasticsearch for everything** | Can store JSON blobs | No transactions, no foreign keys, eventual consistency | Rejected — operational metadata needs ACID |

### Consequences

**Positive:**
- Each store is used for what it was designed for.
- Demonstrates architectural maturity in interviews ("I chose the right database for each access pattern").
- Natural separation of concerns: PG = config, ES = search, Redis = speed, Kafka = events.

**Negative:**
- Four connections to manage in every service.
- More Docker containers to run locally.
- Operational complexity increases (four failure domains instead of one).

---

## ADR-004: Redis as a Data Structure Server (Not Just a Cache)

### Context
Redis is often used as a simple key-value cache. This project needs counters, rankings, recent-item queues, and unique sets — all with different performance characteristics.

### Decision
Use **multiple Redis data structures** intentionally, each chosen for its specific access pattern.

| Feature | Redis Structure | Why |
|---------|----------------|-----|
| Total logs | String (`INCR`) | Atomic counter, O(1) |
| Errors per service | String (`INCR`) | Simple per-service counter |
| Top services by volume | Sorted Set (`ZINCRBY`) | O(log N) insertion, O(log N + M) range query for top-N |
| Top error messages | Sorted Set (`ZINCRBY`) | Ranked frequency without recalculation |
| Recent errors (last 100) | List (`LPUSH` + `LTRIM`) | FIFO queue, O(1) push + trim |
| Unique IPs per day | Set (`SADD`) | Automatic deduplication, O(1) add, O(1) cardinality |
| Rate limiting | TTL String (`SETEX`) | Auto-expiry prevents manual cleanup |

### Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Hash for all counters** | Grouped namespace | No atomic field increment across keys; no ranking | Rejected — Hash `HINCRBY` is per-field, not per-key |
| **Sorted Set for everything** | Unified data type | Overkill for simple counters; higher memory overhead | Rejected — wrong complexity for simple increments |
| **PostgreSQL for counters** | Persistent, transactional | `UPDATE` + `SELECT COUNT` is disk-bound and slow | Rejected — real-time metrics need sub-millisecond latency |
| **Elasticsearch aggregations** | Built-in `terms` agg | Every query scans inverted index; not real-time | Rejected — aggregations are for analytics, not live dashboards |

### Consequences

**Positive:**
- Each data structure matches its use case perfectly.
- Demonstrates deep Redis knowledge in interviews.
- Pipeline commands (`LPUSH` + `LTRIM`) reduce round-trips.

**Negative:**
- More key patterns to document and maintain.
- Memory usage grows with unique keys (mitigated by TTL on ephemeral data).

---

## ADR-005: Elasticsearch Versioned Indices with Strict Mapping

### Context
Log data is time-series. Elasticsearch indices grow forever if not managed. Mappings evolve as requirements change.

### Decision
- **Index naming:** `logs-v1-YYYY.MM.DD` (daily rollover, versioned prefix).
- **Mapping:** `dynamic: strict` — reject documents with unknown fields.
- **Index template:** Auto-applies to `logs-v1-*`.

### Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Single index `logs`** | Simple | Becomes a giant shard; reindexing for mapping changes is painful | Rejected — time-series data needs time-based partitioning |
| **Monthly indices `logs-2026.08`** | Fewer indices | Individual shards grow large; harder to delete old data granularly | Rejected — daily gives better granularity for retention |
| **Dynamic mapping (default)** | Flexible, no schema changes needed | Mapping explosion risk; wrong types inferred (e.g., `2026-08-06` as date vs text) | Rejected — production systems use explicit mappings |
| **Data streams + ILM** | Automatic rollover, lifecycle management | More complex setup; overkill for local development | Rejected — ILM is Phase 2; manual daily indices teach the concept |

### Consequences

**Positive:**
- Daily indices prevent unbounded shard growth.
- Version prefix (`v1`) enables schema evolution: create `logs-v2-*` with new mapping, dual-write, then migrate.
- `dynamic: strict` catches schema violations at ingestion time.

**Negative:**
- More indices to manage (one per day).
- Search queries must use wildcard (`logs-v1-*`) or alias.
- No automatic cleanup yet (future: Curator or ILM).

---

## ADR-006: Bulk Indexing to Elasticsearch

### Context
Indexing logs one-by-one to Elasticsearch creates 1 HTTP request per log. At 1,000 logs/sec, that's 1,000 HTTP requests/sec — unsustainable.

### Decision
Buffer logs in memory and flush via the **Elasticsearch Bulk API**:
- **Batch size:** 100 documents
- **Flush interval:** 5 seconds (whichever comes first)
- **Refresh policy:** `false` during bulk, `wait_for` on final flush

### Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Index one-by-one** | Simple code, immediate visibility | 100x more HTTP overhead; ES thread pool exhaustion | Rejected — throughput would collapse |
| **Larger batch (1000 docs)** | Fewer requests | Higher memory usage; longer latency before logs appear | Rejected — 100 is the sweet spot for local dev |
| **Smaller batch (10 docs)** | Lower memory | More requests; less throughput benefit | Rejected — diminishing returns below 50 |
| **Async fire-and-forget** | Maximum speed | No error handling; silent data loss | Rejected — at-least-once delivery is a requirement |

### Consequences

**Positive:**
- ~100x reduction in HTTP requests.
- ES can optimize bulk indexing internally (better segment merging).
- Configurable batch size and flush interval for tuning.

**Negative:**
- 5-second delay before logs appear in search (acceptable for analytics, not for real-time alerting).
- Memory buffer grows if flush fails; needs backpressure handling.
- If the processor crashes mid-batch, those logs are lost (mitigated by Kafka offset commit after successful flush).

---

## ADR-007: Dead Letter Queue as a Kafka Topic

### Context
When the Stream Processor fails to parse or index a log, we need a failure path that doesn't block the pipeline or lose the message.

### Decision
Use a **dedicated Kafka topic `app-logs-dlq`** for failed messages. The processor commits the original offset and publishes the failure to DLQ.

### Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Write to PostgreSQL table** | Queryable, relational | Adds PG write load; different failure mode if PG is down | Rejected — Kafka is already the event backbone |
| **Write to local file** | Simple, no network | Not observable from other services; file rotation complexity | Rejected — breaks microservice share-nothing principle |
| **Retry indefinitely** | No data loss | Blocks consumer, causes lag, potential head-of-line blocking | Rejected — one poison message stalls the entire pipeline |
| **Skip and log warning** | Pipeline never stalls | Silent data loss; impossible to audit | Rejected — violates observability principles |

### Consequences

**Positive:**
- Same infrastructure (Kafka) for both success and failure paths.
- DLQ is replayable — can reprocess failed messages after a bug fix.
- DLQ growth is observable via Kafka consumer lag metrics.
- Commits original offset immediately — no head-of-line blocking.

**Negative:**
- DLQ messages consume Kafka disk space (30-day retention configured).
- Requires a separate consumer or CLI tool to inspect DLQ contents.
- No automatic retry logic yet (future: scheduled replay worker).

---

## ADR-008: Microservices vs. Modular Monolith

### Context
We need to decide whether to build separate deployable services or a single application with internal modules.

### Decision
Build **three separate Go binaries** (Ingestor, Processor, Analytics) that communicate via Kafka and HTTP. Two CLI tools (`logctl`, `loadgen`) are separate utilities.

### Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Modular monolith** | Simpler deployment, shared code, easier testing | Doesn't teach service boundaries, network failures, or independent scaling | Rejected — microservice patterns are an explicit learning goal |
| **5+ microservices** (parser, normalizer, indexer, metrics, alerts) | Pure single-responsibility | Operational nightmare for a solo dev; 4-week timeline impossible | Rejected — over-engineered for an MVP |
| **Serverless (Lambda/Cloud Functions)** | No infrastructure management | Cold starts, vendor lock-in, harder to run locally | Rejected — learning goal is infrastructure, not abstraction |

### Consequences

**Positive:**
- Each service has a single, clear responsibility.
- Services can be scaled independently (e.g., 3 processor instances, 1 ingestor).
- Forces explicit interface contracts (HTTP + Kafka).
- Demonstrates microservice thinking in interviews.

**Negative:**
- More Docker containers to manage locally.
- Network failures between services must be handled (retries, timeouts).
- Code duplication for config, logging, and client setup (mitigated by `internal/` shared packages).

---

## ADR-009: No Frontend, CLI-First Interface

### Context
The developer's goal is backend and database engineering, not frontend development. A dashboard would consume significant time without advancing the core learning objectives.

### Decision
Expose all functionality via **HTTP APIs and CLI tools**. No web UI.

### Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **React dashboard** | Visual, impressive to non-technical viewers | 2–3 weeks of React, CSS, state management; zero Kafka/Redis/ES learning | Rejected — frontend is not a goal |
| **Simple HTML + JS** | Quick to build | Still requires design, CORS, build tooling | Rejected — adds complexity without value |
| **Kibana as the "UI"** | Free, powerful, already included | Read-only; can't trigger ingestion or manage services | Accepted as debug/inspection tool only |
| **CLI + API only** | Fast to build, scriptable, demonstrates API design | Less visual for demos | **Chosen** — optimal for learning backend |

### Consequences

**Positive:**
- 100% of time spent on backend, data pipelines, and infrastructure.
- CLI tools (`logctl`, `loadgen`) demonstrate API consumption and tooling skills.
- Terminal demos are actually impressive to backend interviewers.

**Negative:**
- Harder to demo to non-technical stakeholders (mitigated by Kibana for log inspection).
- No real-time WebSocket push for metrics (polling via `curl` or CLI instead).

---

## ADR-010: Docker Compose (No Kubernetes)

### Context
We need a way to run Kafka, Redis, Elasticsearch, PostgreSQL, and three Go services locally.

### Decision
Use **Docker Compose** for local orchestration. Kubernetes is explicitly out of scope.

### Alternatives Considered

| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Kubernetes (minikube)** | Production-like, scalable, impressive resume word | 20+ hours of YAML, networking, storage, and debugging before writing any business logic | Rejected — would consume the entire timeline learning K8s |
| **Systemd services** | No containers, native | Hard to reproduce across machines; no isolation | Rejected — not portable |
| **Cloud managed services** (Confluent, Elastic Cloud, Redis Cloud) | Zero infrastructure management | Costs money; hides operational complexity; harder to debug locally | Rejected — learning goal includes running the infrastructure |
| **Docker Compose** | Single file, one command, portable, sufficient for local dev | Not production orchestration | **Chosen** — optimal for learning and demoing |

### Consequences

**Positive:**
- `docker-compose up -d` starts the entire system in under 60 seconds.
- Easy to reset state (`docker-compose down -v && docker-compose up -d`).
- Portable across macOS, Linux, and Windows (WSL2).

**Negative:**
- Single-node only — no HA, no clustering.
- Not a production deployment pattern (but the services are designed to be K8s-ready).

---

## ADR-011: `segmentio/kafka-go` vs. `confluent-kafka-go`

### Context
Go has two major Kafka client libraries. The choice affects build complexity, performance, and API ergonomics.

### Decision
Use **`github.com/segmentio/kafka-go`** for all Kafka interactions.

### Alternatives Considered

| Library | Pros | Cons | Verdict |
|---------|------|------|---------|
| **segmentio/kafka-go** | Pure Go (no CGO), simple API, good documentation, supports consumer groups | Slightly less performant than librdkafka for extreme throughput | **Chosen** — simplicity and portability win |
| **confluent-kafka-go** | Based on librdkafka (C), maximum performance, feature-complete | Requires CGO, complex build setup, heavier binary | Rejected — CGO complicates cross-compilation and Docker builds |
| **franz-go** | Modern, high-performance, pure Go | Newer, smaller community, different API design | Rejected — less documentation for beginners |

### Consequences

**Positive:**
- `go build` works everywhere without C compiler setup.
- Docker images are tiny (`FROM alpine` + static binary).
- Consumer group rebalancing and offset management are straightforward.

**Negative:**
- For production systems processing 100k+ msgs/sec, `confluent-kafka-go` would be reconsidered.

---

## ADR-012: `log/slog` for Structured Logging

### Context
Every service needs logging. We need structured, queryable logs with request tracing.

### Decision
Use Go's standard library **`log/slog`** (Go 1.21+) with JSON handler.

### Alternatives Considered

| Library | Pros | Cons | Verdict |
|---------|------|------|---------|
| **log/slog (stdlib)** | Standard, zero dependency, structured, performant | Newer; less ecosystem tooling than zap | **Chosen** — standard library is the future |
| **uber-go/zap** | Blazing fast, mature, widely used | External dependency; slightly more complex API | Rejected — slog is sufficient and standard |
| **sirupsen/logrus** | Simple, popular | Maintenance mode; slower than zap/slog | Rejected — deprecated in practice |
| **zerolog** | Fast, JSON-first | External dependency; API is opinionated | Rejected — slog covers the same use case |

### Consequences

**Positive:**
- No external logging dependency.
- JSON output is parseable by the platform itself (dogfooding).
- Request ID injection via `slog.With("request_id", id)`.

**Negative:**
- Fewer third-party integrations (e.g., no direct Zap → Datadog hook; but JSON is universal).

---

## ADR-013: Environment-Based Configuration (12-Factor)

### Context
Services need configuration for ports, connection strings, and tuning parameters. We want a consistent approach across all services.

### Decision
Use **environment variables** for all configuration, parsed by `github.com/caarlos0/env`. No YAML/JSON config files.

### Alternatives Considered

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **YAML config files** | Hierarchical, comments, version-controlled | File paths, mounting, secrets in git | Rejected — 12-factor app principle: config in env |
| **Command-line flags** | Explicit, self-documenting | Verbose Docker Compose commands, harder to manage many vars | Rejected — env vars are cleaner for containers |
| **Consul/etcd** | Dynamic, distributed | Overkill for local dev; adds infrastructure | Rejected — not needed for 4-week MVP |
| **Environment variables** | Container-native, 12-factor, secret-manager friendly | Flat namespace, no nesting | **Chosen** — standard for cloud-native apps |

### Consequences

**Positive:**
- `docker-compose.yml` sets all env vars in one place.
- Easy to override per environment (dev, staging, prod).
- Secrets can be injected via Docker secrets or orchestrator without code changes.

**Negative:**
- Flat namespace — no nested config objects (mitigated by prefixing: `INGESTOR_`, `PROCESSOR_`).

---

---

## ADR-014: Elasticsearch Log Lifecycle & Dedicated Retention Management Service

### Context
High-volume log ingestion continuously expands Elasticsearch storage. Without automated lifecycle management, disk usage grows unbounded. We need a robust, configurable retention policy and administrative API while preserving architectural separation of concerns and storage boundaries.

### Decision
1. **Index-Level Retention:** Leverage the existing daily index strategy (`logs-v1-YYYY.MM.DD`) to delete entire expired indices via the Elasticsearch Delete Index API rather than deleting individual documents. Dropping an entire index is an immediate \(O(1)\) metadata operation that instantly frees disk space without triggering expensive Lucene document tombstoning and segment merges.
2. **Dedicated Retention Service (`cmd/retention`):** Run the automated background periodic retention runner as a dedicated standalone service rather than embedding it inside Analytics API instances. This prevents multiple API replicas from competing for retention execution and adheres to the Single Responsibility Principle.
3. **Admin API Separation:** Analytics API (`cmd/analytics`) exposes administrative REST endpoints (`/admin/logs/*`) that directly invoke the `retention.Manager` for on-demand actions (manual retention runs, index deletion, storage statistics), with strict safety guards preventing deletion of the current active write index.
4. **Storage Responsibilities:**
   - **Elasticsearch:** Source of truth for log documents, full-text search, and index lifecycle.
   - **Redis:** Fast derived real-time metrics (counters, 5m sliding windows, leaderboards). Cumulative Redis metrics are **not** decremented upon Elasticsearch index drops to preserve performance and avoid massive Redis key scans; metric reconciliation is marked as a future milestone.
   - **PostgreSQL:** Future metadata/configuration repository (tenants, applications, retention policies, alert rules). PostgreSQL is not placed in the raw log data path.

### Safety Guarantees
- Only operates on `logs-v1-YYYY.MM.DD` formatted indices.
- Strictly protects today's active write index (`logs-v1-<today>`) from retention or manual deletion (returns HTTP 422).
- Rejects non-log indices, system indices (`.kibana`, `.security`), and arbitrary queries (returns HTTP 400).
- Exposes Prometheus operational telemetry (`log_platform_retention_*`, `log_platform_admin_deletions_*`).

---

## ADR-015: Multi-Tenant Architecture & PostgreSQL Metadata Store

### Context
To support multiple independent organizations/tenants on a shared platform, the system requires clear domain boundaries, secure tenant isolation, and administrative metadata management without introducing synchronous database overhead to the high-throughput log ingestion and stream processing paths.

### Decision
1. **Control-Plane vs. Data-Plane Separation:**
   - **PostgreSQL (Control Plane):** Serves exclusively as the relational metadata database for tenants, hashed API keys, service registrations, retention configurations, and future alert rules/RBAC. **PostgreSQL is never placed in the hot log ingestion path.**
   - **Kafka (Data Plane Transport):** The event payload carries `tenant_id` alongside log payload fields, allowing downstream consumers to process events without querying PostgreSQL per message.
   - **Elasticsearch (Data Plane Storage & Search):** Uses shared daily indices (`logs-v1-YYYY.MM.DD`) with a `tenant_id` keyword property. Search queries enforce strict tenant scoping via Elasticsearch boolean term filters (`{"term": {"tenant_id": "<tenant_id>"}}`).
   - **Redis (Real-Time Metrics):** Real-time counters, sliding error windows, and leaderboards use centralized tenant namespacing (`tenant:{tenant_id}:stats:...`, `tenant:{tenant_id}:leaderboard:...`) with backward-compatible defaults.
2. **SOLID Repository Layer:**
   - Repository interfaces (`TenantRepository`, `APIKeyRepository`, `ServiceRepository`, `RetentionPolicyRepository`) decouple business operations from PostgreSQL database logic.
   - In-memory mock repositories enable fast, independent unit tests without requiring a live PostgreSQL instance.
3. **API Key Security:**
   - API keys are cryptographically generated (32 bytes entropy) and stored exclusively as SHA-256 hex hashes (`key_hash`). Plaintext keys are never persisted or logged.
   - Future API-key resolution will be cached at the ingress boundary (e.g. in-memory or Redis cache) to prevent PostgreSQL query bottlenecks.

### Consequences
- **Positive:** Multi-tenant metadata management is established with zero performance penalty on the existing 1,600+ logs/sec ingestion pipeline.
- **Negative:** Search queries and metric aggregations must strictly include tenant scope to prevent cross-tenant data leakage.

---

## Summary Table

| # | Decision | Status | Reversibility |
|---|----------|--------|---------------|
| 001 | Go as language | Accepted | Hard (rewrite everything) |
| 002 | Kafka KRaft mode | Accepted | Medium (can add ZK later) |
| 003 | Polyglot persistence (4 stores) | Accepted | Hard (architectural) |
| 004 | Redis data structures (not just cache) | Accepted | Easy (key patterns evolve) |
| 005 | ES versioned indices + strict mapping | Accepted | Medium (reindexing needed) |
| 006 | Bulk indexing (100 docs / 5s) | Accepted | Easy (tune parameters) |
| 007 | DLQ as Kafka topic | Accepted | Medium (topic config) |
| 008 | 3 microservices + 2 CLI tools | Accepted | Hard (merge/split services) |
| 009 | No frontend, CLI-first | Accepted | Easy (add UI later) |
| 010 | Docker Compose (no K8s) | Accepted | Easy (add K8s manifests later) |
| 011 | segmentio/kafka-go | Accepted | Medium (swap client library) |
| 012 | log/slog for logging | Accepted | Easy (swap handler) |
| 013 | Environment-based config | Accepted | Easy (add file config later) |
| 014 | Index lifecycle & dedicated retention service | Accepted | Medium (swap lifecycle manager) |
| 015 | Multi-tenant architecture & PostgreSQL metadata | Accepted | Medium (schema evolution) |

---

*This document captures the reasoning behind every major technical choice. In interviews, expect questions like "Why Kafka over RabbitMQ?" or "Why not just use PostgreSQL for everything?" The answers are here.*


