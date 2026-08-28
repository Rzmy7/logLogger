# Sequence Diagrams & Data Flows

> **Version:** 1.0  
> **Date:** 2026-08-06  
> **Status:** Draft  
> **Related:** `01-scope.md`, `03-api-spec.md`, `04-data-model.md`

---

## 1. Legend

```
┌──────┐  Actor / External system
│ Actor│
└──┬───┘
   │
   ▼
┌──────────┐  Service / Component
│ Service  │
└────┬─────┘
   │
   │────▶  Synchronous call (HTTP, function call)
   │
   │───▶│  Asynchronous message (Kafka)
   │
   │───▶  Publish to topic
   │
   │◀───  Consume from topic
   │
   │────▶ Database read/write
   │
   │────▶ Cache read/write
   │
   │────▶ DLQ / Failure path
   │
   ────▶  Returns response
```

---

## 2. Happy Path: Log Ingestion to Searchable

**Scenario:** Client sends a valid log. System processes it. User searches and finds it.

```
Client        Ingestor       PostgreSQL    Kafka         Processor      Elasticsearch    Redis        Analytics
  │              │               │            │              │                │             │              │
  │ POST /logs   │               │            │              │                │             │              │
  │─────────────▶│               │            │              │                │             │              │
  │              │ SELECT service│            │              │                │             │              │
  │              │──────────────▶│            │              │                │             │              │
  │              │◀──────────────│            │              │                │             │              │
  │              │ (service exists)            │              │                │             │              │
  │              │               │            │              │                │             │              │
  │              │ Publish to    │            │              │                │             │              │
  │              │ app-logs      │            │              │                │             │              │
  │              │──────────────▶│───────────▶│              │                │             │              │
  │              │               │            │              │                │             │              │
  │ 202 Accepted │               │            │              │                │             │              │
  │◀─────────────│               │            │              │                │             │              │
  │              │               │            │              │                │             │              │
  │              │               │            │ Consume      │                │             │              │
  │              │               │            │◀─────────────│                │             │              │
  │              │               │            │              │                │             │              │
  │              │               │            │              │ Parse & Validate│             │              │
  │              │               │            │              │                │             │              │
  │              │               │            │              │ Bulk Index (100)│             │              │
  │              │               │            │              │────────────────▶│             │              │
  │              │               │            │              │                │             │              │
  │              │               │            │              │ Update Redis    │             │              │
  │              │               │            │              │───────────────────────────────▶│              │
  │              │               │            │              │                │             │              │
  │              │               │            │              │ Commit Offset   │             │              │
  │              │               │            │◀─────────────│                │             │              │
  │              │               │            │              │                │             │              │
  │              │               │            │              │                │             │              │
  │ GET /search  │               │            │              │                │             │              │
  │───────────────────────────────────────────────────────────────────────────────────────────▶│
  │              │               │            │              │                │             │              │
  │              │               │            │              │                │             │              │
  │              │               │            │              │                │             │              │
  │              │               │            │              │                │ Query ES    │              │
  │              │               │            │              │                │◀────────────│              │
  │              │               │            │              │                │             │              │
  │ 200 OK       │               │            │              │                │             │              │
  │◀───────────────────────────────────────────────────────────────────────────────────────────│
  │              │               │            │              │                │             │              │
```

**Timing expectations:**
- Ingestor → Kafka: < 10ms
- Processor → ES + Redis: < 50ms per batch
- Search query (ES): < 200ms
- Metrics query (Redis): < 10ms

---

## 3. Failure Path: Invalid Log → Dead Letter Queue

**Scenario:** Client sends a log with an unknown service. System rejects at ingestion. Then, a log with valid service but unparseable JSON reaches the processor.

```
Client        Ingestor       PostgreSQL    Kafka         Processor      DLQ Topic
  │              │               │            │              │                │
  │ POST /logs   │               │            │              │                │
  │─────────────▶│               │            │              │                │
  │              │ SELECT service│            │              │                │
  │              │──────────────▶│            │              │                │
  │              │◀──────────────│            │              │                │
  │              │ (service NOT found)         │              │                │
  │              │               │            │              │                │
  │ 400 Bad Req  │               │            │              │                │
  │◀─────────────│               │            │              │                │
  │              │               │            │              │                │
  │              │               │            │              │                │
  │              │               │            │              │                │
  │ POST /logs   │               │            │              │                │
  │─────────────▶│               │            │              │                │
  │              │ SELECT service│            │              │                │
  │              │──────────────▶│            │              │                │
  │              │◀──────────────│            │              │                │
  │              │ (service exists)            │              │                │
  │              │               │            │              │                │
  │              │ Publish       │            │              │                │
  │              │──────────────▶│───────────▶│              │                │
  │              │               │            │              │                │
  │ 202 Accepted │               │            │              │                │
  │◀─────────────│               │            │              │                │
  │              │               │            │              │                │
  │              │               │            │ Consume      │                │
  │              │               │            │◀─────────────│                │
  │              │               │            │              │                │
  │              │               │            │              │ Parse FAILS     │
  │              │               │            │              │ (invalid JSON)  │
  │              │               │            │              │                │
  │              │               │            │              │ Publish to DLQ  │
  │              │               │            │              │────────────────▶│
  │              │               │            │              │                │
  │              │               │            │              │ Commit Offset   │
  │              │               │            │◀─────────────│                │
  │              │               │            │              │                │
```

**Key principle:** The original offset is committed **after** DLQ publish succeeds. This prevents the poison message from being re-consumed indefinitely while ensuring it's not lost.

---

## 4. Real-Time Metrics Query

**Scenario:** User wants to see current error rates without waiting for Elasticsearch indexing.

```
Client        Analytics API    Redis
  │              │               │
  │ GET /metrics │               │
  │─────────────▶│               │
  │              │               │
  │              │ MGET stats:*  │
  │              │──────────────▶│
  │              │◀──────────────│
  │              │               │
  │              │ ZREVRANGE     │
  │              │ leaderboard:* │
  │              │──────────────▶│
  │              │◀──────────────│
  │              │               │
  │ 200 OK       │               │
  │◀─────────────│               │
  │              │               │
```

**Why Redis?** All data is in memory. No disk I/O. No index scanning. Sub-10ms response.

**Why not Elasticsearch?** ES aggregations require scanning the inverted index and computing counts. At scale, this is 100–1000x slower than a Redis `ZREVRANGE`.

---

## 5. Historical Search Query

**Scenario:** User searches for "timeout" in payment-api ERROR logs from the last 7 days.

```
Client        Analytics API    Elasticsearch
  │              │               │
  │ GET /search  │               │
  │─────────────▶│               │
  │              │               │
  │              │ Build bool    │
  │              │ query:        │
  │              │ - match: msg  │
  │              │ - term: svc   │
  │              │ - term: level │
  │              │ - range: time │
  │              │               │
  │              │ POST logs-v1-*/_search
  │              │──────────────▶│
  │              │◀──────────────│
  │              │ (hits + aggs) │
  │              │               │
  │ 200 OK       │               │
  │◀─────────────│               │
  │              │               │
```

**Index pattern:** `logs-v1-*` matches all daily indices. ES routes the query to relevant shards automatically.

**Pagination:** `from=(page-1)*size`, `size=20`.

---

## 6. Service Metadata Lookup

**Scenario:** User lists all services with their applications and environments.

```
Client        Analytics API    PostgreSQL
  │              │               │
  │ GET /services│               │
  │─────────────▶│               │
  │              │               │
  │              │ SELECT + JOIN │
  │              │ services      │
  │              │ applications  │
  │              │ environments  │
  │              │──────────────▶│
  │              │◀──────────────│
  │              │               │
  │ 200 OK       │               │
  │◀─────────────│               │
  │              │               │
```

**Query pattern:**
```sql
SELECT s.id, s.name, a.name AS application, e.name AS environment, s.created_at
FROM services s
JOIN applications a ON s.application_id = a.id
JOIN environments e ON s.environment_id = e.id
ORDER BY s.name;
```

---

## 7. Admin CLI: Creating a Service

**Scenario:** Developer uses `logctl` to register a new service before sending logs.

```
Developer     logctl CLI     Analytics API    PostgreSQL
  │              │               │               │
  │ logctl svc   │               │               │
  │ create ...   │               │               │
  │─────────────▶│               │               │
  │              │               │               │
  │              │ POST /services│               │
  │              │ (internal)    │               │
  │              │──────────────▶│               │
  │              │               │               │
  │              │               │ INSERT INTO   │
  │              │               │ services      │
  │              │               │──────────────▶│
  │              │               │◀──────────────│
  │              │               │               │
  │              │◀─────────────│               │
  │              │               │               │
  │ Created!     │               │               │
  │◀─────────────│               │               │
  │              │               │               │
```

**Why this matters:** You cannot send logs for a service that doesn't exist. This enforces data quality at the ingestion boundary.

---

## 8. Load Generator: Benchmarking

**Scenario:** Developer runs `loadgen` to measure system throughput.

```
loadgen       Ingestor       Kafka         Processor      Elasticsearch    Redis
  │              │            │              │                │             │
  │ POST /logs   │            │              │                │             │
  │ (x1000/sec)  │            │              │                │             │
  │─────────────▶│            │              │                │             │
  │              │ Publish    │              │                │             │
  │              │───────────▶│              │                │             │
  │              │            │              │                │             │
  │ 202 Accepted │            │              │                │             │
  │◀─────────────│            │              │                │             │
  │              │            │              │                │             │
  │ (repeat)     │            │              │                │             │
  │              │            │ Consume      │                │             │
  │              │            │◀─────────────│                │             │
  │              │            │              │ Bulk Index     │             │
  │              │            │              │───────────────▶│             │
  │              │            │              │ Update Redis   │             │
  │              │            │              │─────────────────────────────▶│
  │              │            │              │                │             │
```

**Metrics collected by loadgen:**
- Total requests sent
- Success count (HTTP 202)
- Failure count (non-202)
- Request rate (req/sec)
- Latency percentiles (p50, p99)

---

## 9. Graceful Shutdown

**Scenario:** Developer presses Ctrl+C on the Stream Processor. In-flight work must complete.

```
OS/SIGTERM    Stream Processor    Kafka Consumer    ES Bulk Buffer    Redis Pipeline
  │              │                  │                 │                 │
  │ SIGTERM      │                  │                 │                 │
  │─────────────▶│                  │                 │                 │
  │              │                  │                 │                 │
  │              │ Stop consuming   │                 │                 │
  │              │ (no new polls)   │                 │                 │
  │              │                  │                 │                 │
  │              │ Finish current   │                 │                 │
  │              │ batch processing │                 │                 │
  │              │                  │                 │                 │
  │              │ Flush ES bulk    │                 │                 │
  │              │────────────────────────────────────▶│                 │
  │              │                  │                 │                 │
  │              │ Execute Redis    │                 │                 │
  │              │ pipeline         │                 │                 │
  │              │───────────────────────────────────────────────────────▶│
  │              │                  │                 │                 │
  │              │ Commit Kafka     │                 │                 │
  │              │ offsets          │                 │                 │
  │              │─────────────────▶│                 │                 │
  │              │                  │                 │                 │
  │              │ Exit 0           │                 │                 │
  │              │                  │                 │                 │
```

**Implementation in Go:**
```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

// Run processor
<-ctx.Done()

// Graceful shutdown
processor.Shutdown(ctx, 30*time.Second)
```

**Timeout:** 30 seconds. If shutdown takes longer, force exit.

---

## 10. System Startup Sequence

**Order matters.** Services must wait for their dependencies.

```
Step 1: Infrastructure
  ├─ PostgreSQL starts
  ├─ Redis starts
  ├─ Elasticsearch starts
  └─ Kafka (KRaft) starts
        └─ Create topics: app-logs, app-logs-dlq

Step 2: Migrations
  └─ Run PostgreSQL migrations (applications, environments, services, etc.)

Step 3: Seed Data
  └─ Insert environments: production, staging, development

Step 4: Services
  ├─ Stream Processor starts (waits for Kafka, ES, Redis)
  ├─ Analytics API starts (waits for Redis, ES, PostgreSQL)
  └─ Log Ingestor starts (waits for Kafka, PostgreSQL)

Step 5: Verification
  ├─ curl localhost:8081/health
  ├─ curl localhost:8082/health
  └─ logctl service list
```

**Docker Compose `depends_on`:**
```yaml
services:
  ingestor:
    depends_on:
      - kafka
      - postgres
  processor:
    depends_on:
      - kafka
      - elasticsearch
      - redis
  analytics:
    depends_on:
      - redis
      - elasticsearch
      - postgres
```

**Note:** `depends_on` only waits for container start, not service readiness. Each Go service should implement retry loops with backoff for its dependencies.

---

## 11. Data Consistency Model

### Source of Truth Hierarchy

```
┌─────────────────────────────────────────┐
│           Kafka (app-logs)              │
│     Source of truth for events          │
└─────────────────┬───────────────────────┘
                  │
      ┌───────────┼───────────┐
      ▼           ▼           ▼
┌─────────┐ ┌─────────┐ ┌─────────┐
│    ES   │ │  Redis  │ │   DLQ   │
│ (search)│ │(metrics)│ │(failures│
│         │ │         │ │  audit) │
└─────────┘ └─────────┘ └─────────┘
   Derived     Derived    Derived
   (index)    (aggregate) (audit)
```

**Key principle:** Kafka is the immutable event log. ES and Redis are **derived views** that can be rebuilt from Kafka if corrupted.

### Failure Recovery

| Failure | Impact | Recovery |
|---------|--------|----------|
| ES index corrupted | Search broken | Reindex from Kafka (replay) |
| Redis data lost | Metrics blank | Rebuild from ES or replay Kafka |
| Kafka data lost | Catastrophic | Not recoverable (Kafka is source of truth) |
| DLQ messages pile up | Audit trail grows | Inspect via `logctl dlq inspect`; fix bug; replay if needed |

---

## 13. Mermaid Sequence Diagrams

### 13.1 Ingestion & Dual-Sink Stream Processing

```mermaid
sequenceDiagram
    autonumber
    actor Client as Log Client
    participant Ingestor as Ingestor API (:8081)
    participant Kafka as Kafka (app-logs)
    participant Processor as Stream Processor
    participant ES as Elasticsearch (logs-v1-*)
    participant Redis as Redis Cache
    participant DLQ as Kafka (app-logs-dlq)

    Client->>Ingestor: POST /api/v1/logs
    alt Invalid Schema
        Ingestor-->>Client: 400 Bad Request
    else Valid Schema
        Ingestor->>Kafka: Publish message (key=trace_id/service)
        Ingestor-->>Client: 202 Accepted (request_id, trace_id)
    end

    Kafka->>Processor: Fetch batch / message
    alt Malformed / Poison Message
        Processor->>DLQ: Publish to app-logs-dlq (error diagnostics)
        Processor->>Kafka: Commit original offset
    else Valid Event
        par Dual Sink Writes
            Processor->>ES: Index document (IndexLog)
            Processor->>Redis: Pipelined metric counters (RecordLog)
        end
        alt Sinks Succeeded
            Processor->>Kafka: Commit message offset
        else Any Sink Failed
            Processor-->>Processor: Retry with backoff (Do NOT commit offset)
        end
    end
```

### 13.2 Automated Background Retention Cycle

```mermaid
sequenceDiagram
    autonumber
    participant Timer as Retention Ticker (1h)
    participant Service as Retention Service (:8084)
    participant Manager as RetentionManager (internal/retention)
    participant ES as Elasticsearch (:9200)
    participant Prom as Prometheus Metrics

    Timer->>Service: Ticker trigger
    Service->>Manager: RunRetention(ctx, retentionDays=30)
    Manager->>ES: ListIndices("logs-v1-*")
    ES-->>Manager: List of IndexInfo (name, docs, size)
    
    loop For each index
        alt Index is today's active write index (logs-v1-today)
            Manager->>Manager: Skip (Preserve active index)
        else Index date < (now - retentionDays)
            Manager->>ES: DeleteIndex(indexName)
            ES-->>Manager: 200 OK (Acknowledged)
            Manager->>Prom: Inc(log_platform_retention_indices_deleted_total)
        else Index is younger than retentionDays
            Manager->>Manager: Skip (Preserve recent index)
        end
    end
    
    Manager->>Prom: Inc(log_platform_retention_runs_total{status="success"})
    Manager-->>Service: RetentionResult (evaluated, deleted, duration)
```

### 13.3 Administrative Index Deletion & Safety Rejections

```mermaid
sequenceDiagram
    autonumber
    actor Admin as Platform Admin
    participant API as Analytics API (:8082)
    participant Manager as RetentionManager
    participant ES as Elasticsearch

    Admin->>API: DELETE /admin/logs/indices/{index}
    
    alt Index name does not match logs-v1-YYYY.MM.DD
        API-->>Admin: 400 Bad Request (INVALID_INDEX_NAME)
    else Index is today's active write index
        API-->>Admin: 422 Unprocessable Entity (PROTECTED_INDEX)
    else Index is valid historical log index
        API->>Manager: DeleteIndexByName(ctx, index)
        Manager->>ES: DeleteIndex(index)
        alt Index not found in ES
            ES-->>Manager: 404 Not Found
            Manager-->>API: 404 Not Found
            API-->>Admin: 404 Not Found
        else Index deleted successfully
            ES-->>Manager: 200 OK
            Manager-->>API: nil
            API-->>Admin: 200 OK ({"deleted_index": index, "status": "deleted"})
        end
    end
```

### 13.4 Multi-Tenant Resolution & Log Streaming Flow

```mermaid
sequenceDiagram
    autonumber
    actor Client as Log Client
    participant Ingestor as Ingestor API (:8081)
    participant Cache as Metadata Cache / PG
    participant Kafka as Kafka (app-logs)
    participant Processor as Stream Processor
    participant ES as Elasticsearch (logs-v1-*)
    participant Redis as Redis Store

    Client->>Ingestor: POST /api/v1/logs [Header: Authorization Bearer raw_key]
    Ingestor->>Ingestor: SHA256(raw_key) -> key_hash
    Ingestor->>Cache: Lookup tenant by key_hash (Cached)
    alt Invalid / Revoked Key
        Ingestor-->>Client: 401 Unauthorized
    else Valid Tenant
        Ingestor->>Kafka: Publish event (tenant_id, trace_id, level, service, message)
        Ingestor-->>Client: 202 Accepted
    end

    Kafka->>Processor: Consume event
    par Isolated Sinks
        Processor->>ES: Index document with keyword tenant_id
        Processor->>Redis: Record metrics to tenant:{tenant_id}:*
    end
    Processor->>Kafka: Commit offset
```

---

*These diagrams are the contract for how data moves through the system. When debugging, identify which arrow is broken.*


