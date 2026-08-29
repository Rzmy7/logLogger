# API & Interface Specification

> **Version:** 1.0  
> **Date:** 2026-08-06  
> **Status:** Draft  
> **Related:** `01-scope.md`, `04-data-model.md`

---

## 1. Overview

This document defines all public interfaces for the log analytics platform:

- **HTTP APIs:** REST endpoints exposed by the Ingestor and Analytics services.
- **CLI Tools:** Command-line interfaces for the admin tool (`logctl`) and load generator (`loadgen`).

All HTTP APIs return JSON and use standard HTTP status codes. All timestamps are RFC3339 unless noted otherwise.

---

## 2. HTTP API Conventions

### 2.1 Base URLs

| Service | Base URL | Environment Variable |
|---------|----------|---------------------|
| Ingestor | `http://localhost:8081` | `INGESTOR_ADDR` |
| Analytics | `http://localhost:8082` | `ANALYTICS_ADDR` |

### 2.2 Common Response Format

**Success (2xx):**
```json
{
  "data": { ... },
  "meta": {
    "request_id": "req_abc123",
    "timestamp": "2026-08-06T10:00:00Z"
  }
}
```

**Error (4xx/5xx):**
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Field 'level' must be one of: DEBUG, INFO, WARN, ERROR, FATAL",
    "details": { "field": "level", "received": "CRITICAL" }
  },
  "meta": {
    "request_id": "req_abc123",
    "timestamp": "2026-08-06T10:00:00Z"
  }
}
```

### 2.3 Common Headers

| Header | Required | Value |
|--------|----------|-------|
| `Content-Type` | Yes (POST/PUT) | `application/json` |
| `X-Request-ID` | No | Client-generated trace ID (falls back to server-generated) |

### 2.5 Multi-Tenant Authentication & Future Authorization Model

> [!NOTE]
> In the current foundational phase, requests without explicit authorization headers default to `tenant_id: "default"` for 100% backward compatibility with local testing, benchmarking, and existing tests. The multi-tenant metadata layer in PostgreSQL establishes the schema and repository contracts for future authentication enforcement.

**Future Authorization Flow:**
1. **API Key Header:** Clients provide `Authorization: Bearer <raw_api_key>` (e.g. `Authorization: Bearer ll_live_9a8f...`) or `X-API-Key: <raw_api_key>`.
2. **Ingress Resolution:** The Ingestor/Analytics API hashes the provided key via SHA-256 and resolves the active `tenant_id` from the cached metadata layer (never executing a per-message synchronous database query on the hot log path).
3. **Tenant Scoping:**
   - Ingested log events are automatically stamped with the resolved `tenant_id` and published to Kafka.
   - Search requests (`GET /search`) automatically enforce boolean filter `{"term": {"tenant_id": "<tenant_id>"}}` in Elasticsearch.
   - Real-time metrics queries are routed to the tenant's isolated Redis keys (`tenant:<tenant_id>:*`).

---

## 3. Log Ingestor API (`:8081`)

### 3.1 `POST /api/v1/logs`

Accept and queue a log for processing.

**Request:**
```http
POST /api/v1/logs HTTP/1.1
Host: localhost:8081
Content-Type: application/json
X-Request-ID: req_abc123

{
  "timestamp": "2026-08-06T10:00:00Z",
  "level": "ERROR",
  "service": "payment-api",
  "message": "DB connection timeout after 30s",
  "trace_id": "abc-123-def-456",
  "ip": "192.168.1.5"
}
```

**Field Specification:**

| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| `timestamp` | string | Yes | RFC3339 format |
| `level` | string | Yes | Enum: `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL` |
| `service` | string | Yes | Must exist in PostgreSQL `services` table |
| `message` | string | Yes | 1–1000 characters |
| `trace_id` | string | No | Any string, used for Kafka partitioning |
| `ip` | string | No | Valid IPv4 or IPv6 address |

**Response `202 Accepted`:**
```json
{
  "data": {
    "status": "queued",
    "trace_id": "abc-123-def-456",
    "request_id": "req_abc123"
  },
  "meta": {
    "request_id": "req_abc123",
    "timestamp": "2026-08-06T10:00:00.100Z"
  }
}
```

**Response `400 Bad Request` (validation failure):**
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Validation failed",
    "details": [
      { "field": "level", "message": "must be one of DEBUG, INFO, WARN, ERROR, FATAL" },
      { "field": "service", "message": "service 'unknown-api' does not exist" }
    ]
  },
  "meta": { "request_id": "req_abc123", "timestamp": "2026-08-06T10:00:00.100Z" }
}
```

**Response `429 Too Many Requests`:**
```json
{
  "error": {
    "code": "RATE_LIMITED",
    "message": "Rate limit exceeded. Try again in 45 seconds."
  },
  "meta": { "request_id": "req_abc123", "timestamp": "2026-08-06T10:00:00.100Z" }
}
```

**Response `503 Service Unavailable`:**
```json
{
  "error": {
    "code": "KAFKA_UNAVAILABLE",
    "message": "Unable to queue log. Kafka broker unreachable."
  },
  "meta": { "request_id": "req_abc123", "timestamp": "2026-08-06T10:00:00.100Z" }
}
```

**Rate Limiting:**
- Checked via Redis key `ratelimit:{client_ip}`
- Max 100 requests per minute per IP
- TTL: 60 seconds

---

### 3.2 `GET /health`

Health check for load balancers and monitoring.

**Request:**
```http
GET /health HTTP/1.1
Host: localhost:8081
```

**Response `200 OK` (all healthy):**
```json
{
  "data": {
    "status": "healthy",
    "services": {
      "kafka": "up",
      "postgresql": "up"
    }
  },
  "meta": { "request_id": "req_abc123", "timestamp": "2026-08-06T10:00:00Z" }
}
```

**Response `503 Service Unavailable` (degraded):**
```json
{
  "data": {
    "status": "degraded",
    "services": {
      "kafka": "up",
      "postgresql": "down"
    }
  },
  "meta": { "request_id": "req_abc123", "timestamp": "2026-08-06T10:00:00Z" }
}
```

---

## 4. Analytics API (`:8082`)

### 4.1 `GET /health`

Health check for Analytics API and its dependencies.

**Request:**
```http
GET /health HTTP/1.1
Host: localhost:8082
```

**Response `200 OK`:**
```json
{
  "data": {
    "status": "healthy",
    "services": {
      "redis": "up",
      "elasticsearch": "up",
      "postgresql": "up"
    }
  },
  "meta": { "request_id": "req_xyz789", "timestamp": "2026-08-06T10:00:00Z" }
}
```

---

### 4.2 `GET /metrics/live`

Real-time metrics from Redis. Sub-10ms response time.

**Request:**
```http
GET /metrics/live HTTP/1.1
Host: localhost:8082
```

**Query Parameters:**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `services` | string | No | `all` | Comma-separated list of service names, or `all` |

**Response `200 OK`:**
```json
{
  "data": {
    "total_logs": 15420,
    "services": {
      "payment-api": {
        "total_logs": 8921,
        "total_errors": 42,
        "errors_last_5m": 3
      },
      "auth-service": {
        "total_logs": 4102,
        "total_errors": 7,
        "errors_last_5m": 0
      },
      "notification-service": {
        "total_logs": 2397,
        "total_errors": 1,
        "errors_last_5m": 0
      }
    }
  },
  "meta": { "request_id": "req_xyz789", "timestamp": "2026-08-06T10:00:00Z" }
}
```

**Redis Queries Executed:**
- `GET stats:logs:total`
- `GET stats:logs:{service}`
- `GET stats:errors:{service}`
- `GET stats:errors:last_5m:{service}`

---

### 4.3 `GET /metrics/top-errors`

Ranked error messages by frequency (Redis Sorted Set).

**Request:**
```http
GET /metrics/top-errors?n=5 HTTP/1.1
Host: localhost:8082
```

**Query Parameters:**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `n` | int | No | 5 | Number of top errors to return (max 100) |

**Response `200 OK`:**
```json
{
  "data": {
    "top_errors": [
      { "message": "DB connection timeout", "count": 156 },
      { "message": "HTTP 500 from upstream", "count": 89 },
      { "message": "Redis connection refused", "count": 34 },
      { "message": "JWT validation failed", "count": 21 },
      { "message": "Timeout waiting for lock", "count": 12 }
    ]
  },
  "meta": { "request_id": "req_xyz789", "timestamp": "2026-08-06T10:00:00Z" }
}
```

**Redis Query Executed:**
- `ZREVRANGE leaderboard:errors 0 {n-1} WITHSCORES`

---

### 4.4 `GET /metrics/top-services`

Top services by log volume (Redis Sorted Set).

**Request:**
```http
GET /metrics/top-services?n=5 HTTP/1.1
Host: localhost:8082
```

**Query Parameters:**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `n` | int | No | 5 | Number of top services to return |

**Response `200 OK`:**
```json
{
  "data": {
    "top_services": [
      { "service": "payment-api", "count": 8921 },
      { "service": "auth-service", "count": 4102 },
      { "service": "notification-service", "count": 2397 }
    ]
  },
  "meta": { "request_id": "req_xyz789", "timestamp": "2026-08-06T10:00:00Z" }
}
```

**Redis Query Executed:**
- `ZREVRANGE leaderboard:services 0 {n-1} WITHSCORES`

---

### 4.5 `GET /search`

Full-text search in Elasticsearch with filters.

**Request:**
```http
GET /search?q=timeout&service=payment-api&level=ERROR&from=2026-08-01T00:00:00Z&to=2026-08-06T23:59:59Z&page=1&size=20 HTTP/1.1
Host: localhost:8082
```

**Query Parameters:**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `q` | string | No | `*` | Full-text search query on `message` field |
| `service` | string | No | — | Exact match filter on `service` |
| `level` | string | No | — | Exact match filter on `level` |
| `trace_id` | string | No | — | Exact match filter on `trace_id` |
| `from` | string | No | `now-24h` | Start timestamp (RFC3339) |
| `to` | string | No | `now` | End timestamp (RFC3339) |
| `page` | int | No | 1 | Page number (1-indexed) |
| `size` | int | No | 20 | Results per page (max 100) |

**Response `200 OK`:**
```json
{
  "data": {
    "total": 1042,
    "page": 1,
    "size": 20,
    "pages": 53,
    "logs": [
      {
        "timestamp": "2026-08-06T09:45:00Z",
        "level": "ERROR",
        "service": "payment-api",
        "message": "DB connection timeout after 30s",
        "trace_id": "abc-123-def-456",
        "ip": "192.168.1.5",
        "ingested_at": "2026-08-06T09:45:01Z"
      }
    ]
  },
  "meta": { "request_id": "req_xyz789", "timestamp": "2026-08-06T10:00:00Z" }
}
```

**Response `400 Bad Request` (invalid date format):**
```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid date format",
    "details": { "field": "from", "expected": "RFC3339", "received": "2026-08-01" }
  },
  "meta": { "request_id": "req_xyz789", "timestamp": "2026-08-06T10:00:00Z" }
}
```

**Elasticsearch Query Generated:**
```json
{
  "query": {
    "bool": {
      "must": [
        { "match": { "message": "timeout" } }
      ],
      "filter": [
        { "term": { "service": "payment-api" } },
        { "term": { "level": "ERROR" } },
        { "range": { "timestamp": { "gte": "2026-08-01T00:00:00Z", "lte": "2026-08-06T23:59:59Z" } } }
      ]
    }
  },
  "sort": [{ "timestamp": "desc" }],
  "from": 0,
  "size": 20
}
```

---

### 4.7 `GET /admin/logs/stats`

Get cluster-level storage statistics and list of active/historical log indices from Elasticsearch.

**Request:**
```http
GET /admin/logs/stats HTTP/1.1
Host: localhost:8082
```

**Response `200 OK`:**
```json
{
  "data": {
    "total_logs": 34925,
    "total_indices": 3,
    "total_size_bytes": 7157586,
    "oldest_index": "logs-v1-2026.08.01",
    "oldest_log_date": "2026-08-01T00:00:00Z",
    "newest_index": "logs-v1-2026.08.28",
    "newest_log_date": "2026-08-28T00:00:00Z",
    "indices": [
      {
        "name": "logs-v1-2026.08.01",
        "doc_count": 12000,
        "store_size_bytes": 2450000,
        "creation_date": "2026-08-01T00:00:00Z",
        "status": "open"
      },
      {
        "name": "logs-v1-2026.08.28",
        "doc_count": 22925,
        "store_size_bytes": 4707586,
        "creation_date": "2026-08-28T00:00:00Z",
        "status": "open"
      }
    ]
  },
  "meta": { "request_id": "req_stats_01", "timestamp": "2026-08-28T12:00:00Z" }
}
```

---

### 4.8 `POST /admin/logs/retention/run`

Manually trigger an immediate retention cycle with a specified retention period.

**Request:**
```http
POST /admin/logs/retention/run?days=30 HTTP/1.1
Host: localhost:8082
```

**Query Parameters:**

| Param | Type | Required | Default | Description |
|---|---|---|---|---|
| `days` | int | No | 30 | Retention window in days (must be positive integer) |

**Response `200 OK`:**
```json
{
  "data": {
    "evaluated_count": 5,
    "deleted_count": 2,
    "deleted_indices": [
      "logs-v1-2026.07.01",
      "logs-v1-2026.07.02"
    ],
    "cutoff_date": "2026-07-29T00:00:00Z",
    "duration": 124000000
  },
  "meta": { "request_id": "req_ret_01", "timestamp": "2026-08-28T12:00:00Z" }
}
```

---

### 4.9 `DELETE /admin/logs/indices/{index}`

Safely delete a specific historical log index by name.

**Safety Rules:**
- Only operates on `logs-v1-YYYY.MM.DD` index format.
- Strictly rejects deletion of today's active write index (`422 Unprocessable Entity`).
- Rejects non-log and system indices (`400 Bad Request`).

**Request:**
```http
DELETE /admin/logs/indices/logs-v1-2026.08.01 HTTP/1.1
Host: localhost:8082
```

**Response `200 OK`:**
```json
{
  "data": {
    "deleted_index": "logs-v1-2026.08.01",
    "status": "deleted"
  },
  "meta": { "request_id": "req_del_01", "timestamp": "2026-08-28T12:00:00Z" }
}
```

---

### 4.10 `DELETE /admin/logs?before=<RFC3339>`

Delete all historical log indices created strictly before a cutoff timestamp.

**Request:**
```http
DELETE /admin/logs?before=2026-08-01T00:00:00Z HTTP/1.1
Host: localhost:8082
```

**Response `200 OK`:**
```json
{
  "data": {
    "evaluated_count": 4,
    "deleted_count": 2,
    "deleted_indices": [
      "logs-v1-2026.07.15",
      "logs-v1-2026.07.20"
    ],
    "cutoff_date": "2026-08-01T00:00:00Z",
    "duration": 98000000
  },
  "meta": { "request_id": "req_del_before_01", "timestamp": "2026-08-28T12:00:00Z" }
}
```

---

### 4.11 `GET /services`

List all services with their application and environment (PostgreSQL join).

**Request:**
```http
GET /services HTTP/1.1
Host: localhost:8082
```

**Response `200 OK`:**
```json
{
  "data": {
    "services": [
      {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "name": "payment-api",
        "application": "ecommerce-platform",
        "environment": "production",
        "created_at": "2026-08-01T00:00:00Z"
      },
      {
        "id": "550e8400-e29b-41d4-a716-446655440001",
        "name": "auth-service",
        "application": "ecommerce-platform",
        "environment": "production",
        "created_at": "2026-08-01T00:00:00Z"
      }
    ]
  },
  "meta": { "request_id": "req_xyz789", "timestamp": "2026-08-06T10:00:00Z" }
}
```

**PostgreSQL Query Executed:**
```sql
SELECT s.id, s.name, a.name as application, e.name as environment, s.created_at
FROM services s
JOIN applications a ON s.application_id = a.id
JOIN environments e ON s.environment_id = e.id
ORDER BY s.name;
```

---

### 4.7 `GET /services/{id}/recent-errors`

Last 100 errors for a specific service (Redis List).

**Request:**
```http
GET /services/550e8400-e29b-41d4-a716-446655440000/recent-errors?n=10 HTTP/1.1
Host: localhost:8082
```

**Query Parameters:**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `n` | int | No | 10 | Number of recent errors (max 100) |

**Response `200 OK`:**
```json
{
  "data": {
    "service": "payment-api",
    "recent_errors": [
      {
        "timestamp": "2026-08-06T09:45:00Z",
        "level": "ERROR",
        "message": "DB connection timeout after 30s",
        "trace_id": "abc-123-def-456"
      }
    ]
  },
  "meta": { "request_id": "req_xyz789", "timestamp": "2026-08-06T10:00:00Z" }
}
```

**Redis Query Executed:**
- `LRANGE recent:errors:payment-api 0 {n-1}`

---

## 5. Admin CLI (`logctl`)

`logctl` is the operator CLI for managing and inspecting the Log Platform by communicating exclusively with the Analytics/Admin HTTP API.

### 5.1 Global Configuration & Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--api-url <url>` | `$LOGCTL_API_URL` or `http://localhost:8082` | Analytics API base URL |
| `--json` | `false` | Output machine-readable JSON to stdout |
| `--help`, `-h` | — | Display help information |

---

### 5.2 `logctl health`

Check health and status of the Analytics API and its dependencies (Elasticsearch, Redis).

**Usage:**
```bash
logctl health [--json]
```

**Example:**
```bash
logctl health
```

**Output:**
```text
STATUS                       healthy
DEPENDENCY (elasticsearch)   healthy
DEPENDENCY (redis)           healthy
TIMESTAMP                    2026-08-29T10:18:44Z
REQUEST ID                   req_abc123
```

---

### 5.3 `logctl logs stats`

Show storage statistics, document counts, and active daily log indices.

**Usage:**
```bash
logctl logs stats [--json]
```

**Output:**
```text
Storage Statistics
------------------
Total Logs:     50000
Total Indices:  1
Total Size:     8.3 MB (8.26 MB)
Oldest Index:   logs-v1-2026.08.29 (2026-08-29 00:00:00 +0000 UTC)
Newest Index:   logs-v1-2026.08.29 (2026-08-29 00:00:00 +0000 UTC)

INDEX NAME           DOCUMENTS   SIZE     STATUS   CREATED
logs-v1-2026.08.29   50000       8.3 MB   open     2026-08-29 09:36:03.068 +0000 UTC
```

---

### 5.4 `logctl logs search`

Search and filter indexed log documents via the Analytics API (`GET /search`).

**Usage:**
```bash
logctl logs search [flags]
```

**Flags:**

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--query`, `-q` | string | `""` | Full-text search match in log message |
| `--service` | string | `""` | Filter by service name |
| `--level` | string | `""` | Filter by level (`DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`) |
| `--trace-id` | string | `""` | Filter by trace ID |
| `--tenant-id` | string | `""` | Filter by tenant ID |
| `--from` | string | `""` | Filter from timestamp (RFC3339) |
| `--to` | string | `""` | Filter to timestamp (RFC3339) |
| `--page` | int | `1` | Page number |
| `--size` | int | `20` | Results per page (max 100) |
| `--json` | bool | `false` | Output machine-readable JSON |

**Example:**
```bash
logctl logs search --service payment-api --level ERROR --size 2
```

**Output:**
```text
Search Results (Total: 42, Page 1/21)
TIMESTAMP                     LEVEL  SERVICE      TENANT   TRACE ID                MESSAGE
2026-08-29T09:37:07.538Z      ERROR  payment-api  default  trace-99e2b0d66ffc0786  Timeout waiting for lock on resource_768
2026-08-29T09:37:07.464Z      ERROR  payment-api  default  trace-4eb37cd8e73821ff  DB connection timeout after 851s
```

---

### 5.5 `logctl logs delete-index`

Delete a specific daily log index by name (destructive). Prompts for interactive confirmation unless `--yes` is supplied.

**Usage:**
```bash
logctl logs delete-index <index_name> [--yes] [--json]
```

**Safety Protection:**
- Rejects non-log indices (e.g. `.kibana`, `.security`) with `400 INVALID_INDEX_NAME`.
- Rejects today's active write index with `422 PROTECTED_INDEX`.

**Example:**
```bash
logctl logs delete-index logs-v1-2026.08.01 --yes
```

---

### 5.6 `logctl logs delete-before`

Delete all log indices older than the specified cutoff timestamp (destructive). Prompts for confirmation unless `--yes` is supplied.

**Usage:**
```bash
logctl logs delete-before <RFC3339_timestamp> [--yes] [--json]
```

**Example:**
```bash
logctl logs delete-before 2026-08-01T00:00:00Z --yes
```

---

### 5.7 `logctl retention status`

Display retention lifecycle summary, active retention threshold, and index details.

**Usage:**
```bash
logctl retention status [--json]
```

---

### 5.8 `logctl retention run`

Trigger manual retention policy cleanup against the Analytics API (`POST /admin/logs/retention/run?days=N`).

**Usage:**
```bash
logctl retention run [--days N] [--json]
```

**Example:**
```bash
logctl retention run --days 30
```

**Output:**
```text
Retention Execution Complete
----------------------------
Evaluated Indices: 4
Deleted Indices:   1
Cutoff Date:       2026-07-30 00:00:00 +0000 UTC
Duration:          15.67ms
Deleted Indices:   logs-v1-2026.07.15
```

---

## 6. Load Generator CLI (`loadgen`)

### 6.1 Usage

```bash
loadgen [flags]
```

### 6.2 Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--rate` | int | 100 | Target logs per second |
| `--duration` | duration | 60s | How long to run |
| `--ingestor` | string | `http://localhost:8081` | Ingestor URL |
| `--service` | string | `payment-api` | Service name in generated logs |
| `--level` | string | `mixed` | `mixed` (weighted), `INFO`, `ERROR`, `DEBUG`, `WARN`, `FATAL` |
| `--message-len` | int | 50 | Average message length (randomized ±50%) |
| `--trace-id` | bool | true | Include random trace IDs |
| `--ip` | bool | true | Include random IPs |

### 6.3 Examples

**Basic run:**
```bash
go run ./cmd/loadgen --rate=500 --duration=5m
```

**High-volume stress test:**
```bash
go run ./cmd/loadgen --rate=5000 --duration=10m --level=mixed --service=payment-api
```

**Error-only flood:**
```bash
go run ./cmd/loadgen --rate=100 --duration=1m --level=ERROR
```

### 6.4 Output

```
Load Generator
==============
Target Rate:     500 logs/sec
Duration:        5m0s
Service:         payment-api
Level:           mixed

Starting in 3s...

[00:30] 15000 logs sent | 498.2 req/s | p50: 8ms | p99: 32ms
[01:00] 30000 logs sent | 501.1 req/s | p50: 7ms | p99: 28ms
...

Done.
Total:    150000 logs
Success:  150000
Failed:   0
Avg Rate: 499.8 req/s
p50 Lat:  8ms
p99 Lat:  31ms
```

### 6.5 Log Generation Rules

When `--level=mixed`:

| Level | Weight |
|-------|--------|
| `INFO` | 70% |
| `DEBUG` | 15% |
| `WARN` | 10% |
| `ERROR` | 4% |
| `FATAL` | 1% |

Messages are randomly generated from a pool of realistic templates:
- `"Request completed in {n}ms"`
- `"DB connection timeout after {n}s"`
- `"Cache miss for key {key}"`
- `"HTTP {code} from upstream"`
- `"JWT validation failed"`

---

## 7. Error Code Reference

| Code | HTTP | Meaning |
|------|------|---------|
| `VALIDATION_ERROR` | 400 | Request body failed schema validation |
| `NOT_FOUND` | 404 | Requested resource does not exist |
| `RATE_LIMITED` | 429 | Too many requests from this client |
| `KAFKA_UNAVAILABLE` | 503 | Kafka broker unreachable |
| `POSTGRES_UNAVAILABLE` | 503 | PostgreSQL unreachable |
| `ELASTICSEARCH_ERROR` | 503 | Elasticsearch query failed |
| `REDIS_UNAVAILABLE` | 503 | Redis unreachable |
| `INTERNAL_ERROR` | 500 | Unexpected server error |

---

## 8. Changelog

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-08-06 | Initial specification |

---

*This document is the contract for all public interfaces. Any change to endpoints, fields, or CLI commands requires updating this file and recording the reason in `07-development-log.md`.*
