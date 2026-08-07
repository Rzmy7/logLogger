# Local Development Runbook

> **Version:** 1.0  
> **Date:** 2026-08-06  
> **Status:** Draft  
> **Related:** `01-scope.md`, `05-sequence-diagrams.md`

---

## 1. Prerequisites

| Tool | Minimum Version | Purpose | Install Link |
|------|----------------|---------|--------------|
| Go | 1.22 | Service language | https://go.dev/dl |
| Docker | 24.0 | Container runtime | https://docs.docker.com/get-docker |
| Docker Compose | 2.20 | Multi-container orchestration | Included with Docker Desktop |
| curl | Any | HTTP testing | Pre-installed on macOS/Linux |
| make | Any | Build automation | Pre-installed on macOS/Linux; `choco install make` on Windows |

**Verify installations:**
```bash
go version          # go1.22.x
docker --version    # Docker version 24.x
docker compose version  # Docker Compose version v2.x
curl --version      # curl 8.x
```

---

## 2. Quick Start (5 Minutes)

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/log-platform.git
cd log-platform
```

### Step 2: Start Infrastructure

```bash
docker compose -f deployments/docker-compose.yml up -d
```

**Expected output:**
```
[+] Running 6/6
 ✔ Container log-platform-postgres-1      Started
 ✔ Container log-platform-redis-1         Started
 ✔ Container log-platform-elasticsearch-1 Started
 ✔ Container log-platform-kafka-1         Started
 ✔ Container log-platform-kibana-1        Started
```

**Wait 30 seconds** for all services to initialize (especially Kafka KRaft and Elasticsearch).

### Step 3: Verify Infrastructure

```bash
# PostgreSQL
docker exec log-platform-postgres-1 pg_isready -U postgres
# Output: /var/run/postgresql:5432 - accepting connections

# Redis
docker exec log-platform-redis-1 redis-cli ping
# Output: PONG

# Elasticsearch
curl -s http://localhost:9200/_cluster/health | jq '.status'
# Output: "yellow" or "green"

# Kafka
docker exec log-platform-kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
# Output: (empty initially; topics created by services)
```

### Step 4: Run Migrations

```bash
go run ./cmd/migrate
```

**Expected output:**
```
2026/08/06 10:00:00 Migration 001_create_applications.sql applied
2026/08/06 10:00:00 Migration 002_create_environments.sql applied
2026/08/06 10:00:00 Migration 003_create_services.sql applied
2026/08/06 10:00:00 Migration 004_create_alert_rules.sql applied
2026/08/06 10:00:00 Migration 005_create_saved_searches.sql applied
2026/08/06 10:00:00 All migrations completed successfully
```

### Step 5: Seed Initial Data

```bash
go run ./cmd/seed
```

**Expected output:**
```
2026/08/06 10:00:00 Created environment: production
2026/08/06 10:00:00 Created environment: staging
2026/08/06 10:00:00 Created environment: development
2026/08/06 10:00:00 Created application: ecommerce
2026/08/06 10:00:00 Created service: ecommerce/production/payment-api
2026/08/06 10:00:00 Created service: ecommerce/production/auth-service
2026/08/06 10:00:00 Seeding completed
```

### Step 6: Start Services

Open **3 separate terminal windows** and run each service:

**Terminal 1 — Stream Processor:**
```bash
go run ./cmd/processor
```

**Terminal 2 — Analytics API:**
```bash
go run ./cmd/analytics
```

**Terminal 3 — Log Ingestor:**
```bash
go run ./cmd/ingestor
```

**Expected output for each:**
```
2026/08/06 10:00:00 INFO service=processor kafka=connected redis=connected elasticsearch=connected
2026/08/06 10:00:00 INFO service=processor consumer_group=log-processors topic=app-logs started
```

### Step 7: Verify Everything Works

```bash
# Health checks
curl http://localhost:8081/health
curl http://localhost:8082/health

# List services
curl http://localhost:8082/services | jq

# Send a test log
curl -X POST http://localhost:8081/api/v1/logs   -H "Content-Type: application/json"   -d '{
    "timestamp": "2026-08-06T10:00:00Z",
    "level": "ERROR",
    "service": "payment-api",
    "message": "DB connection timeout after 30s",
    "trace_id": "abc-123-def-456",
    "ip": "192.168.1.5"
  }'

# Expected: {"data":{"status":"queued","trace_id":"abc-123-def-456"},...}
```

**Wait 5 seconds** for the processor to flush, then:

```bash
# Check metrics
curl http://localhost:8082/metrics/live | jq

# Search logs
curl "http://localhost:8082/search?q=timeout&service=payment-api" | jq

# View in Kibana
open http://localhost:5601
# Navigate to: Stack Management → Index Patterns → Create → logs-v1-*
# Then: Discover → Select logs-v1-* pattern
```

---

## 3. Full Reset

To wipe all data and start fresh:

```bash
# Stop all services (Ctrl+C in each terminal)

# Stop and remove containers + volumes
docker compose -f deployments/docker-compose.yml down -v

# Restart infrastructure
docker compose -f deployments/docker-compose.yml up -d

# Re-run migrations and seed
go run ./cmd/migrate
go run ./cmd/seed

# Restart services
go run ./cmd/processor
go run ./cmd/analytics
go run ./cmd/ingestor
```

---

## 4. Port Reference

| Service | Port | Access |
|---------|------|--------|
| Log Ingestor | 8081 | `curl http://localhost:8081` |
| Analytics API | 8082 | `curl http://localhost:8082` |
| PostgreSQL | 5432 | `psql -h localhost -U postgres` |
| Redis | 6379 | `redis-cli -p 6379` |
| Elasticsearch | 9200 | `curl http://localhost:9200` |
| Kibana | 5601 | `http://localhost:5601` |
| Kafka | 9092 | `kafka-console-consumer.sh --bootstrap-server localhost:9092` |

---

## 5. Common Commands

### Docker Compose

```bash
# Start everything
docker compose -f deployments/docker-compose.yml up -d

# View logs
docker compose -f deployments/docker-compose.yml logs -f kafka

# Stop everything
docker compose -f deployments/docker-compose.yml down

# Stop and delete all data (volumes)
docker compose -f deployments/docker-compose.yml down -v

# Restart single service
docker compose -f deployments/docker-compose.yml restart redis
```

### PostgreSQL

```bash
# Connect
docker exec -it log-platform-postgres-1 psql -U postgres -d logplatform

# List tables
\dt

# Query services
SELECT s.name, a.name AS app, e.name AS env
FROM services s
JOIN applications a ON s.application_id = a.id
JOIN environments e ON s.environment_id = e.id;

# Exit
\q
```

### Redis

```bash
# Connect
docker exec -it log-platform-redis-1 redis-cli

# View all keys
KEYS *

# View counters
GET stats:logs:total
GET stats:errors:payment-api

# View leaderboard
ZREVRANGE leaderboard:errors 0 4 WITHSCORES

# View recent errors
LRANGE recent:errors:payment-api 0 9

# View unique IPs
SCARD unique:ips:2026-08-06

# Exit
EXIT
```

### Elasticsearch

```bash
# Cluster health
curl -s http://localhost:9200/_cluster/health | jq

# List indices
curl -s http://localhost:9200/_cat/indices?v

# Search logs
curl -X POST "http://localhost:9200/logs-v1-*/_search"   -H "Content-Type: application/json"   -d '{
    "query": { "match": { "message": "timeout" } },
    "sort": [{ "timestamp": "desc" }],
    "size": 10
  }' | jq

# Delete all indices (DANGER — full reset)
curl -X DELETE "http://localhost:9200/logs-v1-*"
```

### Kafka

```bash
# List topics
docker exec log-platform-kafka-1 kafka-topics.sh   --bootstrap-server localhost:9092 --list

# Consume main topic
docker exec log-platform-kafka-1 kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic app-logs   --from-beginning

# Consume DLQ
docker exec log-platform-kafka-1 kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic app-logs-dlq   --from-beginning

# Check consumer group lag
docker exec log-platform-kafka-1 kafka-consumer-groups.sh   --bootstrap-server localhost:9092   --describe   --group log-processors
```

---

## 6. Using the CLI Tools

### logctl

```bash
# Build
go build -o bin/logctl ./cmd/logctl

# Create application
./bin/logctl app create ecommerce "E-commerce Platform"

# Create environment
./bin/logctl env create production

# Create service
./bin/logctl service create ecommerce production payment-api "Payment API"

# List services
./bin/logctl service list

# Search logs
./bin/logctl search --service=payment-api --level=ERROR --last=1h

# Run benchmark
./bin/logctl benchmark --rate=1000 --duration=5m

# Inspect DLQ
./bin/logctl dlq inspect --limit=10
```

### loadgen

```bash
# Build
go build -o bin/loadgen ./cmd/loadgen

# Basic run
./bin/loadgen --rate=500 --duration=60s

# High volume stress test
./bin/loadgen --rate=5000 --duration=10m --level=mixed

# Error-only flood
./bin/loadgen --rate=100 --duration=1m --level=ERROR
```

---

## 7. Troubleshooting

### Problem: Kafka container exits immediately

**Symptoms:** `docker ps` shows Kafka container as `Exited (1)`

**Cause:** KRaft mode requires explicit node ID and quorum voters.

**Fix:**
```bash
docker compose -f deployments/docker-compose.yml down -v
docker compose -f deployments/docker-compose.yml up -d kafka
sleep 10
docker logs log-platform-kafka-1
```

Check for `KAFKA_NODE_ID` and `KAFKA_CONTROLLER_QUORUM_VOTERS` in `docker-compose.yml`.

### Problem: Elasticsearch shows `cluster_health: red`

**Symptoms:** `curl localhost:9200/_cluster/health` returns `"status":"red"`

**Cause:** ES needs time to initialize. Single-node with no replicas may show yellow initially.

**Fix:**
```bash
# Wait 60 seconds
curl -s http://localhost:9200/_cluster/health | jq

# If still red, check logs
docker logs log-platform-elasticsearch-1

# Common fix: increase Docker memory limit to 4GB+
```

### Problem: "service does not exist" when sending logs

**Symptoms:** Ingestor returns `400` with `service 'payment-api' does not exist`

**Cause:** PostgreSQL migrations not run, or seed data not inserted.

**Fix:**
```bash
go run ./cmd/migrate
go run ./cmd/seed
./bin/logctl service list
```

### Problem: Processor not consuming messages

**Symptoms:** Kafka lag grows, Redis counters stay at 0, ES has no logs

**Cause:** Processor not running, or consumer group not joining.

**Fix:**
```bash
# Check processor is running
pgrep -f "go run ./cmd/processor"

# Check consumer group
docker exec log-platform-kafka-1 kafka-consumer-groups.sh   --bootstrap-server localhost:9092   --describe   --group log-processors

# If lag is high but no active members, restart processor
```

### Problem: Kibana shows "No matching indices"

**Symptoms:** Kibana Discover shows no data

**Cause:** Index pattern not created, or no logs indexed yet.

**Fix:**
```bash
# 1. Send at least one log
curl -X POST http://localhost:8081/api/v1/logs   -H "Content-Type: application/json"   -d '{...}'

# 2. Wait 5 seconds for processor flush

# 3. In Kibana:
#    - Stack Management → Index Patterns → Create index pattern
#    - Pattern: logs-v1-*
#    - Time field: timestamp
#    - Save

# 4. Discover → Select logs-v1-* → Set time range to "Last 15 minutes"
```

### Problem: Port already in use

**Symptoms:** `bind: address already in use` when starting a service

**Fix:**
```bash
# Find process using port
lsof -i :8081

# Kill it
kill -9 <PID>

# Or use different port
INGESTOR_ADDR=:8083 go run ./cmd/ingestor
```

---

## 8. Environment Variables

All services read configuration from environment variables.

### Ingestor

| Variable | Default | Description |
|----------|---------|-------------|
| `INGESTOR_ADDR` | `:8081` | HTTP listen address |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC` | `app-logs` | Topic to publish to |
| `POSTGRES_URL` | `postgres://postgres:postgres@localhost:5432/logplatform?sslmode=disable` | PostgreSQL connection string |
| `REDIS_ADDR` | `localhost:6379` | Redis address (for rate limiting) |
| `LOG_LEVEL` | `info` | slog level: debug, info, warn, error |

### Processor

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC` | `app-logs` | Topic to consume from |
| `KAFKA_GROUP` | `log-processors` | Consumer group ID |
| `KAFKA_DLQ_TOPIC` | `app-logs-dlq` | Dead letter queue topic |
| `ES_URL` | `http://localhost:9200` | Elasticsearch URL |
| `ES_INDEX_PREFIX` | `logs-v1` | Index name prefix |
| `ES_BULK_SIZE` | `100` | Documents per bulk flush |
| `ES_FLUSH_INTERVAL` | `5s` | Max time between flushes |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `LOG_LEVEL` | `info` | slog level |

### Analytics

| Variable | Default | Description |
|----------|---------|-------------|
| `ANALYTICS_ADDR` | `:8082` | HTTP listen address |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `ES_URL` | `http://localhost:9200` | Elasticsearch URL |
| `POSTGRES_URL` | `postgres://postgres:postgres@localhost:5432/logplatform?sslmode=disable` | PostgreSQL connection string |
| `LOG_LEVEL` | `info` | slog level |

---

## 9. Development Workflow

### Typical Session

```bash
# 1. Start infrastructure (if not running)
docker compose -f deployments/docker-compose.yml up -d

# 2. Run migrations (only if schema changed)
go run ./cmd/migrate

# 3. Start services (3 terminals)
# Terminal 1: go run ./cmd/processor
# Terminal 2: go run ./cmd/analytics
# Terminal 3: go run ./cmd/ingestor

# 4. Test
curl http://localhost:8081/health
curl http://localhost:8082/health
./bin/loadgen --rate=100 --duration=30s

# 5. Inspect
curl http://localhost:8082/metrics/live | jq
./bin/logctl search --service=payment-api --last=5m

# 6. Shutdown
# Ctrl+C in each service terminal
# docker compose -f deployments/docker-compose.yml down
```

### Adding a New Feature

1. Update `03-api-spec.md` if changing interfaces
2. Update `04-data-model.md` if changing schemas
3. Write code in `internal/` or `cmd/`
4. Add tests in `*_test.go` files
5. Update `06-runbook.md` if adding new commands
6. Record decision in `07-development-log.md`

---

## 10. Performance Baselines

Measured on development hardware. Update these after running benchmarks.

| Metric | Target | How to Measure |
|--------|--------|----------------|
| Ingestion latency (p99) | < 50ms | `loadgen` output |
| Metrics API latency | < 10ms | `curl -w "%{time_total}" localhost:8082/metrics/live` |
| Search API latency (1h) | < 200ms | `curl -w "%{time_total}" "localhost:8082/search?q=timeout"` |
| Processor throughput | > 500 logs/sec | `loadgen --rate=500` with zero errors |
| ES bulk flush time | < 100ms | Processor logs |

---

*This runbook is a living document. If you encounter a problem not listed here, add the solution and commit it.*
