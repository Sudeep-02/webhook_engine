# 🪝 Async-Webhook-Engine (AWE)

> **A Distributed, High-Throughput Event Delivery System** built with Python asyncio.  
> Designed for **at-least-once delivery**, **horizontal scalability**, and **observability at scale**.

[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.135-green.svg)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/postgresql-18-blue.svg)](https://www.postgresql.org)
[![Redis](https://img.shields.io/badge/redis-8-red.svg)](https://redis.io)

---

## 📐 System Architecture

```mermaid
graph TB
    Client[Client Application] -->|POST /events| API[FastAPI Ingest Layer]
    
    API -->|1. Idempotency Check| Redis[(Redis Cache)]
    API -->|2. Persist Event| PG[(PostgreSQL)]
    API -->|3. Enqueue| RedisQueue[Redis Sorted Set Queue]
    
    RedisQueue -->|Dequeue| Worker1[Worker Pool]

    
    Worker1 -->|Fetch Subscription| Redis
    Worker1 -->|HTTP POST + HMAC| Target[Target Endpoint]
    Worker1 -->|Record Attempt| PG

    PG -.->|Recovery Sweeper| RedisQueue
    API -->|Traces| Jaeger[Jaeger/Otel]
```

---

## 🏗️ Architecture Design

### Core Components

| Component | Technology | Purpose | Scale Strategy |
|-----------|-----------|---------|----------------|
| **API Layer** | FastAPI + uvicorn | HTTP ingest, idempotency, validation | Horizontal (stateless) |
| **Queue** | Redis Sorted Sets | Delayed execution, atomic dequeue | Vertical + sharding |
| **Workers** | asyncio + httpx | Async delivery with connection pooling | Horizontal (stateless) |
| **Persistence** | PostgreSQL (partitioned) | Event storage, audit trail | Vertical + partitioning |
| **Cache** | Redis + L1 memory | Subscription lookup optimization | Vertical + TTL |

### Deployment Modes

```
┌─────────────────────────────────────────────────────────────┐
│  API Node (APP_MODE=api)                                    │
│  ├── HTTP endpoints (/events, /health, /metrics)            │
│  ├── Partition maintenance (cron: daily)                    │
│  ├── Recovery sweeper (every 60s)                           │
│  └── No worker threads                                      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Worker Node (APP_MODE=worker)                              │
│  ├── Delivery workers (configurable concurrency)            │
│  ├── No HTTP server                                         │
│  ├── Graceful shutdown with buffer flush                    │
│  └── Scale based on queue depth                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 🗄️ Database Design & Indexing Strategy

### Schema Overview

**subscriptions** (Non-partitioned, small dataset)
- `id` (SERIAL PRIMARY KEY)
- `event_type` (VARCHAR 50, UNIQUE, INDEXED)
- `target_url` (VARCHAR 255)
- `secret` (VARCHAR 255)
- `is_active` (BOOLEAN)
- `created_at` (TIMESTAMPTZ)

**webhook_events** (Partitioned by RANGE created_at)
- `id` (UUIDv7, PRIMARY KEY part 1)
- `created_at` (TIMESTAMPTZ, PRIMARY KEY part 2, PARTITION KEY)
- `idempotency_key` (VARCHAR, indexed with created_at)
- `event_type` (VARCHAR 50)
- `payload` (JSONB)
- `is_delivered` (BOOLEAN, default FALSE)
- `is_failed` (BOOLEAN, default FALSE)

**delivery_attempts** (Partitioned by RANGE created_at)
- `id` (UUIDv7, PRIMARY KEY part 1)
- `created_at` (TIMESTAMPTZ, PRIMARY KEY part 2, PARTITION KEY)
- `event_id` (UUID, foreign key reference)
- `attempt_number` (INTEGER)
- `http_status` (INTEGER)
- `response_body` (JSONB)
- `error_message` (TEXT)

### Partitioning Strategy


**Query Pruning**: Time-range queries automatically skip irrelevant partitions  
**Efficient Retention**: `DROP TABLE partition_name CASCADE` vs expensive `DELETE` operations  
**Index Locality**: Smaller per-partition indexes fit in memory → faster scans  
**Write Isolation**: Hot partitions absorb writes; cold partitions archived

**Daily Partition Creation:**
```
webhook_events_2026_04_06: VALUES FROM '2026-04-06 00:00:00+00' TO '2026-04-07 00:00:00+00'
webhook_events_2026_04_07: VALUES FROM '2026-04-07 00:00:00+00' TO '2026-04-08 00:00:00+00'
webhook_events_2026_04_08: VALUES FROM '2026-04-08 00:00:00+00' TO '2026-04-09 00:00:00+00'
```

**Retention Policy**: Auto-drop partitions older than 7 days via cron job

### Indexing Deep Dive

| Index Name | Table | Columns | Type | Purpose |
|------------|-------|---------|------|---------|
| `idx_webhook_idempotency` | webhook_events | idempotency_key, created_at | UNIQUE | Enforce idempotency at DB layer (fallback) |
| `idx_webhook_events_pending` | webhook_events | created_at | PARTIAL (`WHERE is_delivered=FALSE AND is_failed=FALSE`) | Fast recovery sweeper queries |
| `idx_subscriptions_event_type` | subscriptions | event_type | UNIQUE | O(1) subscription lookup |

**Critical Query Pattern**: All queries must include `created_at >= search_window` filter to enable partition pruning and avoid full table scans across all historical partitions.

---

## ⚡ Performance Optimizations

### Multi-Layer Caching Hierarchy

**Subscription Lookup Flow:**
```
Request: Get subscription for event_type
        │
        ▼
┌─────────────────────────────────┐
│  L1 Cache (Memory)              │
│  • OrderedDict LRU              │
│  • 500 entries max              │
│  • 60s TTL                      │
└───────────────┬─────────────────┘
                │
   ┌────────────┴────────────┐
   │ HIT (95%)               │ MISS (5%)
   ▼                         ▼
Return cached           ┌─────────────────────────┐
subscription            │  L2 Cache (Redis)       │
                        │  • SETEX                │
                        │  • 1h TTL               │
                        └───────────┬─────────────┘
                                    │
                           ┌────────┴────────┐
                           │ HIT (4%)        │ MISS (1%)
                           ▼                 ▼
                    Populate L1 +      ┌──────────────┐
                    return             │ PostgreSQL   │
                                       │ • Indexed    │
                                       │ • Fallback   │
                                       └──────┬───────┘
                                              │
                                       Populate L1+L2
                                       + return
```

**Attempt Count Caching:**
- Cache key: `attempts:{event_id}`
- TTL: 300 seconds
- Avoids repeated `COUNT(*)` aggregation queries on `delivery_attempts`
- Falls back to DB query on cache miss/error

### Queue Design: Redis Sorted Sets

**Why Sorted Sets over Message Brokers?**
 Native delayed execution via timestamp scores  
 Atomic dequeue via Lua script (prevents race conditions)  
 Simple operations: `ZADD`, `ZRANGE`, `ZREM`  
 Lower operational overhead for target scale  

**Enqueue with delay:**
```
Score = current_timestamp + delay_seconds
Member = event_id (UUID string)
Operation: ZADD queue:delivery {member: score}
```

**Atomic Dequeue (Lua script):**
1. Get first element with `score <= now`
2. If exists: `ZREM` and `RETURN` member
3. Otherwise: return `nil`

This guarantees exactly-one claim per event across concurrent workers.

### HTTP Delivery Optimizations

**Connection Pooling:**
- Shared `httpx.AsyncClient` across all worker tasks
- `max_connections`: 1000 | `max_keepalive_connections`: 200
- Reuses TCP connections to same target endpoints

**Timeout Strategy:**
| Timeout | Value | Purpose |
|---------|-------|---------|
| Connect | 5s | DNS resolution + TCP handshake |
| Read | 10s | Wait for response body |
| Write | 10s | Send request body (large payloads) |
| Pool | 10s | Wait for available connection from pool |

**Signature Verification:**
- `HMAC-SHA256` on canonicalized JSON (`sort_keys=True`)
- Header: `X-Webhook-Signature: sha256={signature}`
- Enables target endpoints to verify payload authenticity and prevent tampering

---

## 🔁 Reliability Patterns

### Idempotency Flow

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Redis
    participant PG as PostgreSQL
    
    Client->>API: POST /events (idempotency_key)
    API->>Redis: SET NX idempotency:{key} {event_id}
    
    alt Key already exists (Duplicate)
        Redis-->>API: nil
        API-->>Client: 200 {idempotent: true}
    else First request
        Redis-->>API: OK
        API->>PG: INSERT webhook_event
        API->>Redis: ZADD queue:delivery
        API-->>Client: 201 {id: event_id}
    end
```

**Fail-Open Strategy**: On Redis error, allow processing (prefer duplicate over dropped event). Mitigated by DB-level unique constraint on `(idempotency_key, created_at)` as backup.

### Retry Strategy: Exponential Backoff + Full Jitter

```
Attempt 1: delay = random(0, 2s)
Attempt 2: delay = random(0, 4s)
Attempt 3: delay = random(0, 8s)
Attempt 4: delay = random(0, 16s)
Attempt 5: delay = random(0, 32s)
Max: 300s cap
```

**Retry-After Header Priority**: If target returns `Retry-After: 60`, respect it instead of calculated backoff (capped at `MAX_DELAY`).

**Retryable Status Codes**: 429,  5xx,  0 (network timeout)  
**Terminal Status Codes**:  400,  401,  403,  404 (client errors dropped immediately)

### Recovery Service (Crash Consistency)

**Purpose**: Bridge gap between Redis (speed layer) and PostgreSQL (source of truth) during crashes, network partitions, or Redis restarts.

**Flow** (runs every 60s):
1. Query PostgreSQL for events where:
   - `is_delivered = FALSE` AND `is_failed = FALSE`
   - `created_at < now - 2 minutes` (should have been processed)
   - `created_at >= now - 24 hours` (partition pruning)
2. Re-enqueue stuck events to Redis queue
3. Log recovery actions for audit

**Trade-off**: Small window of potential duplicate processing (mitigated by idempotency layer).

---

## 📊 Observability

### Metrics (Prometheus)

| Metric | Type | Labels | SLO Target |
|--------|------|--------|------------|
| `webhook_received_total` | Counter | event_type | Track ingest rate |
| `webhook_delivered_total` | Counter | event_type | >99% success rate |
| `webhook_failed_total` | Counter | event_type, reason | <1% failure rate |
| `delivery_latency_histogram` | Histogram | event_type | p95 < 1s |
| `queue_depth_gauge` | Gauge | - | Alert if >5000 |
| `redis_command_latency_seconds` | Histogram | command, status | p99 < 50ms |

### Structured Logging

Every operation logs:
- **correlation_id**: Distributed tracing across async boundaries
- **phase_latency**: Breakdown of time spent in each phase (dequeue, DB fetch, HTTP delivery, etc.)
- **context**: event_id, event_type, attempt_number, http_status

**Example log structure:**
```json
{
  "event": "webhook_delivered",
  "correlation_id": "req_abc123",
  "event_id": "018abc-def0-7000-8000-123456789abc",
  "attempt": 2,
  "status": 200,
  "latency_total_ms": 125.4,
  "latency_dequeue_ms": 2.1,
  "latency_db_fetch_ms": 15.3,
  "latency_http_delivery_ms": 89.2,
  "latency_db_write_ms": 10.1
}
```

### Distributed Tracing

**OpenTelemetry → Jaeger integration:**
- Auto-instrumentation: FastAPI routes, httpx client, SQLAlchemy queries
- Manual spans: Business logic (idempotency check, delivery attempt)
- Trace propagation: Correlation ID passed from API → Worker → HTTP request

---

## 🔑 Key Design Decisions & Trade-offs

| Decision | Rationale | Alternative | Trade-off |
|----------|-----------|-------------|-----------|
| **PostgreSQL RANGE partitioning** | Efficient time-based retention & query pruning; native PG support | TimescaleDB / No partitioning | Must include `created_at` in queries for pruning |
| **UUIDv7 for event IDs** | Time-sortable → better insert locality; distributed-safe | ULID / Snowflake IDs | 16 bytes vs 8 bytes (negligible at scale) |
| **Redis Sorted Sets queue** | Native delayed execution; atomic Lua dequeue; simple ops | RabbitMQ / Kafka | Redis single point of failure (mitigated by recovery) |
| **L1+L2 caching** | Reduces Redis round-trips for hot keys; L1 eviction prevents memory bloat | Redis-only / No cache | Cache invalidation complexity (mitigated by short TTLs) |
| **Fail-open idempotency** | Availability > strict consistency for non-financial webhooks | Fail-closed | Small window of duplicate processing |
| **Separate api/worker processes** | Independent scaling; workers avoid FastAPI overhead | Unified process | Slightly more complex deployment |
| **Partial index on pending events** | Recovery sweeper scans ~10% of rows vs. 100% | Full index | Minimal index maintenance overhead on writes |
| **Full jitter backoff** | Prevents thundering herd when upstream recovers | Fixed backoff | Less predictable retry timing (acceptable for async) |

---

## 🚀 Deployment & Scaling

### Environment Configuration
```env
# Core
APP_MODE=api|worker
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/db
REDIS_URL=redis://host:6379/0

# Worker Tuning
WORKER_CONCURRENCY=100
DB_POOL_SIZE=20
WORKER_BATCH_SIZE=100
WORKER_FLUSH_INTERVAL=5.0

# Caching
SUB_L1_CACHE_SIZE=500
SUB_L1_CACHE_TTL=60
SUB_CACHE_TTL=3600
ATTEMPT_COUNT_CACHE_TTL=300

# Retry & Polling
ERROR_BACKOFF_BASE=2.0
ERROR_BACKOFF_MAX=30.0
WORKER_POLL_BASE=0.05
WORKER_POLL_MAX=2.0

# Retention
PARTITION_RETENTION_DAYS=7
```

### Scaling Guidelines
**API Nodes**: Scale horizontally based on ingest rate (target: <100ms p95 response time)  
**Worker Nodes**: Scale based on `queue_depth_gauge` metric (Warning: >1000, Critical: >5000)  
**PostgreSQL**: Monitor partition size (<1M rows/partition), tune `DB_POOL_SIZE`, consider read replicas for analytics  
**Redis**: Monitor memory usage, enable AOF persistence, consider Redis Cluster for sharding if queue >100K events

---

## 📦 Tech Stack

| Category | Technology | Version |
|----------|-----------|---------|
| **Framework** | FastAPI | 0.135.2 |
| **Server** | uvicorn + uvloop | 0.42.0 |
| **HTTP Client** | httpx | 0.28.1 |
| **ORM** | SQLAlchemy (async) | 2.0.48 |
| **DB Driver** | asyncpg | 0.31.0 |
| **Cache/Queue** | redis | 7.4.0 |
| **UUIDs** | uuid7 | 2025.0.1 |
| **Metrics** | prometheus_client | 0.21.1 |
| **Tracing** | OpenTelemetry | 1.40.0 |
| **Logging** | structlog | 24.4.0 |
| **Testing** | pytest + pytest-asyncio | 9.0.2 |
| **Load Testing** | locust | 2.33.0 |

---

## 🎯 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| **Throughput** | 23,000 events/min | Sustained load |
| **Ingest Latency** | p99 < 100ms | API response time |
| **Delivery Latency** | p99 < 1s | Event → HTTP POST |
| **Success Rate** | >99% | Delivered vs failed |
| **Recovery Time** | <2 minutes | Stuck event detection |
| **Cache Hit Rate** | >95% | Subscription lookups |

---


