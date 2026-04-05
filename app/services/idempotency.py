# app/services/idempotency.py
import time
import uuid
import asyncio
from collections import OrderedDict
from typing import Optional

from prometheus_client import Histogram

from app.redis_client import redis_client
from app.core.logging_config import logger

# ─────────────────────────────────────────────────────────────
# Prometheus Metrics
# ─────────────────────────────────────────────────────────────
redis_command_latency = Histogram(
    "redis_command_latency_seconds",
    "Redis command latency in seconds",
    ["command", "status"],  # e.g., "set_nx", "get" + "success", "error"
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

idempotency_l1_cache_hits = Histogram(
    "idempotency_l1_cache_hits_total",
    "Count of L1 cache hits/misses",
    ["result"],  # "hit", "miss"
)


class IdempotencyService:
    """
    Idempotency service with:
    - Atomic Redis SET NX for distributed deduplication
    - In-memory L1 cache for hot keys (reduces Redis round-trips)
    - Prometheus metrics for observability
    - Fail-open strategy on Redis errors
    """

    TTL_SECONDS = 86400  # 24 hours
    PREFIX = "idempotency:"

    # ── L1 Cache Configuration ──
    _local_cache: OrderedDict = OrderedDict()
    _cache_lock = asyncio.Lock()
    _L1_CACHE_MAX_SIZE = 1000
    _L1_CACHE_TTL_SECONDS = 60  # Short TTL; Redis is source of truth

    @classmethod
    async def check_and_set(
        cls, idempotency_key: str, event_id: uuid.UUID
    ) -> bool:
        """
        Atomically check if key exists and set it if not.

        Returns:
            True  → First time seeing this key (✅ Green light to process)
            False → Key already exists (❌ Duplicate - stop here)

        Flow:
            1. Check L1 cache (microsecond lookup)
            2. If miss, check Redis with atomic SET NX
            3. Populate L1 cache on Redis success
            4. Record Prometheus metrics
        """
        redis_key = f"{cls.PREFIX}{idempotency_key}"
        event_id_str = str(event_id)

        # ── Step 1: Check L1 cache (fast path) ──
        async with cls._cache_lock:
            if idempotency_key in cls._local_cache:
                idempotency_l1_cache_hits.labels(result="hit").observe(1)
                logger.debug(
                    "idempotency_l1_cache_hit",
                    key=idempotency_key,
                )
                return False  # Already seen recently

        idempotency_l1_cache_hits.labels(result="miss").observe(1)

        # ── Step 2: Redis atomic check-and-set ──
        try:
            start = time.perf_counter()
            result = await redis_client.set(
                redis_key,
                event_id_str,
                nx=True,  # Set if Not Exists = atomic lock
                ex=cls.TTL_SECONDS,
            )
            latency = time.perf_counter() - start

            redis_command_latency.labels(
                command="set_nx", status="success"
            ).observe(latency)

            is_first_time = bool(result)

            # ── Step 3: Populate L1 cache on success ──
            if is_first_time:
                async with cls._cache_lock:
                    cls._local_cache[idempotency_key] = {
                        "event_id": event_id_str,
                        "expires_at": time.time() + cls._L1_CACHE_TTL_SECONDS,
                    }
                    # LRU eviction if cache grows too large
                    if len(cls._local_cache) > cls._L1_CACHE_MAX_SIZE:
                        cls._local_cache.popitem(last=False)

            return is_first_time

        except Exception as e:
            redis_command_latency.labels(
                command="set_nx", status="error"
            ).observe(time.perf_counter() - start)

            logger.warning(
                "idempotency_redis_error",
                key=idempotency_key,
                error=str(e),
                fallback="allow_processing",  # Fail-open strategy
            )
            # Allow processing on Redis error (better to process duplicate than drop request)
            return True

    @classmethod
    async def get_existing_event_id(
        cls, idempotency_key: str
    ) -> Optional[str]:
        """
        Fetch the original event_id for a duplicate request.
        Used to return the original response on idempotent replays.

        Returns:
            event_id (str) if found, None otherwise
        """
        redis_key = f"{cls.PREFIX}{idempotency_key}"

        try:
            start = time.perf_counter()
            val = await redis_client.get(redis_key)
            latency = time.perf_counter() - start

            redis_command_latency.labels(
                command="get", status="success"
            ).observe(latency)

            # decode_responses=True means val is already str or None
            return val  # type: ignore

        except Exception as e:
            redis_command_latency.labels(
                command="get", status="error"
            ).observe(time.perf_counter() - start)

            logger.warning(
                "idempotency_cache_read_error",
                key=idempotency_key,
                error=str(e),
                fallback="treat_as_new",
            )
            return None

    @classmethod
    async def cleanup_expired_l1_cache(cls):
        """
        Background task to remove expired entries from L1 cache.
        Call this periodically (e.g., every 30s) from a background task.
        """
        now = time.time()
        async with cls._cache_lock:
            expired_keys = [
                key
                for key, data in cls._local_cache.items()
                if isinstance(data, dict)
                and data.get("expires_at", 0) < now
            ]
            for key in expired_keys:
                del cls._local_cache[key]

            if expired_keys:
                logger.debug(
                    "idempotency_l1_cache_cleanup",
                    removed_count=len(expired_keys),
                )

    @classmethod
    def get_cache_stats(cls) -> dict:
        """
        Return cache statistics for debugging/monitoring.
        """
        return {
            "l1_cache_size": len(cls._local_cache),
            "l1_cache_max": cls._L1_CACHE_MAX_SIZE,
            "l1_cache_ttl_seconds": cls._L1_CACHE_TTL_SECONDS,
            "redis_ttl_seconds": cls.TTL_SECONDS,
        }