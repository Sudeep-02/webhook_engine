import asyncio
import random
import time
import json
import os
from uuid import UUID
from datetime import datetime, timedelta, timezone
from collections import OrderedDict
from typing import Optional
from sqlalchemy import select, func, update

from app.database import AsyncSessionLocal
from app.models import WebhookEvent, DeliveryAttempt, Subscription
from app.services.queue_service import QueueService
from app.services.retry_strategy import RetryStrategy
from app.services.webhook_delivery import WebhookDeliveryService
from app.redis_client import redis_client

# --- Infrastructure Imports ---
from app.core.logging_config import logger
from app.core.metrics import (
    webhook_delivered_total,
    webhook_failed_total,
    webhook_retry_total,
    delivery_latency_histogram,
    active_workers_gauge,
)


class DeliveryWorker:
    
    POLL_INTERVAL_BASE = float(os.getenv("WORKER_POLL_BASE", "0.05"))  # Base poll interval
    POLL_INTERVAL_MAX = float(os.getenv("WORKER_POLL_MAX", "2.0"))     # Max backoff
    BATCH_SIZE = int(os.getenv("WORKER_BATCH_SIZE", "100"))
    FLUSH_INTERVAL = float(os.getenv("WORKER_FLUSH_INTERVAL", "5.0"))
    SUBSCRIPTION_CACHE_TTL = int(os.getenv("SUB_CACHE_TTL", "3600"))
    CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "100"))
    
    
    ERROR_BACKOFF_BASE = float(os.getenv("ERROR_BACKOFF_BASE", "2.0"))
    ERROR_BACKOFF_MAX = float(os.getenv("ERROR_BACKOFF_MAX", "30.0"))
    

    L1_CACHE_MAX_SIZE = int(os.getenv("SUB_L1_CACHE_SIZE", "500"))
    L1_CACHE_TTL_SECONDS = int(os.getenv("SUB_L1_CACHE_TTL", "60"))  # Short TTL; Redis is source of truth

    #6: Redis cache TTL for attempt counts (seconds)
    ATTEMPT_COUNT_CACHE_TTL = int(os.getenv("ATTEMPT_COUNT_CACHE_TTL", "300"))
    
    def __init__(self):
        self.running = False

        #  Use asyncio.Queue for lock-free buffering (replaces list + lock)
        self.success_queue: asyncio.Queue = asyncio.Queue()
        self.failure_queue: asyncio.Queue = asyncio.Queue()
        # buffer_lock removed - Queue is thread-safe for async
        
        #  Track flush tasks to avoid fire-and-forget
        self._active_flush_tasks = set()
        
        #  In-memory L1 cache for hot subscription keys
        self._sub_cache: OrderedDict = OrderedDict()
        self._sub_cache_lock = asyncio.Lock()
        
        #  Adaptive polling state
        self._current_poll_interval = self.POLL_INTERVAL_BASE
        
        #  Semaphore to limit concurrent DB sessions (prevents pool exhaustion)
        pool_size = int(os.getenv("DB_POOL_SIZE", "20"))  # ← Removed duplicate import
        self._db_semaphore = asyncio.Semaphore(max(1, pool_size - 5)) 
        
        
        
    @staticmethod
    def _log_phase(phase_name: str, start: float, level: str = "debug", **extra):
        """Log latency for any phase in ms with structured fields."""
        latency_ms = round((time.time() - start) * 1000, 2)
        log_data = {
            "phase": phase_name,
            "latency_ms": latency_ms,
            **extra
        }
        if level == "info":
            logger.info("phase_latency", **log_data)
        elif level == "warning" and latency_ms > 100:
            logger.warning("phase_latency_slow", **log_data)
        else:
            logger.debug("phase_latency", **log_data)
        return latency_ms

        
    async def _sub_cache_get(self, event_type: str) -> Optional[dict]:
        """Get subscription from L1 cache if fresh."""
        async with self._sub_cache_lock:
            entry = self._sub_cache.get(event_type)
            if entry:
                data, expires_at = entry
                if time.time() < expires_at:
                    self._sub_cache.move_to_end(event_type)  # LRU
                    return data
                else:
                    del self._sub_cache[event_type]  # Expired
        return None

    async def _sub_cache_set(self, event_type: str, data: dict):  # ← FIXED: Added 'data:' parameter name
        """Set subscription in L1 cache with TTL."""
        async with self._sub_cache_lock:
            if len(self._sub_cache) >= self.L1_CACHE_MAX_SIZE:
                self._sub_cache.popitem(last=False)  # LRU eviction
            self._sub_cache[event_type] = (data, time.time() + self.L1_CACHE_TTL_SECONDS)

    async def _sub_cache_cleanup(self):
        """Remove expired L1 entries. Call periodically."""
        now = time.time()
        async with self._sub_cache_lock:
            expired = [k for k, (_, exp) in self._sub_cache.items() if exp < now]
            for k in expired:
                del self._sub_cache[k]
            if expired:
                logger.debug("sub_l1_cache_cleanup", removed=len(expired))
             
    async def _get_attempt_count_cached(self, event_id: str, db, search_window: datetime) -> int:
        """
        Get attempt count with Redis caching to avoid repeated DB queries.
        
        Flow:
        1. Try Redis cache first (fast path)
        2. If miss, query DB (slow path)
        3. Cache result in Redis with short TTL
        4. Return count
        """
        cache_key = f"attempts:{event_id}"
        
        # Try Redis first
        try:
            cached = await redis_client.get(cache_key)
            if cached is not None:
                return int(cached)
        except Exception as e:
            logger.warning("attempt_count_cache_read_error", event_id=event_id, error=str(e))
            # Fall through to DB query on cache error
        
        # Cache miss: query DB
        try:
            attempts_query = (
                select(func.count())
                .select_from(DeliveryAttempt)
                .where(
                    DeliveryAttempt.event_id == UUID(event_id) if isinstance(event_id, str) else event_id,
                    DeliveryAttempt.created_at >= search_window,
                )
            )
            count_result = await db.execute(attempts_query)
            res = count_result.scalar()
            count = int((res or 0) + 1)
        except Exception as e:
            logger.warning("attempt_count_db_query_error", event_id=event_id, error=str(e))
            return 1  # Safe fallback
        
        # Cache result in Redis with short TTL
        try:
            await redis_client.setex(cache_key, self.ATTEMPT_COUNT_CACHE_TTL, str(count))
        except Exception as e:
            logger.warning("attempt_count_cache_write_error", event_id=event_id, error=str(e))
            # Non-fatal: we still return the correct count
        
        return count
    
    
    
    async def start(self):
        self.running = True
        active_workers_gauge.set(1)
        logger.info(
            "worker_started",
            message="Delivery worker started",
            concurrency=self.CONCURRENCY,
            poll_interval_base=self.POLL_INTERVAL_BASE,
            db_pool_hint="Ensure DB_POOL_SIZE >= CONCURRENCY to avoid contention",
            buffering="asyncio.Queue (lock-free)"  # ← Note new buffering strategy
        )

    
        # Start background tasks
        flush_task = asyncio.create_task(self._timer_flush())
        cache_cleanup_task = asyncio.create_task(self._timer_cache_cleanup())
        
        workers = [asyncio.create_task(self._worker_loop()) for _ in range(self.CONCURRENCY)]
        
        try:
            await asyncio.gather(*workers)
        finally:
            await self.stop()
            for task in [flush_task, cache_cleanup_task]:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _worker_loop(self):
        """Persistent worker with adaptive polling and error backoff."""
        worker_id = id(asyncio.current_task())
        while self.running:
            try:
                await self.process_one(worker_id)
                self._current_poll_interval = self.POLL_INTERVAL_BASE  # Reset on success
            except Exception as e:
                logger.error("worker_critical_error", worker_id=worker_id, error=str(e), exc_info=True)
                backoff = min(
                    self.ERROR_BACKOFF_MAX,
                    self.ERROR_BACKOFF_BASE * (1 + random.random())  # Jitter
                )
                await asyncio.sleep(backoff)

    async def _timer_flush(self):
        """Background loop to flush buffers periodically."""
        while self.running:
            await asyncio.sleep(self.FLUSH_INTERVAL)
            await self._flush_to_db_tracked()

    async def _timer_cache_cleanup(self):
        """Background loop to clean expired L1 cache entries."""
        while self.running:
            await asyncio.sleep(self.L1_CACHE_TTL_SECONDS // 2)
            await self._sub_cache_cleanup()
            
            
    async def stop(self):
        self.running = False
        logger.info("worker_shutdown_initiated", message="Flushing buffers...")
        await self._flush_to_db_tracked()
        await WebhookDeliveryService.close()
        active_workers_gauge.set(0)
        logger.info("worker_stopped", message="Delivery worker stopped")
        
        
    async def _flush_to_db_tracked(self):
        """Flush with task tracking and error handling."""
        flush_task = asyncio.create_task(self.flush_to_db())
        self._active_flush_tasks.add(flush_task)
        flush_task.add_done_callback(self._active_flush_tasks.discard)
        
        if len(self._active_flush_tasks) > 10:
            done = [t for t in self._active_flush_tasks if t.done()]
            for t in done:
                self._active_flush_tasks.discard(t)
                try:
                    await t
                except Exception as e:
                    logger.error("flush_task_failed", error=str(e))
                    
    async def get_subscription(self, event_type: str, db):
        # Check L1 cache first
        cache_start = time.time()
        cached = await self._sub_cache_get(event_type)
        if cached:
            self._log_phase("sub_l1_cache_hit", cache_start, level="info", event_type=event_type)
            return cached

        # Cache miss: check Redis
        cache_key = f"sub_cache:{event_type}"
        redis_start = time.time()
        try:
            cached_sub = await redis_client.get(cache_key)
            redis_latency = self._log_phase("redis_get", redis_start, cache_key=cache_key, hit=(cached_sub is not None))
            if cached_sub:
                sub_data = json.loads(cached_sub)
                await self._sub_cache_set(event_type, sub_data)
                return sub_data
        except Exception as e:
            redis_latency = self._log_phase("redis_get_error", redis_start, cache_key=cache_key, error=str(e))
            logger.error("cache_read_error", error=str(e), latency_ms=redis_latency)

        # Cache Miss: Query Postgres
        db_start = time.time()
        try:
            stmt = select(Subscription).where(Subscription.event_type == event_type)
            result = await db.execute(stmt)
            subscription = result.scalar_one_or_none()
            db_latency = self._log_phase(
                "db_subscription_query", db_start,
                event_type=event_type, found=(subscription is not None)
            )
        except Exception as e:
            db_latency = self._log_phase("db_subscription_query_error", db_start, event_type=event_type, error=str(e))
            logger.error("subscription_query_error", error=str(e), latency_ms=db_latency)
            return None
        
        if subscription:
            sub_data = {
                "target_url": subscription.target_url,
                "secret": subscription.secret,
                "event_type": subscription.event_type,
            }
            redis_write_start = time.time()
            try:
                await redis_client.setex(cache_key, self.SUBSCRIPTION_CACHE_TTL, json.dumps(sub_data))
                self._log_phase("redis_setex", redis_write_start, cache_key=cache_key)
            except Exception as e:
                logger.error("cache_write_error", error=str(e))
            await self._sub_cache_set(event_type, sub_data)
            return sub_data
        return None

    async def process_one(self, worker_id: int):
        dequeue_start = time.time()
        event_id = await QueueService.dequeue()
        dequeue_latency = self._log_phase("queue_dequeue", dequeue_start, worker_id=worker_id)

        if not event_id:
            self._current_poll_interval = min(
                self.POLL_INTERVAL_MAX,
                self._current_poll_interval * 1.5
            )
            await asyncio.sleep(self._current_poll_interval)
            return

        event_uuid = UUID(event_id) if isinstance(event_id, str) else event_id
        overall_start = time.time() 
        logger.debug("processing_event", event_id=event_id, worker_id=worker_id, dequeue_latency_ms=dequeue_latency)
        
        search_window = datetime.now(timezone.utc) - timedelta(days=1)
        async with self._db_semaphore:
            async with AsyncSessionLocal() as db:
                db_fetch_start = time.time()
                stmt = select(WebhookEvent).where(
                    WebhookEvent.id == event_uuid,
                    WebhookEvent.created_at >= search_window,
                )
                result = await db.execute(stmt)
                event = result.scalar_one_or_none()
                db_fetch_latency = self._log_phase(
                    "db_event_fetch", db_fetch_start,
                    event_id=event_id, found=(event is not None)
                )

                if not event or event.is_delivered:
                    self._log_phase("event_skip", overall_start, event_id=event_id, reason="not_found_or_delivered")
                    return

                sub_lookup_start = time.time()
                subscription = await self.get_subscription(event.event_type, db)
                sub_lookup_latency = self._log_phase(
                    "subscription_lookup_total", sub_lookup_start,
                    event_id=event_id, event_type=event.event_type, found=(subscription is not None)
                )
                
                
                if not subscription:
                    logger.warning(
                        "subscription_not_found",
                        event_type=event.event_type, event_id=event_id,
                        latency_ms=self._log_phase("process_fail", overall_start, event_id=event_id, reason="no_subscription")
                    )
                    webhook_failed_total.labels(event_type=event.event_type, reason="subscription_not_found").inc()
                    # Use Queue
                    await self.failure_queue.put(event_id)
                    return

                #Use Redis-cached attempt count
                attempt_count_start = time.time()
                attempt_number = await self._get_attempt_count_cached(event_id, db, search_window)
                attempt_count_latency = self._log_phase(
                    "db_attempt_count", attempt_count_start,
                    event_id=event_id, attempt_number=attempt_number
                )

                delivery_start = time.time()
                status, resp_body, error, resp_headers = await WebhookDeliveryService.deliver(
                    url=subscription["target_url"],
                    payload=event.payload,
                    secret=subscription["secret"],
                    event_type=event.event_type,
                )
                delivery_latency = self._log_phase(
                    "http_delivery", delivery_start,
                    event_id=event_id, url=subscription["target_url"], status=status
                )

                http_status = int(status) if status is not None else 0

                db_write_start = time.time()
                db.add(
                    DeliveryAttempt(
                        event_id=event_uuid,
                        created_at=datetime.now(timezone.utc),
                        attempt_number=attempt_number,
                        http_status=http_status,
                        response_body=resp_body if resp_body else None,
                        error_message=error,
                    )
                )
                await db.commit()
                db_write_latency = self._log_phase(
                    "db_attempt_insert_commit", db_write_start,
                    event_id=event_id, attempt_number=attempt_number
                )

                total_latency = time.time() - overall_start
                delivery_latency_histogram.labels(event_type=event.event_type).observe(total_latency)


                latency_breakdown = {
                    "latency_total_ms": round(total_latency * 1000, 2),
                    "latency_dequeue_ms": round(dequeue_latency, 2),
                    "latency_db_fetch_ms": round(db_fetch_latency, 2),
                    "latency_subscription_ms": round(sub_lookup_latency, 2),
                    "latency_attempt_count_ms": round(attempt_count_latency, 2),
                    "latency_http_delivery_ms": round(delivery_latency, 2),
                    "latency_db_write_ms": round(db_write_latency, 2),
                }

           
            
                if 200 <= http_status < 300:
                    webhook_delivered_total.labels(event_type=event.event_type).inc()
                    #  Use Queue 
                    await self.success_queue.put(event_id)
                
                    if total_latency * 1000 > 1000:
                        logger.info(
                            "webhook_delivered_slow",
                            event_id=event_id,
                            attempt=attempt_number,
                            status=http_status,
                            **latency_breakdown,
                            hint="Check phase_latency logs for bottleneck"
                        )
                    else:
                        logger.info(
                            "webhook_delivered",
                            event_id=event_id,
                            attempt=attempt_number,
                            status=http_status,
                            **latency_breakdown
                        )

                elif RetryStrategy.should_retry(attempt_number, http_status):
                    delay = RetryStrategy.calculate_delay(attempt_number, resp_headers)
                    retry_enqueue_start = time.time()
                    await QueueService.enqueue(event_uuid, delay)
                    retry_enqueue_latency = self._log_phase(
                        "queue_retry_enqueue", retry_enqueue_start,
                        event_id=event_id, delay=delay
                    )
                    
                    webhook_retry_total.labels(
                        event_type=event.event_type, attempt_number=attempt_number
                    ).inc()
                    
                    logger.warning(
                        "webhook_retry_queued",
                        event_id=event_id,
                        attempt=attempt_number,
                        delay=delay,
                        status=http_status,
                        latency_retry_enqueue_ms=round(retry_enqueue_latency, 2),
                        **latency_breakdown
                    )
                else:
                    webhook_failed_total.labels(
                        event_type=event.event_type,
                        reason="max_retries" if attempt_number >= RetryStrategy.MAX_RETRIES else "status_failure",
                    ).inc()
                    # Queue 
                    await self.failure_queue.put(event_id)
                    
                    logger.error(
                        "webhook_dead_lettered",
                        event_id=event_id,
                        attempt=attempt_number,
                        status=http_status,
                        error=error,
                        **latency_breakdown
                    )

                # Check queue sizes 
                buffer_total = self.success_queue.qsize() + self.failure_queue.qsize()
                if buffer_total >= self.BATCH_SIZE:
                    flush_start = time.time()
                    await self._flush_to_db_tracked()
                    self._log_phase("flush_triggered_sync", flush_start, buffer_size=buffer_total)
                
                if random.random() < 0.01:
                    queue_stats = await QueueService.get_queue_stats()
                    if queue_stats["backpressure_warning"]:
                        logger.warning(
                            "queue_backpressure_warning",
                            **queue_stats,
                            hint="Consider scaling workers or reducing CONCURRENCY"
                        )

    #Flush with Queue draining + detailed timing
    async def flush_to_db(self):
        """Bulk update PostgreSQL with full latency breakdown."""
        #  Drain from asyncio.Queue 
        to_success = []
        to_fail = []
        
        # Drain success queue up to BATCH_SIZE
        while not self.success_queue.empty() and len(to_success) < self.BATCH_SIZE:
            try:
                item = self.success_queue.get_nowait()
                to_success.append(item)
                self.success_queue.task_done()
            except asyncio.QueueEmpty:
                break
            
            
            
            # Drain failure queue up to BATCH_SIZE
        while not self.failure_queue.empty() and len(to_fail) < self.BATCH_SIZE:
            try:
                item = self.failure_queue.get_nowait()
                to_fail.append(item)
                self.failure_queue.task_done()
            except asyncio.QueueEmpty:
                break

        if not to_success and not to_fail:
            return

        flush_start = time.time()
        try:
            async with self._db_semaphore:
                async with AsyncSessionLocal() as db:
                    db_ops_start = time.time()
                    
                    if to_success:
                        await db.execute(
                            update(WebhookEvent)
                            .where(WebhookEvent.id.in_(to_success))
                            .values(is_delivered=True)
                        )
                    if to_fail:
                        await db.execute(
                            update(WebhookEvent)
                            .where(WebhookEvent.id.in_(to_fail))
                            .values(is_failed=True)
                        )
                    
                    db_ops_latency = self._log_phase(
                        "db_bulk_update_execute", db_ops_start,
                        success_count=len(to_success), failure_count=len(to_fail)
                    )
                    
                    commit_start = time.time()
                    await db.commit()
                    commit_latency = self._log_phase("db_commit", commit_start)
                    
                    total_flush_latency = self._log_phase(
                        "bulk_update_complete", flush_start,
                        success=len(to_success), failed=len(to_fail),
                        db_ops_latency_ms=round(db_ops_latency, 2),
                        commit_latency_ms=round(commit_latency, 2)
                    )
                    
                    logger.info(
                        "bulk_update_complete",
                        success=len(to_success),
                        failed=len(to_fail),
                        total_flush_latency_ms=round(total_flush_latency, 2),
                    )
                    
                
        except Exception as e:
            flush_latency = self._log_phase("flush_error", flush_start, error=str(e))
            logger.error("flush_error", error=str(e), latency_ms=round(flush_latency, 2))
            # Optional: re-queue failed items
            # for item in to_success + to_fail:
            #     await self.failure_queue.put(item)