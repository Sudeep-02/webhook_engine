import asyncio
import time
import json
from uuid import UUID
from datetime import datetime, timedelta, timezone

from app.database import AsyncSessionLocal
from app.models import WebhookEvent, DeliveryAttempt, Subscription
from app.services.queue_service import QueueService
from app.services.retry_strategy import RetryStrategy
from app.services.webhook_delivery import WebhookDeliveryService
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func, update
from sqlalchemy import update
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
    POLL_INTERVAL = 0.05
    BATCH_SIZE = 100  # for postgres
    FLUSH_INTERVAL = 5.0  # Force save every 5 seconds even if batch isn't full
    SUBSCRIPTION_CACHE_TTL = 3600  # Cache for 1 hour
    CONCURRENCY = 100  # Static worker pool size 

    def __init__(self):
        self.running = False

        # Buffers for Batching
        self.success_buffer = []
        self.failure_buffer = []
        self.buffer_lock = asyncio.Lock()
        
    
    @staticmethod
    def _log_phase(phase_name: str, start: float, **extra):
        """Log latency for any phase in ms with structured fields."""
        latency_ms = round((time.time() - start) * 1000, 2)
        log_data = {
            "phase": phase_name,
            "latency_ms": latency_ms,
            **extra
        }
        logger.info("phase_latency", **log_data)
        return latency_ms

        
        

    async def start(self):
        self.running = True
        active_workers_gauge.set(1)  
        logger.info("worker_started", message="Delivery worker started")

    
        flush_task = asyncio.create_task(self._timer_flush())
        workers = [asyncio.create_task(self._worker_loop()) for _ in range(self.CONCURRENCY)]

        try:
            # Wait for all workers (they run until self.running = False)
            await asyncio.gather(*workers)
        finally:
            # Ensure cleanup on exit
            await self.stop()
            flush_task.cancel()
            try:
                await flush_task
            except asyncio.CancelledError:
                pass  # Expected after cancellation

    async def _worker_loop(self):
        """Persistent worker that continuously processes queue items."""
        worker_id = id(asyncio.current_task())
        while self.running:
            try:
                await self.process_one(worker_id)
            except Exception as e:
                logger.error("worker_critical_error", error=str(e), exc_info=True)
                await asyncio.sleep(2)  # Match File 1's error backoff

    async def _timer_flush(self):
        """Background loop to ensure data is saved every few seconds."""
        while self.running:
            await asyncio.sleep(self.FLUSH_INTERVAL)
            await self.flush_to_db()

   

    async def stop(self):
        self.running = False
        await self.flush_to_db()  # Final flush before shutting down
        await WebhookDeliveryService.close()
        active_workers_gauge.set(0)
        logger.info("worker_stopped", message="Delivery worker stopped")

    async def get_subscription(self, event_type, db):
        cache_key = f"sub_cache:{event_type}"
        cache_hit = False
        
        redis_start = time.time()
        try:
            cached_sub = await redis_client.get(cache_key)
            redis_latency = self._log_phase(
                "redis_get", redis_start,
                cache_key=cache_key, hit=(cached_sub is not None)
            )
            if cached_sub:
                return json.loads(cached_sub)
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
                await redis_client.setex(
                    cache_key, self.SUBSCRIPTION_CACHE_TTL, json.dumps(sub_data)
                )
                self._log_phase("redis_setex", redis_write_start, cache_key=cache_key)
            except Exception as e:
                logger.error("cache_write_error", error=str(e))
            return sub_data
        return None

    async def process_one(self, worker_id: int):
           # ── Phase 0: Queue Dequeue ──
        dequeue_start = time.time()
        event_id = await QueueService.dequeue()
        dequeue_latency = self._log_phase("queue_dequeue", dequeue_start, worker_id=worker_id)

        if not event_id:
            await asyncio.sleep(self.POLL_INTERVAL)
            return

        # Normalize event_id to UUID once at the start
        event_uuid = UUID(event_id) if isinstance(event_id, str) else event_id
        overall_start = time.time() 
        
        logger.debug("processing_event", event_id=event_id, worker_id=worker_id, dequeue_latency_ms=dequeue_latency)
        
        search_window = datetime.now(timezone.utc) - timedelta(days=1)

        async with AsyncSessionLocal() as db:
            # Fetch Event (partition-aware)
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
                async with self.buffer_lock:
                    self.failure_buffer.append(event_id)
                return

            # Count attempts (partition-aware, type-safe)
            attempt_count_start = time.time()
            try:
                attempts_query = (
                    select(func.count())
                    .select_from(DeliveryAttempt)
                    .where(
                        DeliveryAttempt.event_id == event_uuid,  # UUID type match
                        DeliveryAttempt.created_at >= search_window,
                    )
                )
                count_result = await db.execute(attempts_query)
                res = count_result.scalar()
                # Defensive: guarantee integer, handle None/0/unexpected types
                attempt_number = int((res or 0) + 1)
            except Exception as e:
                logger.warning("attempt_count_error", event_id=event_id, error=str(e))
                attempt_number = 1
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

            # Ensure http_status is integer (delivery service may return string)
            http_status = int(status) if status is not None else 0

            # ── Phase 5: Record Attempt in DB ──
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


            # Latency metric
            total_latency = time.time() - overall_start
            delivery_latency_histogram.labels(event_type=event.event_type).observe(total_latency)

            # --- Outcome Handling ---
            if 200 <= http_status < 300:
                # SUCCESS
                webhook_delivered_total.labels(event_type=event.event_type).inc()
                async with self.buffer_lock:
                    self.success_buffer.append(event_id)
                
                logger.info(
                    "webhook_delivered",
                    event_id=event_id,
                    attempt=attempt_number,
                    status=http_status,
                    # Breakdown of latencies for this successful delivery:
                    latency_total_ms=round(total_latency * 1000, 2),
                    latency_dequeue_ms=round(dequeue_latency, 2),
                    latency_db_fetch_ms=round(db_fetch_latency, 2),
                    latency_subscription_ms=round(sub_lookup_latency, 2),
                    latency_attempt_count_ms=round(attempt_count_latency, 2),
                    latency_http_delivery_ms=round(delivery_latency, 2),
                    latency_db_write_ms=round(db_write_latency, 2),
                )

            elif RetryStrategy.should_retry(attempt_number, http_status):
                # RETRY
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
                    latency_total_ms=round(total_latency * 1000, 2),
                    latency_retry_enqueue_ms=round(retry_enqueue_latency, 2),
                )
            else:
                # FAILED (dead letter)
                webhook_failed_total.labels(
                    event_type=event.event_type,
                    reason="max_retries" if attempt_number >= RetryStrategy.MAX_RETRIES else "status_failure",
                ).inc()
                async with self.buffer_lock:
                    self.failure_buffer.append(event_id)
                
                logger.error(
                    "webhook_dead_lettered",
                    event_id=event_id,
                    attempt=attempt_number,
                    status=http_status,
                    error=error,
                    latency_total_ms=round(total_latency * 1000, 2),
                )

            # Trigger flush if buffer hits threshold
            if len(self.success_buffer) + len(self.failure_buffer) >= self.BATCH_SIZE:
                flush_start = time.time()
                asyncio.create_task(self.flush_to_db())
                self._log_phase("flush_triggered_async", flush_start, buffer_size=len(self.success_buffer) + len(self.failure_buffer))

    async def flush_to_db(self):
        """Bulk update PostgreSQL to save Disk I/O."""
        async with self.buffer_lock:
            to_success = list(self.success_buffer)
            to_fail = list(self.failure_buffer)
            self.success_buffer.clear()
            self.failure_buffer.clear()

        if not to_success and not to_fail:
            return

        flush_start = time.time()
        try:
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