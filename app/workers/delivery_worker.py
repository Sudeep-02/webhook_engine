import asyncio
import time
import json

from uuid import UUID
from app.database import AsyncSessionLocal
from app.models import WebhookEvent, DeliveryAttempt, Subscription
from app.services.queue_service import QueueService
from app.services.retry_strategy import RetryStrategy
from app.services.webhook_delivery import WebhookDeliveryService
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func
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
    CONCURRENCY = 100  # Static worker pool size (replaces semaphore)

    def __init__(self):
        self.running = False
        # Note: Semaphore removed - worker pool size (100) naturally limits concurrency
        # Buffers for Batching
        self.success_buffer = []
        self.failure_buffer = []
        self.buffer_lock = asyncio.Lock()

    async def start(self):
        self.running = True
        active_workers_gauge.set(1)  # Track worker status
        logger.info("worker_started", message="Delivery worker started")

        # Start the background timer to flush small batches
        flush_task = asyncio.create_task(self._timer_flush())

        # Static Worker Pool: Create fixed number of persistent workers
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
        while self.running:
            try:
                await self.process_one()
            except Exception as e:
                logger.error("worker_critical_error", error=str(e), exc_info=True)
                await asyncio.sleep(2)  # Match File 1's error backoff

    async def _timer_flush(self):
        """Background loop to ensure data is saved every few seconds."""
        while self.running:
            await asyncio.sleep(self.FLUSH_INTERVAL)
            await self.flush_to_db()

    async def flush_to_db(self):
        """The 'Senior' move: Bulk update PostgreSQL to save Disk I/O."""
        async with self.buffer_lock:
            to_success = list(self.success_buffer)
            to_fail = list(self.failure_buffer)
            self.success_buffer.clear()
            self.failure_buffer.clear()

        if not to_success and not to_fail:
            return

        try:
            async with AsyncSessionLocal() as db:
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
                await db.commit()
                logger.info(
                    "bulk_update_complete", success=len(to_success), failed=len(to_fail)
                )
        except Exception as e:
            logger.error("flush_error", error=str(e))

    async def stop(self):
        self.running = False
        await self.flush_to_db()  # Final flush before shutting down
        await WebhookDeliveryService.close()
        active_workers_gauge.set(0)
        logger.info("worker_stopped", message="Delivery worker stopped")

    async def get_subscription(self, event_type, db):
        cache_key = f"sub_cache:{event_type}"

        try:
            # redis_client has decode_responses=True, so this returns a string
            cached_sub = await redis_client.get(cache_key)
            if cached_sub:
                return json.loads(cached_sub)
        except Exception as e:
            logger.error("cache_read_error", error=str(e))

        # Cache Miss: Query Postgres
        stmt = select(Subscription).where(Subscription.event_type == event_type)
        result = await db.execute(stmt)
        subscription = result.scalar_one_or_none()

        if subscription:
            sub_data = {
                "target_url": subscription.target_url,
                "secret": subscription.secret,
                "event_type": subscription.event_type,
            }
            try:
                await redis_client.setex(
                    cache_key, self.SUBSCRIPTION_CACHE_TTL, json.dumps(sub_data)
                )
            except Exception as e:
                logger.error("cache_write_error", error=str(e))
            return sub_data
        return None

    async def process_one(self):
        event_id = await QueueService.dequeue()
        if not event_id:
            await asyncio.sleep(self.POLL_INTERVAL)
            return

        # Normalize event_id to UUID once at the start
        event_uuid = UUID(event_id) if isinstance(event_id, str) else event_id

        start_time = time.time()
        search_window = datetime.now(timezone.utc) - timedelta(days=7)

        async with AsyncSessionLocal() as db:
            # Fetch Event (partition-aware)
            stmt = select(WebhookEvent).where(
                WebhookEvent.id == event_uuid,
                WebhookEvent.created_at >= search_window,
            )
            result = await db.execute(stmt)
            event = result.scalar_one_or_none()

            if not event or event.is_delivered:
                return

            subscription = await self.get_subscription(event.event_type, db)

            if not subscription:
                logger.warning(
                    "subscription_not_found",
                    event_type=event.event_type,
                    event_id=event_id,
                )
                webhook_failed_total.labels(
                    event_type=event.event_type, reason="subscription_not_found"
                ).inc()
                async with self.buffer_lock:
                    self.failure_buffer.append(event_id)  # Keep as string for buffer consistency
                return

            # Count attempts (partition-aware, type-safe)
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
                attempt_number = 1  # Safe fallback

            # Deliver webhook
            (
                status,
                resp_body,
                error,
                resp_headers,
            ) = await WebhookDeliveryService.deliver(
                url=subscription["target_url"],
                payload=event.payload,
                secret=subscription["secret"],
                event_type=event.event_type,
            )

            # Ensure http_status is integer (delivery service may return string)
            http_status = int(status) if status is not None else 0

            # Record attempt (type-safe)
            db.add(
                DeliveryAttempt(
                    event_id=event_uuid,  # Use UUID, not string
                    created_at=datetime.now(timezone.utc),
                    attempt_number=attempt_number,  # Guaranteed int
                    http_status=http_status,  # Guaranteed int
                    response_body=resp_body if resp_body else None,
                    error_message=error,
                )
            )
            await db.commit()

            # Latency metric
            latency = time.time() - start_time
            delivery_latency_histogram.labels(event_type=event.event_type).observe(latency)

            # --- Outcome Handling ---
            if 200 <= http_status < 300:
                webhook_delivered_total.labels(event_type=event.event_type).inc()
                async with self.buffer_lock:
                    self.success_buffer.append(event_id)  # Keep string for buffer
                logger.info(
                    "webhook_delivered",
                    event_id=event_id,
                    attempt=attempt_number,
                    status=http_status,
                    latency_ms=round(latency * 1000, 2),
                )

            elif RetryStrategy.should_retry(attempt_number, http_status):
                delay = RetryStrategy.calculate_delay(attempt_number, resp_headers)
                await QueueService.enqueue(event_uuid, delay)  # UUID for queue
                webhook_retry_total.labels(
                    event_type=event.event_type, attempt_number=attempt_number
                ).inc()
                logger.warning(
                    "webhook_retry_queued",
                    event_id=event_id,
                    attempt=attempt_number,
                    delay=delay,
                    status=http_status,
                )
            else:
                webhook_failed_total.labels(
                    event_type=event.event_type,
                    reason="max_retries"
                    if attempt_number >= RetryStrategy.MAX_RETRIES
                    else "status_failure",
                ).inc()
                async with self.buffer_lock:
                    self.failure_buffer.append(event_id)
                logger.error(
                    "webhook_dead_lettered",
                    event_id=event_id,
                    attempt=attempt_number,
                    status=http_status,
                    error=error,
                )

            # Trigger flush if buffer hits threshold
            if len(self.success_buffer) + len(self.failure_buffer) >= self.BATCH_SIZE:
                asyncio.create_task(self.flush_to_db())