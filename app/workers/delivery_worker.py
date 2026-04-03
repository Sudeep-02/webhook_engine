import asyncio
import time

# import uuid
from app.database import AsyncSessionLocal
from app.models import WebhookEvent, DeliveryAttempt, Subscription
from app.services.queue_service import QueueService
from app.services.retry_strategy import RetryStrategy
from app.services.webhook_delivery import WebhookDeliveryService
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func

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
    POLL_INTERVAL = 0.01

    def __init__(self):
        self.running = False
        self.semaphore = asyncio.Semaphore(85)

    async def start(self):
        self.running = True
        active_workers_gauge.set(1)  # Track worker status
        logger.info("worker_started", message="Delivery worker started")

        while self.running:
            try:
                asyncio.create_task(self.process_one())
            except Exception as e:
                logger.error("worker_critical_error", error=str(e), exc_info=True)
                await asyncio.sleep(2)
            await asyncio.sleep(self.POLL_INTERVAL)

    async def stop(self):
        """Added graceful stop for lifespan support"""
        self.running = False
        active_workers_gauge.set(0)
        logger.info("worker_stopped", message="Delivery worker stopped")

    async def process_one(self):
        event_id = await QueueService.dequeue()
        if not event_id:
            return

        async with self.semaphore:
            start_time = time.time()

            # Define a search window for Partition Pruning.
            # Since we only keep 7 days of data, we tell Postgres to ONLY look in the last 7 days.
            search_window = datetime.now(timezone.utc) - timedelta(days=7)

            async with AsyncSessionLocal() as db:
                # This is CRITICAL. Without 'created_at >=', Postgres scans all 30M+ rows.
                stmt = select(WebhookEvent).where(
                    WebhookEvent.id == event_id,
                    WebhookEvent.created_at >= search_window,
                )

                result = await db.execute(stmt)
                event = result.scalar_one_or_none()

                if not event or event.is_delivered:
                    return

                sub_result = await db.execute(
                    select(Subscription).where(
                        Subscription.event_type == event.event_type
                    )
                )
                sub = sub_result.scalar_one_or_none()

                if not sub:
                    logger.warning(
                        "subscription_not_found",
                        event_type=event.event_type,
                        event_id=event_id,
                    )
                    return

                # Efficiently count attempts using the partition key
                # We filter by event_id AND the search window to hit the right partition.
                attempts_query = (
                    select(func.count())
                    .select_from(DeliveryAttempt)
                    .where(
                        DeliveryAttempt.event_id == event_id,
                        DeliveryAttempt.created_at >= search_window,
                    )
                )
                count_result = await db.execute(attempts_query)
                res = count_result.scalar()
                attempt_number = (res if res is not None else 0) + 1

                # Deliver
                (
                    status,
                    resp_body,
                    error,
                    resp_headers,
                ) = await WebhookDeliveryService.deliver(
                    url=sub.target_url,
                    payload=event.payload,  # Kept your JSONB direct access
                    secret=sub.secret,
                    event_type=event.event_type,
                )

                # Because DeliveryAttempt is partitioned, it MUST have a created_at.
                # Record the attempt
                db.add(
                    DeliveryAttempt(
                        event_id=event_id,
                        created_at=datetime.now(timezone.utc),
                        attempt_number=attempt_number,
                        http_status=status,
                        response_body=resp_body
                        if resp_body
                        else None,  # Truncate large responses
                        error_message=error,
                    )
                )

                # Calculate and record latency metric
                latency = time.time() - start_time
                delivery_latency_histogram.labels(event_type=event.event_type).observe(
                    latency
                )

                # --- Outcome Handling ---
                if 200 <= status < 300:
                    event.is_delivered = True
                    webhook_delivered_total.labels(event_type=event.event_type).inc()
                    logger.info(
                        "webhook_delivered",
                        event_id=event_id,
                        attempt=attempt_number,
                        status=status,
                        latency_ms=round(latency * 1000, 2),
                    )

                elif RetryStrategy.should_retry(attempt_number, status):
                    delay = RetryStrategy.calculate_delay(attempt_number, resp_headers)
                    await QueueService.enqueue(event_id, delay)

                    webhook_retry_total.labels(
                        event_type=event.event_type, attempt_number=attempt_number
                    ).inc()

                    logger.warning(
                        "webhook_retry_queued",
                        event_id=event_id,
                        attempt=attempt_number,
                        delay=delay,
                        status=status,
                    )
                else:
                    # Permanent failure or max retries reached
                    # We mark it as is_failed = True so the RecoveryService ignores it forever.
                    event.is_failed = True

                    webhook_failed_total.labels(
                        event_type=event.event_type,
                        reason="max_retries"
                        if attempt_number >= RetryStrategy.MAX_RETRIES
                        else "status_failure",
                    ).inc()

                    logger.error(
                        "webhook_dead_lettered",
                        event_id=event_id,
                        attempt=attempt_number,
                        status=status,
                        error=error,
                    )

                await db.commit()
