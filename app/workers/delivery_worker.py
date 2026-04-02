import asyncio
import time
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models import WebhookEvent, DeliveryAttempt, Subscription
from app.services.queue_service import QueueService
from app.services.retry_strategy import RetryStrategy
from app.services.webhook_delivery import WebhookDeliveryService

# --- New Infrastructure Imports ---
from app.core.logging_config import logger
from app.core.metrics import (
    webhook_delivered_total,
    webhook_failed_total,
    webhook_retry_total,
    delivery_latency_histogram,
    active_workers_gauge
)

class DeliveryWorker:
    POLL_INTERVAL = 1
    
    def __init__(self):
        self.running = False
    
    async def start(self):
        self.running = True
        active_workers_gauge.set(1) # Track worker status
        logger.info("worker_started", message="Delivery worker started")
        
        while self.running:
            try:
                await self.process_one()
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
        
        start_time = time.time()
        
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(WebhookEvent).where(WebhookEvent.id == event_id))
            event = result.scalar_one_or_none()
            
            if not event or event.is_delivered:
                return

            sub_result = await db.execute(select(Subscription).where(Subscription.event_type == event.event_type))
            sub = sub_result.scalar_one_or_none()
            
            if not sub:
                logger.warning("subscription_not_found", event_type=event.event_type, event_id=event_id)
                return
            
            # Count existing attempts
            attempts_result = await db.execute(select(DeliveryAttempt).where(DeliveryAttempt.event_id == event_id))
            attempt_number = len(attempts_result.scalars().all()) + 1
            
            # Deliver
            status, resp_body, error, resp_headers = await WebhookDeliveryService.deliver(
                url=sub.target_url,
                payload=event.payload, # Kept your JSONB direct access
                secret=sub.secret,
                event_type=event.event_type
            )
            

            # Record the attempt
            db.add(DeliveryAttempt(
                event_id=event_id, 
                attempt_number=attempt_number, 
                http_status=status, 
                response_body=resp_body[:500] if resp_body else None, # Truncate large responses
                error_message=error
            ))

            # Calculate and record latency metric
            latency = time.time() - start_time
            delivery_latency_histogram.labels(event_type=event.event_type).observe(latency)

            # --- Outcome Handling with Metrics ---
            if 200 <= status < 300:
                event.is_delivered = True
                webhook_delivered_total.labels(event_type=event.event_type).inc()
                logger.info(
                    "webhook_delivered", 
                    event_id=event_id, 
                    attempt=attempt_number, 
                    status=status,
                    latency_ms=round(latency * 1000, 2)
                )
                
            elif RetryStrategy.should_retry(attempt_number, status):
                delay = RetryStrategy.calculate_delay(attempt_number, resp_headers)
                await QueueService.enqueue(event_id, delay)
                
                webhook_retry_total.labels(
                    event_type=event.event_type, 
                    attempt_number=attempt_number
                ).inc()
                
                logger.warning(
                    "webhook_retry_queued", 
                    event_id=event_id, 
                    attempt=attempt_number, 
                    delay=delay, 
                    status=status
                )
            else:
                # Permanent failure or max retries reached
                webhook_failed_total.labels(
                    event_type=event.event_type,
                    reason="max_retries" if attempt_number >= RetryStrategy.MAX_RETRIES else "status_failure"
                ).inc()
                
                logger.error(
                    "webhook_exhausted", 
                    event_id=event_id, 
                    attempt=attempt_number, 
                    status=status, 
                    error=error
                )
            
            await db.commit()