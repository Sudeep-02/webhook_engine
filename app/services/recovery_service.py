from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models import WebhookEvent
from app.services.queue_service import QueueService
from app.core.logging_config import logger


class RecoveryService:
    @staticmethod
    async def sweep_stuck_events():
        """
        Finds events stuck in Postgres (unsent) and puts them back in Redis.
        """
        async with AsyncSessionLocal() as db:
            # Look for events that should have been sent by now
            threshold = datetime.now(timezone.utc) - timedelta(minutes=2)

            # NEW: Search Window for Partition Pruning.
            # We only look back 24 hours. This prevents Postgres from scanning
            # old/deleted partitions, keeping the query sub-millisecond.
            search_window = datetime.now(timezone.utc) - timedelta(hours=24)

            # This query hits your 'idx_webhook_events_pending' Partial Index
            stmt = (
                select(WebhookEvent.id)
                .where(WebhookEvent.is_delivered == False)
                .where(WebhookEvent.is_failed == False)
                .where(WebhookEvent.created_at < threshold)
                # MODIFIED: Added created_at filter to trigger Partition Pruning
                .where(WebhookEvent.created_at >= search_window)
                .limit(100)  # Process in small batches to stay fast
            )

            result = await db.execute(stmt)
            stuck_ids = result.scalars().all()

            if not stuck_ids:
                return 0

            for event_id in stuck_ids:
                # Re-inject the ID into the speed layer (Redis)
                try:
                    await QueueService.enqueue(event_id)
                    logger.info("event_recovered_to_queue", event_id=event_id)
                except Exception as e: 
                    logger.error("recovery_enqueue_failed", event_id=event_id, error=str(e))

            return len(stuck_ids)
