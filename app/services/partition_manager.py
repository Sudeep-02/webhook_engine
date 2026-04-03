from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# NEW: Use your project's actual logger to support the message= and error= syntax
from app.core.logging_config import logger


class PartitionManager:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def sync_partitions(self):
        """Orchestrates creation of future partitions and cleanup of old ones."""
        try:
            await self.create_future_partitions()
            await self.cleanup_old_partitions()
            await self.session.commit()
            logger.info(
                "partition_sync_successful", message="Daily maintenance complete."
            )
        except Exception as e:
            await self.session.rollback()
            logger.error("partition_sync_failed", error=str(e))
            raise e

    async def create_future_partitions(self):
        now = datetime.now(timezone.utc)
        today_midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        
        for days_ahead in [0, 1, 2]:
            target_start = today_midnight + timedelta(days=days_ahead)
            target_end = target_start + timedelta(days=1)
            
            suffix = target_start.strftime("%Y_%m_%d")
            # Format: '2026-04-03 00:00:00+00'
            start_range = target_start.strftime("%Y-%m-%d %H:%M:%S%z")
            end_range = target_end.strftime("%Y-%m-%d %H:%M:%S%z")

            for parent_table in ["webhook_events", "delivery_attempts"]:
                partition_name = f"{parent_table}_{suffix}"

                # 1. Create the partition
                await self.session.execute(
                    text(f"""
                    CREATE TABLE IF NOT EXISTS {partition_name} 
                    PARTITION OF {parent_table}
                    FOR VALUES FROM ('{start_range}') TO ('{end_range}');
                """)
                )

                # 2. CRITICAL: Tell Postgres to update its internal maps for the new table
                await self.session.execute(text(f"ANALYZE {partition_name};"))

                logger.debug("partition_verified", table=partition_name)

        await self.session.commit()

    async def cleanup_old_partitions(self):
        """
        The Garbage Collector.
        Drops partitions older than 7 days.
        """
        retention_days = 7
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        suffix = cutoff_date.strftime("%Y_%m_%d")

        for parent_table in ["webhook_events", "delivery_attempts"]:
            partition_name = f"{parent_table}_{suffix}"

            # 3. CRITICAL: Use CASCADE to ensure indices and constraints are dropped cleanly
            query = text(f"DROP TABLE IF EXISTS {partition_name} CASCADE;")

            await self.session.execute(query)
            logger.info("garbage_collection_complete", partition=partition_name)

        await self.session.commit()
