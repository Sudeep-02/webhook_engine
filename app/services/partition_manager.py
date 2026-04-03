from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# NEW: Use your project's actual logger to support the message= and error= syntax
from app.core.logging_config import logger


class PartitionManager:
    """
    Handles the lifecycle of PostgreSQL partitions.
    At 1M requests/day, we use Daily partitions to keep index sizes small
    enough to fit in RAM/CPU Cache.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def sync_partitions(self):
        """Orchestrates creation of future partitions and cleanup of old ones."""
        try:
            await self.create_future_partitions()
            await self.cleanup_old_partitions()
            logger.info(
                "partition_sync_successful", message="Daily maintenance complete."
            )
        except Exception as e:
            logger.error("partition_sync_failed", error=str(e))
            await self.session.rollback()

    async def create_future_partitions(self):
        """
        Creates partitions for Today, Tomorrow, and Day After Tomorrow.
        This buffer prevents 'No Partition' errors during server reboots.
        """
        for days_ahead in [0, 1, 2]:
            target_date = datetime.now(timezone.utc) + timedelta(days=days_ahead)

            suffix = target_date.strftime("%Y_%m_%d")
            start_range = target_date.strftime("%Y-%m-%d")
            end_range = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

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
