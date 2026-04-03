import time
from typing import Optional
from app.redis_client import redis_client
import uuid


class QueueService:
    """
    Manages webhook delivery queue using Redis Sorted Sets.
    Scores represent the Unix timestamp of when the event should be processed.
    """

    QUEUE_KEY = "queue:delivery"

    @classmethod
    async def enqueue(cls, event_id: uuid.UUID, delay_seconds: float = 0) -> None:
        """
        Add event to delivery queue.
        delay_seconds can be a float (from our RetryStrategy jitter).
        """
        score = time.time() + delay_seconds
        # Using zadd to store event_id as the member and timestamp as the score
        await redis_client.zadd(cls.QUEUE_KEY, {str(event_id): score})

    @classmethod
    async def dequeue(cls) -> Optional[str]:
        """
        Atomically fetch the next event that is ready for processing.
        Only returns an event if its score <= current_time.
        """
        now = time.time()

        # 1. Peek at the first item to see if it's ready
        result = await redis_client.zrange(cls.QUEUE_KEY, 0, 0, withscores=True)

        if not result:
            return None

        event_id_str, score = result[0]

        # 2. Check if the scheduled time has arrived
        if score > now:
            return None

        # 3. Atomically remove it to ensure no other worker grabs it
        # zrem returns 1 if the item was actually removed
        removed = await redis_client.zrem(cls.QUEUE_KEY, event_id_str)

        if removed:
            return event_id_str

        return None

    @classmethod
    async def get_queue_depth(cls) -> int:
        """Monitor total pending tasks in the queue."""
        return await redis_client.zcard(cls.QUEUE_KEY)
