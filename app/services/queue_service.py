import time
import uuid
from typing import Optional, Any, Awaitable, cast
from app.redis_client import redis_client
from app.core.logging_config import logger

class QueueService:
    """
    Manages webhook delivery queue using Redis Sorted Sets.
    Scores represent the Unix timestamp of when the event should be processed.
    """

    QUEUE_KEY = "queue:delivery"
    QUEUE_DEPTH_WARNING = 1000
    QUEUE_DEPTH_CRITICAL = 5000

    @classmethod
    async def enqueue(cls, event_id: uuid.UUID, delay_seconds: float = 0) -> None:
        """
        Add event to delivery queue.
        delay_seconds can be a float (from RetryStrategy jitter).
        """
        score = time.time() + delay_seconds
        # Using zadd to store event_id as the member and timestamp as the score
        await redis_client.zadd(cls.QUEUE_KEY, {str(event_id): score})

    @classmethod
    async def dequeue(cls) -> Optional[str]:
        """
        Atomically fetch and remove the next ready event using a Lua script.
        This prevents race conditions between multiple workers.
        """
        now = time.time()
        dequeue_start = time.time()
        
        # Lua Script: 
        # 1. Get the first element and its score.
        # 2. If it exists and score <= now, REMOVE it and RETURN it.
        # 3. Otherwise, return nil.
        lua_script = """
        local val = redis.call('zrange', KEYS[1], 0, 0, 'withscores')
        if val[1] ~= nil and tonumber(val[2]) <= tonumber(ARGV[1]) then
            redis.call('zrem', KEYS[1], val[1])
            return val[1]
        end
        return nil
        """
        
        try:
            result = await cast(Awaitable[Any], redis_client.eval(lua_script, 1, cls.QUEUE_KEY, now))
            dequeue_latency = (time.time() - dequeue_start) * 1000
            
            
            if dequeue_latency > 50:  # Log only if slow
                logger.warning(
                    "queue_dequeue_slow",
                    latency_ms=round(dequeue_latency, 2),
                    hint="Check Redis latency or network"
                )
            return result
        except Exception as e:
            logger.error("queue_dequeue_error", error=str(e))
            return None
        
        
    @classmethod
    async def get_queue_depth(cls) -> int:
        """
        Monitor total pending tasks for backpressure decisions.
        Returns count of events with score <= now (ready to process).
        """
        now = time.time()
        # Count only ready events (score <= now)
        depth = await redis_client.zcount(cls.QUEUE_KEY, "-inf", now)
        return int(depth) if depth else 0


    @classmethod
    async def get_queue_stats(cls) -> dict:
        """
        Full queue diagnostics for monitoring/alerting.
        """
        now = time.time()
        total = await redis_client.zcard(cls.QUEUE_KEY)
        ready = await redis_client.zcount(cls.QUEUE_KEY, "-inf", now)
        future = total - ready
        
        return {
            "total_pending": int(total) if total else 0,
            "ready_now": int(ready) if ready else 0,
            "scheduled_future": int(future) if future else 0,
            "backpressure_warning": ready > cls.QUEUE_DEPTH_WARNING,
            "backpressure_critical": ready > cls.QUEUE_DEPTH_CRITICAL,
        }