import time
from typing import Optional
from app.redis_client import redis_client
import uuid
from typing import Optional, Any, Awaitable, cast

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
        Atomically fetch and remove the next ready event using a Lua script.
        This prevents race conditions between multiple workers.
        """
        now = time.time()
        
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
        
        result = await cast(Awaitable[Any], redis_client.eval(lua_script, 1, cls.QUEUE_KEY, now))
        return result # Returns event_id string or None

    @classmethod
    async def get_queue_depth(cls) -> int:
        """Monitor total pending tasks in the queue."""
        return await redis_client.zcard(cls.QUEUE_KEY)
