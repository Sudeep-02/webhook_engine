import redis.asyncio as redis
from app.config import settings

# decode_responses=True ensures you get Python strings instead of bytes.
redis_client = redis.from_url(
    settings.REDIS_URL,
    decode_responses=True,
    protocol=3,
    # health_check_interval to prevent silent connection drops
    health_check_interval=30,
)


async def get_redis_status() -> bool:  # Added explicit return type hint
    try:
        # ping() is awaitable and returns True/False or raises
        await redis_client.ping()  # type: ignore
        return True
    except Exception as e:
        print(f"Redis Error: {e}")
        return False
