import redis.asyncio as redis
from app.config import Settings
from app.core.logging_config import logger


# decode_responses=True ensures you get Python strings instead of bytes.
redis_client = redis.from_url(
    Settings.REDIS_URL,
    decode_responses=True,           # Already set — critical for string handling
    max_connections=100,             # Tuned for 150+ concurrent users
    socket_keepalive=True,           # revents stale connections
    retry_on_timeout=True,           # Auto-retry on transient network issues
    health_check_interval=30,        # Proactively detect dead connections
    socket_connect_timeout=5.0,      # ➕ NEW: Fail fast on connection issues
    socket_timeout=10.0,             # ➕ NEW: Prevent hanging on slow Redis
)


async def get_redis_status() -> bool:
    """Health check for Redis connectivity."""
    try:
        await redis_client.ping() # type: ignore
        return True
    except redis.ConnectionError as e:
        logger.error("redis_connection_error", error=str(e))
        return False
    except Exception as e:
        logger.error("redis_unexpected_error", error=str(e), exc_info=True)
        return False