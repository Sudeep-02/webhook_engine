from app.redis_client import redis_client
from typing import Optional
import uuid


class IdempotencyService:
    """
    Handles idempotency checks using Redis to prevent duplicate processing.
    """

    # 24 hours is standard; long enough for retries, short enough to save RAM
    TTL_SECONDS = 86400
    PREFIX = "idempotency:"

    @classmethod
    async def check_and_set(cls, idempotency_key: str, event_id: uuid.UUID) -> bool:
        """
        Atomically check if key exists and set it if not.
        Returns True if this is the FIRST time seeing this key (Green light to process).
        Returns False if key already exists (Duplicate - stop here).
        """
        redis_key = f"{cls.PREFIX}{idempotency_key}"

        # We store the Postgres event_id as the value so we know
        # exactly which database record this key belongs to.
        result = await redis_client.set(
            redis_key,
            str(event_id),
            nx=True,  # "Set if Not Exists" - This is your Atomic Lock
            ex=cls.TTL_SECONDS,
        )

        # Redis return values vary by client library:
        # Some return True/False, others return 1/None.
        # bool() handles both safely.
        return bool(result)

    @classmethod
    async def get_cached_response(cls, idempotency_key: str) -> Optional[int]:
        """
        If a duplicate is hit, we return the original event metadata.
        This allows the API to return a 200 OK with the original ID
        instead of a confusing error.
        """
        redis_key = f"{cls.PREFIX}{idempotency_key}"
        val = await redis_client.get(redis_key)

        # Optional[Dict[str, Any]]: in parameter
        # if event_id:
        #     return {
        #         "id": int(event_id),
        #         "status": "idempotent_match",
        #         "message": "Event already processed"
        #     }
        # return None

        # Redis returns bytes or None;
        return val if val is not None else None
