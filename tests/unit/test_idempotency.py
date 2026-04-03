import pytest
import uuid
from app.services.idempotency import IdempotencyService

class TestIdempotencyService:
    
    @pytest.mark.asyncio
    async def test_check_and_set_success(self, mocker):
        """Should return True when Redis SET (nx=True) succeeds."""
        # 1. Mock the Redis client
        mock_redis = mocker.patch("app.services.idempotency.redis_client")
        # Simulate Redis returning True (Key was set)
        mock_redis.set = mocker.AsyncMock(return_value=True)
        
        event_id = uuid.uuid4()
        id_key = "unique-request-123"
        
        # 2. Execute
        result = await IdempotencyService.check_and_set(id_key, event_id)
        
        # 3. Assertions
        assert result is True
        # Verify it called Redis with the correct prefix, value, and TTL
        mock_redis.set.assert_called_once_with(
            f"idempotency:{id_key}",
            str(event_id),
            nx=True,
            ex=86400
        )

    @pytest.mark.asyncio
    async def test_check_and_set_duplicate(self, mocker):
        """Should return False when Redis SET (nx=True) fails (key exists)."""
        mock_redis = mocker.patch("app.services.idempotency.redis_client")
        # Simulate Redis returning None/False because key already exists
        mock_redis.set = mocker.AsyncMock(return_value=None)
        
        result = await IdempotencyService.check_and_set("dup-key", uuid.uuid4())
        
        assert result is False

    @pytest.mark.asyncio
    async def test_get_cached_response_found(self, mocker):
        """Should return the integer ID if the key exists in Redis."""
        mock_redis = mocker.patch("app.services.idempotency.redis_client")
        mock_redis.get = mocker.AsyncMock(return_value="999")
        
        result = await IdempotencyService.get_cached_response("some-key")
        
        assert result == 999
        mock_redis.get.assert_called_once_with("idempotency:some-key")

    @pytest.mark.asyncio
    async def test_get_cached_response_missing(self, mocker):
        """Should return None if the key is not in Redis."""
        mock_redis = mocker.patch("app.services.idempotency.redis_client")
        mock_redis.get = mocker.AsyncMock(return_value=None)
        
        result = await IdempotencyService.get_cached_response("missing-key")
        
        assert result is None