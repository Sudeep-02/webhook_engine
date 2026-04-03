import pytest
from fakeredis import FakeAsyncRedis
import app.redis_client  # Import the module to access the client

class TestRedisClient:

    @pytest.mark.asyncio
    async def test_get_redis_status_success(self, mocker):
        """Tests the 'try' block: Should return True when Redis is alive."""
        # 1. Create a fake redis server
        fake_redis = FakeAsyncRedis(decode_responses=True)
        
        # 2. Patch the client in the actual module
        mocker.patch("app.redis_client.redis_client", fake_redis)

        # 3. Execute
        from app.redis_client import get_redis_status
        status = await get_redis_status()

        # 4. Assert
        assert status is True

    @pytest.mark.asyncio
    async def test_get_redis_status_failure(self, mocker):
        """Tests the 'except' block: Should return False when Redis crashes."""
        # 1. Create fake redis
        fake_redis = FakeAsyncRedis(decode_responses=True)
        
        # 2. Force the .ping() method to raise an error
        mocker.patch.object(fake_redis, "ping", side_effect=ConnectionError("Redis is down"))
        mocker.patch("app.redis_client.redis_client", fake_redis)

        # 3. Execute
        from app.redis_client import get_redis_status
        status = await get_redis_status()

        # 4. Assert
        assert status is False