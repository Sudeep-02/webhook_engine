import os
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

# 1. Start Docker Containers for the entire test session
@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres

@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer("redis:7-alpine") as redis:
        yield redis

@pytest_asyncio.fixture(scope="session")
async def integration_client(postgres_container, redis_container):
    """
    This fixture is the gatekeeper. 
    It sets ENV VARS before the App is imported.
    """
    # 2. Get the dynamic connection strings from Testcontainers
    db_url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")
    redis_url = f"redis://{redis_container.get_container_host_ip()}:{redis_container.get_exposed_port(6379)}/0"

    # 3. Inject into environment
    os.environ["DATABASE_URL"] = db_url
    os.environ["REDIS_URL"] = redis_url

    # 4. CRITICAL: Import the app only AFTER environment variables are set
    from main import app
    from app.database import engine
    from app.models import Base
    from app.services.partition_manager import PartitionManager
    from app.database import AsyncSessionLocal

    # 5. Database Setup: Create Tables + Partitions
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        pm = PartitionManager(session)
        await pm.sync_partitions() # Creates Today's partitions

    # 6. Initialize Client with the new 'transport' parameter (fixes Pylance error)
    async with AsyncClient(
        transport=ASGITransport(app=app), 
        base_url="http://test"
    ) as client:
        yield client