from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.config import settings
from app.models import Base
import os
from app.core.logging_config import logger

# Read from Docker environment with sensible fallbacks
POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "50"))
MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "30"))
POOL_TIMEOUT = float(os.getenv("DB_POOL_TIMEOUT", "10.0"))  # ← Increased from 5s to reduce timeout errors
POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))

logger.info(
    "db_pool_configured",
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_timeout=POOL_TIMEOUT,
    database_url_host=(
        settings.DATABASE_URL.split("@")[1].split(":")[0]
        if "@" in settings.DATABASE_URL else "unknown"
    ),
    note="Set DB_POOL_SIZE/DB_MAX_OVERFLOW in docker-compose.yml for production"
)

engine = create_async_engine(
    settings.DATABASE_URL,
    pool_size=POOL_SIZE,              # DB_POOL_SIZE env var
    max_overflow=MAX_OVERFLOW,        # DB_MAX_OVERFLOW env var
    pool_timeout=POOL_TIMEOUT,        # Increased timeout to avoid premature failures
    pool_recycle=POOL_RECYCLE,        # Prevent stale connections
    pool_pre_ping=True,               # Detect dropped connections before use
    pool_use_lifo=True,               # Use last-in-first-out for better cache locality
    echo=False,
)

#Create a Session Factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine, autocommit=False, autoflush=False, expire_on_commit=False
)


# 3. FastAPI Dependency
async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()


# 4. Database Initialization (Optional/Development)
async def init_db():
    async with engine.begin() as conn:
        # run_sync is required because Base.metadata.create_all is a sync function
        await conn.run_sync(Base.metadata.create_all)
