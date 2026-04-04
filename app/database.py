from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.config import settings
from app.models import Base
import os


# Read from Docker environment with sensible fallbacks
POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "25"))
MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "5"))


# 1. Create the Async Engine
# Pylance is happy because settings.DATABASE_URL is a guaranteed str
engine = create_async_engine(
    settings.DATABASE_URL,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_timeout=5.0,
    pool_recycle=3600,
    pool_pre_ping=True,
    pool_use_lifo=True,
)

# 2. Create a Session Factory
# In SQLAlchemy 2.0+, 'class_=AsyncSession' is the default for async_sessionmaker
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
