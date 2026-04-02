from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.config import settings
from app.models import Base

# 1. Create the Async Engine
# Pylance is happy because settings.DATABASE_URL is a guaranteed str
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,  # Log SQL queries (set to False in production)
    pool_pre_ping=True,  # Check connection health before using
    pool_size=10,  # Standard pool size
    max_overflow=20,  # Extra connections allowed during spikes
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
