from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.config import settings
from app.models import Base

# 1. Create the Async Engine
# Pylance is happy because settings.DATABASE_URL is a guaranteed str
engine = create_async_engine(
    settings.DATABASE_URL,
    # --- PRO SETTINGS FOR 1M REQS/DAY ---
    
    # 1. Base pool size: How many permanent connections to keep open.
    # At 100 req/s, 20-30 connections can handle thousands of concurrent queries 
    # because asyncpg releases them back to the pool instantly after 'await'.
    pool_size=90, 

    # 2. Max Overflow: How many extra connections to open during a massive spike.
    # If a burst of 500 webhooks hits, this gives you a buffer.
    max_overflow=10,

    # 3. Pool Timeout: How long a request waits for a connection before failing.
    # We set this to 30s. If it takes longer, your DB is likely down or locking.
    pool_timeout=30,

    # 4. Pool Recycle: Prevents "Idle Connection Closed" errors from firewalls/Postgres.
    # We recycle connections every hour.
    pool_recycle=3600,

    # 5. Pre-Ping: Checks if the connection is "alive" before giving it to the app.
    # Crucial for recovering from DB restarts without crashing the API.
    pool_pre_ping=True
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
