import asyncio
import time
import uuid
import uuid6
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, status, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from datetime import datetime, timedelta, timezone

# --- Infrastructure & Services ---
from app.database import get_db, engine, AsyncSessionLocal
from app.models import WebhookEvent, DeliveryAttempt, Base
from app.redis_client import redis_client, get_redis_status
from app.services.idempotency import IdempotencyService
from app.services.queue_service import QueueService
from app.services.recovery_service import RecoveryService
from app.workers.delivery_worker import DeliveryWorker

# --- Partitioning Maintenance ---
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.services.partition_manager import PartitionManager

# --- Observability ---
from app.core.logging_config import logger
from app.core.correlation import CorrelationIDMiddleware, get_correlation_id
from app.core.metrics import (
    webhook_received_total,
    queue_depth_gauge,
    start_metrics_server,
    redis_up_gauge,
)


# Initialize the scheduler
scheduler = AsyncIOScheduler()


async def run_partition_maintenance():
    """Daily Maintenance Task: Runs the Garbage Collector and Table Creator."""
    async with engine.begin() as conn:
        # This creates the 'webhook_events' parent table structure
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        manager = PartitionManager(session)
        await manager.sync_partitions()


# Initialize Background Worker
worker = DeliveryWorker()
worker_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_task
    logger.info("application_startup", message="🚀 Starting Webhook Engine...")

    # NEW: 1. Database Partitioning & Maintenance logic
    # This ensures today's table exists immediately on startup
    try:
        await run_partition_maintenance()
        logger.info("partition_init", message="✅ Database partitions verified.")
    except Exception as e:
        logger.error("partition_init_failed", error=str(e))

    # Schedule the job to run every night at 00:05 AM
    scheduler.add_job(run_partition_maintenance, "cron", hour=0, minute=5)
    scheduler.start()

    # 2. Start Metrics Server (Port 8001)
    start_metrics_server(port=8001)

    # 3. Check Redis Connection & Update Gauge
    redis_is_ok = await get_redis_status()
    if redis_is_ok:
        redis_up_gauge.set(1)
        logger.info("redis_connected", message="Redis Connected!")
    else:
        redis_up_gauge.set(0)
        logger.error("redis_error", message="Warning: Redis is not available")

    # 4. Start Background Delivery Worker
    worker_task = asyncio.create_task(worker.start())

    # 2. Start the Recovery Sweeper Loop
    async def recovery_loop():
        while True:
            try:
                # Runs every 60 seconds
                await RecoveryService.sweep_stuck_events()
            except Exception as e:
                logger.error("recovery_loop_error", error=str(e))
            await asyncio.sleep(60)

    recovery_task = asyncio.create_task(recovery_loop())

    yield  # Application runs here

    logger.info("application_shutdown", message=" Shutting down...")

    scheduler.shutdown()  # Stop the partition manager
    recovery_task.cancel()

    redis_up_gauge.set(0)

    if worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

    # 5. Cleanup Connections
    await redis_client.aclose()
    await engine.dispose()
    logger.info("cleanup_complete", message="✨ Cleanup complete.")


app = FastAPI(lifespan=lifespan, title="Reliable Webhook Engine")

# --- Middleware Stack ---
app.add_middleware(CorrelationIDMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Pydantic Models ---
class EventCreate(BaseModel):
    event_type: str
    payload: dict
    idempotency_key: str


# --- Observability Endpoints ---


@app.get("/health")
async def health_check():
    redis_ok = await get_redis_status()
    return {
        "status": "healthy" if redis_ok else "unhealthy",
        "correlation_id": get_correlation_id(),
    }


@app.get("/metrics")
async def metrics():
    """Expose metrics on the main port as well."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/queue/depth")
async def get_queue_stats():
    depth = await QueueService.get_queue_depth()
    queue_depth_gauge.set(depth)  # Update the prometheus gauge
    return {"pending_count": depth}


# --- Core Business Logic ---


@app.post("/events/", status_code=status.HTTP_201_CREATED)
async def create_event(event: EventCreate, db: AsyncSession = Depends(get_db)):
    correlation_id = get_correlation_id()
    start_time = time.time()

    
    
    # Log incoming request with correlation context
    logger.info(
        "webhook_received",
        correlation_id=correlation_id,
        event_type=event.event_type,
        idempotency_key=event.idempotency_key,
    )

    # Increment Prometheus Counter
    webhook_received_total.labels(event_type=event.event_type).inc()

    # 1. Idempotency Check
    existing_id = await IdempotencyService.get_cached_response(event.idempotency_key)
    if existing_id:
        logger.warning(
            "duplicate_request",
            correlation_id=correlation_id,
            key=event.idempotency_key,
        )
        return {"id": existing_id, "idempotent": True, "message": "Duplicate request"}


    now = datetime.now(timezone.utc)
    stable_timestamp = now.replace(second=0, microsecond=0)
    
    
    # 2. Database & Queue Layer
    try:
        async with db.begin():
            db_event = WebhookEvent(
                id=uuid6.uuid7(),
                idempotency_key=event.idempotency_key,
                created_at=stable_timestamp,
                event_type=event.event_type,
                payload=event.payload,
            )
            db.add(db_event)

        await db.refresh(db_event)

        try:
            await QueueService.enqueue(db_event.id)
        except Exception as e:
            # We don't crash the request!
            # The Recovery Sweeper will pick this up in 2 minutes.
            logger.warning(
                "redis_enqueue_failed_background_recovery_will_handle",
                event_id=db_event.id,
                error=str(e),
            )
            return {
                "id": db_event.id,
                "created_at": db_event.created_at.isoformat(),
                "message": "Event queued!",
            }

        # 3. Finalize Cache
        await IdempotencyService.check_and_set(
            idempotency_key=event.idempotency_key, event_id=db_event.id
        )

        latency_ms = round((time.time() - start_time) * 1000, 2)
        logger.info(
            "webhook_queued",
            correlation_id=correlation_id,
            event_id=db_event.id,
            latency_ms=latency_ms,
        )

        return {
            "id": db_event.id,
            "created_at": db_event.created_at,
            "idempotent": False,
            "message": "Event queued for delivery!",
        }

    except Exception as e:
        # Check if this is a "Unique Constraint" (Idempotency Collision)
        if "unique constraint" in str(e).lower() or "duplicate key" in str(e).lower():
            # 1. Log that we caught a duplicate (The "Log" you asked for)
            logger.warning(
                "idempotency_collision_detected",
                idempotency_key=event.idempotency_key,
                correlation_id=correlation_id,
            )

            # This allows Postgres to skip all old partitions.
            search_window = datetime.now(timezone.utc) - timedelta(hours=48)

            # 2. Rescue: Fetch the existing ID from the database
            # Note: We use a fresh query here because the previous transaction failed
            result = await db.execute(
                select(WebhookEvent.id).where(
                    WebhookEvent.idempotency_key == event.idempotency_key,
                    WebhookEvent.created_at >= search_window,  # 👈 Partition Pruning
                )
            )
            existing_id = result.scalar_one_or_none()

            # 3. Return a successful 200/201 response with the original ID
            return {
                "id": existing_id,
                "idempotent": True,
                "message": "Duplicate request, returning existing ID",
            }

        # If it's a real error (like DB connection lost), then we log and crash
        logger.error(
            "event_creation_failed_critical",
            correlation_id=correlation_id,
            error=str(e),
        )
        raise HTTPException(status_code=500, detail="Internal Server Error")


# --- Debugging Endpoints ---


@app.get("/events/")
async def list_events(db: AsyncSession = Depends(get_db)):
    # MODIFIED: When listing events in a partitioned table, ALWAYS filter or limit.
    # We'll show the last 24 hours by default.
    day_ago = datetime.now(timezone.utc) - timedelta(days=1)
    result = await db.execute(
        select(WebhookEvent)
        .where(WebhookEvent.created_at >= day_ago)
        .order_by(WebhookEvent.created_at.desc())
        .limit(100)
    )
    return result.scalars().all()


@app.get("/events/{event_id}/attempts")
async def get_event_attempts(
    event_id: uuid.UUID, db: AsyncSession = Depends(get_db)
):  # MODIFIED: type to uuid.UUID
    # MODIFIED: Search window for attempts.
    # Since we drop data after 7 days, we only search the valid retention period.
    retention_window = datetime.now(timezone.utc) - timedelta(days=7)

    result = await db.execute(
        select(DeliveryAttempt)
        .where(
            DeliveryAttempt.event_id == event_id,
            DeliveryAttempt.created_at >= retention_window,  # 👈 Partition Pruning
        )
        .order_by(DeliveryAttempt.attempt_number)
    )
    return result.scalars().all()
