import asyncio
import time
import uuid
import uuid6
import os

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

# --- NEW: OpenTelemetry Imports for Jaeger ---
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_fastapi_instrumentator import Instrumentator

# Initialize the scheduler
scheduler = AsyncIOScheduler()


async def run_partition_maintenance():
    """Daily Maintenance Task with Retry Logic."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            async with AsyncSessionLocal() as session:
                manager = PartitionManager(session)
                await manager.sync_partitions()
            
            logger.info("partition_init_success", message="Database partitions verified.")
            break # Exit loop on success
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning("partition_init_retry", attempt=attempt+1, error=str(e))
                await asyncio.sleep(2) # Wait 2 seconds before retrying
            else:
                logger.error("partition_init_failed_final", error=str(e))
                raise # Re-raise if all retries fail

# Initialize Background Worker
worker = DeliveryWorker()
worker_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_task
    
    recovery_task = None
    
   # --- CHANGED: Detect if this container is an API or a WORKER ---
    mode = os.getenv("APP_MODE", "api")
    logger.info("application_startup", mode=mode, message="🚀 Starting Webhook Engine...")
    
    # --- CHANGED: Pointed endpoint to 'jaeger' service name for Docker DNS ---
    resource = Resource(attributes={"service.name": "webhook-engine"})
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317", insecure=True))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    logger.info("tracing_init", message="OpenTelemetry Tracing connected to Jaeger.")

    # --- CHANGED: Start Metrics server on BOTH (API & Workers) so Prometheus can scrape both ---
    # start_metrics_server(port=8001)
    
    #Check Redis Connection & Update Gauge
    redis_is_ok = await get_redis_status()
    if redis_is_ok:
        redis_up_gauge.set(1)
        logger.info("redis_connected", message="Redis Connected!")
    else:
        redis_up_gauge.set(0)
        logger.error("redis_error", message="Warning: Redis is not available")
        
    # --- CHANGED: API-ONLY RESPONSIBILITIES ---
    if mode == "api":
        try:
            await run_partition_maintenance()
            logger.info("partition_init", message="Database partitions verified.")
        except Exception as e:
            logger.error("partition_init_failed", error=str(e))

        scheduler.add_job(run_partition_maintenance, "cron", hour=0, minute=5)
        scheduler.start()
        
        # Start the Recovery Sweeper Loop ONLY on the API node
        async def recovery_loop():
            while True:
                try:
                    await RecoveryService.sweep_stuck_events()
                except Exception as e:
                    logger.error("recovery_loop_error", error=str(e))
                await asyncio.sleep(60)
        recovery_task = asyncio.create_task(recovery_loop())
        logger.info("recovery_task_started", message="Stuck event sweeper active.")
    

    if mode == "worker":
        # Only start the delivery loop if we are a dedicated worker
        worker_task = asyncio.create_task(worker.start())
        logger.info("worker_task_started", message="Delivery worker loop active.")
           
    yield  # Application runs here

    logger.info("application_shutdown", mode=mode, message="Shutting down gracefully...")

   # A. Shutdown Scheduler (API only)
    if mode == "api" and scheduler.running:
        scheduler.shutdown()
        logger.info("scheduler_shutdown", message="Partition maintenance stopped.")
        
        
    if mode == "api" and recovery_task:
        recovery_task.cancel()
        try:
            await recovery_task
        except asyncio.CancelledError:
            pass
        logger.info("recovery_task_cancelled", message="Sweeper loop stopped.")


    # C. Cleanup Worker Task (Worker only)
    if mode == "worker" and worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        logger.info("worker_task_cancelled", message="Delivery loop stopped.")
        
        
    #Cleanup Connections
    await redis_client.aclose()
    await engine.dispose()
    logger.info("cleanup_complete", message="✨ Cleanup complete.")


app = FastAPI(lifespan=lifespan, title="Reliable Webhook Engine")


instrumentator = Instrumentator().instrument(app)

# Automatically trace all FastAPI routes
FastAPIInstrumentor.instrument_app(app)

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
    event_id = uuid6.uuid7()
    stable_timestamp = now.replace(second=0, microsecond=0)
    
    
    # 2. Database & Queue Layer
    try:
        async with db.begin():
            db_event = WebhookEvent(
                id=event_id,
                idempotency_key=event.idempotency_key,
                created_at=stable_timestamp,
                event_type=event.event_type,
                payload=event.payload,
            )
            db.add(db_event)

        try:
            await QueueService.enqueue(event_id)
        except Exception as e:
            # We don't crash the request!
            # The Recovery Sweeper will pick this up in 2 minutes.
            logger.warning(
                "redis_enqueue_failed_background_recovery_will_handle",
                event_id=event_id,
                error=str(e),
            )
            return {
                "id": event_id,
                "created_at": stable_timestamp, # no need to send it is not worth 1 db query
                "message": "Event queued!",
            }

        # 3. Finalize Cache
        await IdempotencyService.check_and_set(
            idempotency_key=event.idempotency_key, event_id=event_id
        )

        latency_ms = round((time.time() - start_time) * 1000, 2)
        logger.info(
            "webhook_queued",
            correlation_id=correlation_id,
            event_id=event_id,
            latency_ms=latency_ms,
        )

        return {
            "id": event_id,
            "created_at": stable_timestamp,
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


if __name__ == "__main__":
    import uvicorn
    import os

    mode = os.getenv("APP_MODE", "api")
    port = int(os.getenv("PORT", 8000))

    if mode == "api":
        logger.info("starting_api_server", port=port)
        uvicorn.run(
            "main:app", 
            host="0.0.0.0", 
            port=port, 
            reload=False,
            workers=1
        )
    else:
        # --- FIXED: Use uvicorn even for the worker to trigger the lifespan ---
        logger.info("starting_worker_mode", port=port)
        uvicorn.run(
            "main:app", 
            host="0.0.0.0", 
            port=port, 
            reload=False,
            workers=1
        )