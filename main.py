import asyncio
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, status, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from app.services.recovery_service import RecoveryService

# --- Infrastructure Imports ---
from app.database import get_db, engine
from app.models import WebhookEvent, DeliveryAttempt
from app.redis_client import redis_client, get_redis_status
from app.services.idempotency import IdempotencyService
from app.services.queue_service import QueueService
from app.workers.delivery_worker import DeliveryWorker

# --- New "Deep Systems" Core Imports ---
from app.core.logging_config import logger
from app.core.correlation import CorrelationIDMiddleware, get_correlation_id
from app.core.metrics import (
    webhook_received_total,
    queue_depth_gauge,
    start_metrics_server
)

# Initialize Background Worker
worker = DeliveryWorker()
worker_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_task
    logger.info("application_startup", message="🚀 Starting Webhook Engine...")

    # 1. Start Metrics Server (Port 8001)
    start_metrics_server(port=8001)

    # 2. Check Redis Connection & Update Gauge
    from app.core.metrics import redis_up_gauge # Ensure this is imported
    
    redis_is_ok = await get_redis_status()
    if redis_is_ok:
        redis_up_gauge.set(1)  # 👈 This makes the metric show 1.0
        logger.info("redis_connected", message="✅ Redis Connected!")
    else:
        redis_up_gauge.set(0)  # 👈 This makes the metric show 0.0
        logger.error("redis_error", message="❌ Warning: Redis is not available")

    # 3. Start Background Delivery Worker
    # The worker.start() method you updated earlier already sets active_workers_gauge to 1
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
    
    recovery_task.cancel()

    logger.info("application_shutdown", message="🛑 Shutting down...")
    
    # 4. Graceful Worker Shutdown
    # Setting these to 0 on shutdown prevents "stale" metrics if the process lingers
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


app = FastAPI(lifespan=lifespan, title="Reliable Webhook Engine v5")

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
        "correlation_id": get_correlation_id()
    }

@app.get("/metrics")
async def metrics():
    """Expose metrics on the main port as well."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/queue/depth")
async def get_queue_stats():
    depth = await QueueService.get_queue_depth()
    queue_depth_gauge.set(depth) # Update the prometheus gauge
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
        idempotency_key=event.idempotency_key
    )

    # Increment Prometheus Counter
    webhook_received_total.labels(event_type=event.event_type).inc()

    # 1. Idempotency Check
    existing_id = await IdempotencyService.get_cached_response(event.idempotency_key)
    if existing_id:
        logger.warning("duplicate_request", correlation_id=correlation_id, key=event.idempotency_key)
        return {"id": existing_id, "idempotent": True, "message": "Duplicate request"}

    # 2. Database & Queue Layer
    try:
        async with db.begin():
            db_event = WebhookEvent(
                idempotency_key=event.idempotency_key,
                event_type=event.event_type,
                payload=event.payload
            )
            db.add(db_event)
        
        await db.refresh(db_event)
        
        try:
            await QueueService.enqueue(db_event.id)
        except Exception as e:
            # We don't crash the request! 
            # The Recovery Sweeper will pick this up in 2 minutes.
            logger.warning("redis_enqueue_failed_background_recovery_will_handle", event_id=db_event.id,error=str(e))
            return {"id": db_event.id, "message": "Accepted"}
        
        # 3. Finalize Cache
        await IdempotencyService.check_and_set(
            idempotency_key=event.idempotency_key,
            event_id=db_event.id
        )

        latency_ms = round((time.time() - start_time) * 1000, 2)
        logger.info(
            "webhook_queued",
            correlation_id=correlation_id,
            event_id=db_event.id,
            latency_ms=latency_ms
        )

        return {"id": db_event.id, "idempotent": False, "message": "Event queued for delivery!"}

    except Exception as e:
        # Check if this is a "Unique Constraint" (Idempotency Collision)
        if "unique constraint" in str(e).lower() or "duplicate key" in str(e).lower():
            # 1. Log that we caught a duplicate (The "Log" you asked for)
            logger.warning(
                "idempotency_collision_detected", 
                idempotency_key=event.idempotency_key,
                correlation_id=correlation_id
            )
            
            # 2. Rescue: Fetch the existing ID from the database
            # Note: We use a fresh query here because the previous transaction failed
            result = await db.execute(
                select(WebhookEvent.id).where(WebhookEvent.idempotency_key == event.idempotency_key)
            )
            existing_id = result.scalar_one_or_none()

            # 3. Return a successful 200/201 response with the original ID
            return {
                "id": existing_id, 
                "idempotent": True, 
                "message": "Duplicate request, returning existing ID"
            }

        # If it's a real error (like DB connection lost), then we log and crash
        logger.error("event_creation_failed_critical", correlation_id=correlation_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal Server Error")

# --- Debugging Endpoints ---

@app.get("/events/")
async def list_events(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(WebhookEvent).limit(100))
    return result.scalars().all()

@app.get("/events/{event_id}/attempts")
async def get_event_attempts(event_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(DeliveryAttempt)
        .where(DeliveryAttempt.event_id == event_id)
        .order_by(DeliveryAttempt.attempt_number)
    )
    return result.scalars().all()