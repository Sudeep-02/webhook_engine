import asyncio
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import observability, events

# --- Infrastructure & Services ---
from app.database import engine, AsyncSessionLocal
from app.models import Base
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
from app.core.correlation import CorrelationIDMiddleware
from app.core.metrics import (
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




scheduler = None
worker = None
worker_task = None
async def run_partition_maintenance():

    max_retries = 5
    for attempt in range(max_retries):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            async with AsyncSessionLocal() as session:
                manager = PartitionManager(session)
                await manager.sync_partitions()

            logger.info(
                "partition_init_success", message="Database partitions verified."
            )
            break  # Exit loop on success
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(
                    "partition_init_retry", attempt=attempt + 1, error=str(e)
                )
                await asyncio.sleep(2)  # Wait 2 seconds before retrying
            else:
                logger.error("partition_init_failed_final", error=str(e))
                raise  # Re-raise if all retries fail


@asynccontextmanager
async def lifespan(app: FastAPI):
    global worker_task, scheduler, worker
    
    scheduler = AsyncIOScheduler()
    worker = DeliveryWorker()
    recovery_task = None

    # CHANGED: Detect if this container is an API or a WORKER ---
    mode = os.getenv("APP_MODE", "api")
    logger.info(
        "application_startup", mode=mode, message="🚀 Starting Webhook Engine..."
    )

    # --- CHANGED: Pointed endpoint to 'jaeger' service name for Docker DNS ---
    resource = Resource(attributes={"service.name": "webhook-engine"})
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(
        OTLPSpanExporter(endpoint="http://jaeger:4317", insecure=True)
    )
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    logger.info("tracing_init", message="OpenTelemetry Tracing connected to Jaeger.")

    # --- CHANGED: Start Metrics server on BOTH (API & Workers) so Prometheus can scrape both ---
    # start_metrics_server(port=8001)

    # Check Redis Connection & Update Gauge
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
        worker_task = asyncio.create_task(worker.start())
        logger.info("worker_task_started", message="Delivery worker loop active.")

    yield  # Application runs here

    logger.info(
        "application_shutdown", mode=mode, message="Shutting down gracefully..."
    )

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

    if mode == "worker" and worker_task:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        logger.info("worker_task_cancelled", message="Delivery loop stopped.")

    # Cleanup Connections
    await redis_client.aclose()
    await engine.dispose()
    logger.info("cleanup_complete", message="✨ Cleanup complete.")


app = FastAPI(lifespan=lifespan, title="Reliable Webhook Engine")

app.include_router(observability.router)
app.include_router(events.router)


instrumentator = Instrumentator().instrument(app)
FastAPIInstrumentor.instrument_app(app)


app.add_middleware(CorrelationIDMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


if __name__ == "__main__":
    import uvicorn
    import os

    mode = os.getenv("APP_MODE", "api")
    port = int(os.getenv("PORT", 8000))

    if mode == "api":
        logger.info("starting_api_server", port=port)
        uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False, workers=1)
    else:
        # --- FIXED: Use uvicorn even for the worker to trigger the lifespan ---
        logger.info("starting_worker_mode", port=port)
        uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False, workers=1)
