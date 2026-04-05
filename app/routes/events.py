import time
import uuid
import uuid6
from datetime import datetime, timedelta, timezone
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

from app.database import get_db
from app.models import WebhookEvent, DeliveryAttempt
from app.services.idempotency import IdempotencyService
from app.services.queue_service import QueueService
from app.core.logging_config import logger
from app.core.correlation import get_correlation_id
from app.core.metrics import webhook_received_total

router = APIRouter(prefix="/events", tags=["events"])


class EventCreate(BaseModel):
    event_type: str
    payload: dict
    idempotency_key: str


@router.post("/", status_code=status.HTTP_201_CREATED)
async def create_event(event: EventCreate, db: AsyncSession = Depends(get_db)):
    event_id = uuid6.uuid7()
    correlation_id = get_correlation_id()
    request_start = time.time()
    steps = {}
    
    logger.info(
        "webhook_received",
        correlation_id=correlation_id,
        event_type=event.event_type,
        idempotency_key=event.idempotency_key,
    )
    webhook_received_total.labels(event_type=event.event_type).inc()

    step_start = time.perf_counter()
    is_first_time = await IdempotencyService.check_and_set(
        idempotency_key=event.idempotency_key, event_id=event_id
    )
    steps["idempotency_check_ms"] = round((time.perf_counter() - step_start) * 1000, 1)
    
    if not is_first_time:
        logger.warning(
            "duplicate_request",
            correlation_id=correlation_id,
            key=event.idempotency_key,
        )
        return {"id": event_id, "idempotent": True, "message": "Duplicate request"}
        
    
    timestamp= datetime.now(timezone.utc) 
    step_start = time.perf_counter()
    
    
    try:
        async with db.begin():
            db_event = WebhookEvent(
                id=event_id,
                idempotency_key=event.idempotency_key,
                created_at=timestamp,
                event_type=event.event_type,
                payload=event.payload,
            )
            db.add(db_event)
        steps["db_insert_ms"] = round((time.perf_counter() - step_start) * 1000, 1)
        
        
        step_start = time.perf_counter()
        # Queue enqueue
        try:
            await QueueService.enqueue(event_id)
        except Exception as e:
            logger.warning(
                "redis_enqueue_failed_background_recovery_will_handle",
                event_id=event_id,
                error=str(e),
            )
            return {
                "id": event_id,
                "created_at":timestamp,
                "message": "Event queued!",
            }
        steps["redis_enqueue_ms"] = round((time.perf_counter() - step_start) * 1000, 1)
      
      
        total_latency_ms = round((time.time() - request_start) * 1000, 2)       
        slow_steps = {k: v for k, v in steps.items() if v > 50}
        
        
        if slow_steps:
            logger.warning(
                "slow_request_steps",
                correlation_id=correlation_id,
                event_id=event_id,
                total_latency_ms=total_latency_ms,
                **slow_steps,
            )
            
            
        logger.info(
            "webhook_queued",
            correlation_id=correlation_id,
            event_id=event_id,
            latency_ms=total_latency_ms,
            steps=steps,  # Include all step timings in log
        )

        return {
            "id": event_id,
            "created_at": timestamp,
            "idempotent": False,
            "message": "Event queued for delivery!",
        }

    except Exception as e:
        if "unique constraint" in str(e).lower() or "duplicate key" in str(e).lower():
            logger.warning(
                "idempotency_collision_detected",
                idempotency_key=event.idempotency_key,
                correlation_id=correlation_id,
            )
            search_window = datetime.now(timezone.utc) - timedelta(hours=24)
            result = await db.execute(
                select(WebhookEvent.id).where(
                    WebhookEvent.idempotency_key == event.idempotency_key,
                    WebhookEvent.created_at >= search_window,
                )
            )
            existing_id = result.scalar_one_or_none()
            return {
                "id": existing_id,
                "idempotent": True,
                "message": "Duplicate request, returning existing ID",
            }

        logger.error(
            "event_creation_failed_critical",
            correlation_id=correlation_id,
            error=str(e),
        )
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get("/")
async def list_events(db: AsyncSession = Depends(get_db)):
    day_ago = datetime.now(timezone.utc) - timedelta(days=1)
    result = await db.execute(
        select(WebhookEvent)
        .where(WebhookEvent.created_at >= day_ago)
        .order_by(WebhookEvent.created_at.desc())
        .limit(100)
    )
    return result.scalars().all()


@router.get("/{event_id}/attempts")
async def get_event_attempts(event_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    retention_window = datetime.now(timezone.utc) - timedelta(days=7)
    result = await db.execute(
        select(DeliveryAttempt)
        .where(
            DeliveryAttempt.event_id == event_id,
            DeliveryAttempt.created_at >= retention_window,
        )
        .order_by(DeliveryAttempt.attempt_number)
    )
    return result.scalars().all()
