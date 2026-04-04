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
    correlation_id = get_correlation_id()
    start_time = time.time()

    logger.info(
        "webhook_received",
        correlation_id=correlation_id,
        event_type=event.event_type,
        idempotency_key=event.idempotency_key,
    )

    webhook_received_total.labels(event_type=event.event_type).inc()

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
            logger.warning(
                "redis_enqueue_failed_background_recovery_will_handle",
                event_id=event_id,
                error=str(e),
            )
            return {
                "id": event_id,
                "created_at": stable_timestamp,
                "message": "Event queued!",
            }

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
        if "unique constraint" in str(e).lower() or "duplicate key" in str(e).lower():
            logger.warning(
                "idempotency_collision_detected",
                idempotency_key=event.idempotency_key,
                correlation_id=correlation_id,
            )
            search_window = datetime.now(timezone.utc) - timedelta(hours=48)
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
