from fastapi import APIRouter, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from app.redis_client import get_redis_status
from app.services.queue_service import QueueService
from app.core.correlation import get_correlation_id
from app.core.metrics import queue_depth_gauge

router = APIRouter()


@router.get("/health")
async def health_check():
    redis_ok = await get_redis_status()
    return {
        "status": "healthy" if redis_ok else "unhealthy",
        "correlation_id": get_correlation_id(),
    }


@router.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/queue/depth")
async def get_queue_stats():
    depth = await QueueService.get_queue_depth()
    queue_depth_gauge.set(depth)
    return {"pending_count": depth}
