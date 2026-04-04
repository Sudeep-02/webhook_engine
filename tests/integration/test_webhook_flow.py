import pytest
import uuid
from sqlalchemy import select
from datetime import datetime, timezone

# Internal imports for verification
from app.models import WebhookEvent
from app.database import AsyncSessionLocal
from app.redis_client import redis_client


@pytest.mark.asyncio
async def test_create_event_success(integration_client):
    """
    Test 1: Standard Flow
    Verifies that a valid payload creates a DB record and a Redis cache entry.
    """
    idempotency_key = f"key-{uuid.uuid4()}"
    payload = {
        "event_type": "order.created",
        "payload": {"order_id": "123", "total": 50.00},
        "idempotency_key": idempotency_key,
    }

    # 1. Action
    response = await integration_client.post("/events/", json=payload)

    # 2. Assert Response
    assert response.status_code == 201
    data = response.json()
    assert "id" in data
    assert data["idempotent"] is False

    # 3. Verify Database (Partitioned Table)
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(WebhookEvent).where(WebhookEvent.idempotency_key == idempotency_key)
        )
        db_event = result.scalar_one_or_none()
        assert db_event is not None
        assert db_event.event_type == "order.created"

    # 4. Verify Redis Idempotency Cache
    redis_val = await redis_client.get(f"idempotency:{idempotency_key}")
    assert redis_val is not None


@pytest.mark.asyncio
async def test_idempotency_collision_handling(integration_client):
    """
    Test 2: The Collision Flow (Issue #2 Fix)
    Verifies that sending the same key twice triggers the 'except' block
    and returns the ORIGINAL ID instead of creating a duplicate.
    """
    idempotency_key = "shared-key-123"
    payload = {
        "event_type": "payment.processed",
        "payload": {"amount": 10},
        "idempotency_key": idempotency_key,
    }

    # 1. First Request (Creates the record)
    res1 = await integration_client.post("/events/", json=payload)
    assert res1.status_code == 201
    id1 = res1.json()["id"]

    # 2. Second Request (Same key, same 'deterministic' timestamp second)
    res2 = await integration_client.post("/events/", json=payload)

    # 3. Assertions
    # Note: Depending on your main.py, this might return 201 or 200,
    # but the key is that 'idempotent' must be True.
    assert res2.status_code in [200, 201]
    data2 = res2.json()
    assert data2["id"] == id1  # Must return the SAME ID
    assert data2["idempotent"] is True
    assert "Duplicate request" in data2["message"]


@pytest.mark.asyncio
async def test_health_check(integration_client):
    """
    Test 3: Infrastructure Check
    Verifies Redis status reporting via the health endpoint.
    """
    response = await integration_client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


@pytest.mark.asyncio
async def test_invalid_payload_error(integration_client):
    """
    Test 4: Validation Check (Issue #3 Fix)
    Verifies that a string payload triggers a 422 error.
    """
    bad_payload = {
        "event_type": "test",
        "payload": '{"this_is": "a_string_not_a_dict"}',  # Wrong type
        "idempotency_key": "bad-payload-key",
    }

    response = await integration_client.post("/events/", json=bad_payload)
    assert response.status_code == 422
