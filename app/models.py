from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy import String, DateTime, Boolean, func, Index, text, Integer
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase
from datetime import datetime, timezone
import uuid6
import uuid


class Base(DeclarativeBase):
    pass


class Subscription(Base):
    """Maps event types to client endpoints (No partitioning needed here)."""

    __tablename__ = "subscriptions"
    id: Mapped[int] = mapped_column(primary_key=True)
    event_type: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    target_url: Mapped[str] = mapped_column(String(255))
    secret: Mapped[str] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class WebhookEvent(Base):
    __tablename__ = "webhook_events"

    # 1. UUIDs are better for distributed systems at scale
    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, default=uuid6.uuid7)
    # 2. Partition Key MUST be part of the Primary Key
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        default=lambda: datetime.now(timezone.utc),
    )

    idempotency_key: Mapped[str] = mapped_column(String)
    event_type: Mapped[str] = mapped_column(String(50))
    payload: Mapped[dict] = mapped_column(JSONB)
    is_delivered: Mapped[bool] = mapped_column(Boolean, default=False)
    is_failed: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (
        # Position arguments (Indexes/Constraints) go first
        Index("idx_webhook_idempotency", "idempotency_key", "created_at", unique=True),
        Index(
            "idx_webhook_events_pending",
            "created_at",
            postgresql_where=text("is_delivered IS FALSE AND is_failed IS FALSE"),
        ),
        # Keyword arguments (Config dict) MUST be the last element
        {"postgresql_partition_by": "RANGE (created_at)"},
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"

    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, default=uuid6.uuid7)
    # 5. Delivery attempts should also be partitioned to stay in sync with events
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        default=lambda: datetime.now(timezone.utc),
    )

    # FKs in partitioned tables are tricky; usually, we link via ID
    # but cannot strictly enforce FK across different partitions in older PG versions.
    event_id: Mapped[uuid.UUID] = mapped_column(UUID, index=True)
    attempt_number: Mapped[int] = mapped_column(Integer)
    http_status: Mapped[int] = mapped_column(Integer, nullable=True)
    response_body: Mapped[dict] = mapped_column(JSONB, nullable=True)
    error_message: Mapped[str] = mapped_column(String, nullable=True)

    __table_args__ = (
        # Even if it's just the dict, it's safer to keep it consistent
        {"postgresql_partition_by": "RANGE (created_at)"},
    )
