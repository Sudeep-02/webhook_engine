from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Integer, String, DateTime, Boolean, ForeignKey,func
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase
from datetime import datetime, timezone

class Base(DeclarativeBase):
    pass

class Subscription(Base):
    """Maps event types to client endpoints."""
    __tablename__ = "subscriptions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_type: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    target_url: Mapped[str] = mapped_column(String(255))
    secret: Mapped[str] = mapped_column(String(255)) # For HMAC signing
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class WebhookEvent(Base):
    __tablename__ = "webhook_events"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    idempotency_key: Mapped[str] = mapped_column(String, unique=True, index=True)
    event_type: Mapped[str] = mapped_column(String(50))
    
    # Using Postgres-specific JSONB
    # Mapped[dict] tells Python it's a dictionary
    # mapped_column(JSONB) tells Postgres it's a binary JSON column
    payload: Mapped[dict] = mapped_column(JSONB)
    
    is_delivered: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    
    
    def __repr__(self) -> str:
        return f"<WebhookEvent(id={self.id}, type={self.event_type}, key={self.idempotency_key})>"

class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[int] = mapped_column(Integer, ForeignKey("webhook_events.id"), index=True)
    attempt_number: Mapped[int] = mapped_column(Integer)
    http_status: Mapped[int] = mapped_column(Integer, nullable=True)
    
    # If you want to store the response body as JSONB too:
    response_body: Mapped[dict] = mapped_column(JSONB, nullable=True)
    
    error_message: Mapped[str] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    
    def __repr__(self) -> str:
        # For attempts, the status code and attempt number are the most critical info
        return f"<DeliveryAttempt(event_id={self.event_id}, attempt={self.attempt_number}, status={self.http_status})>"
    