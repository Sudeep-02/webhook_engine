"""create_partitioned_webhooks

Revision ID: a3cfee2d26a7
Revises: d685420f1ecf
Create Date: 2026-04-02 20:13:46.674807

"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "a3cfee2d26a7"
down_revision: Union[str, Sequence[str], None] = "d685420f1ecf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 1. Drop the old indexes and table first
    # (Order matters: Drop attempts first because of the FK)
    op.drop_table("delivery_attempts")
    op.drop_table("webhook_events")

    # 1. Create the Parent Tables (One per execute)
    op.execute("""
        CREATE TABLE webhook_events (
            id UUID NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            idempotency_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload JSONB,
            is_delivered BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at);
    """)

    # 2. Create the Indexes (Split into separate calls)
    op.execute("""
        CREATE UNIQUE INDEX idx_webhook_idempotency 
        ON webhook_events (idempotency_key, created_at);
    """)

    op.execute("""
        CREATE INDEX idx_webhook_events_pending 
        ON webhook_events (created_at) 
        WHERE (is_delivered IS FALSE);
    """)

    # 3. Create Attempts Table
    op.execute("""
        CREATE TABLE delivery_attempts (
            id UUID NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            event_id UUID NOT NULL,
            attempt_number INTEGER NOT NULL,
            http_status INTEGER,
            response_body JSONB,
            error_message TEXT,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at);
    """)

    # 4. Create the First Partitions (Split these too!)
    op.execute("""
        CREATE TABLE webhook_events_y2026_m04 PARTITION OF webhook_events
        FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
    """)

    op.execute("""
        CREATE TABLE delivery_attempts_y2026_m04 PARTITION OF delivery_attempts
        FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
    """)


def downgrade():
    op.drop_table("delivery_attempts")
    op.drop_table("webhook_events")
