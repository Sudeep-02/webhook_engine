"""remove_redundant_index_and_add_performance_indexes

Revision ID: d685420f1ecf
Revises: ae0eecbafb13
Create Date: 2026-04-02 18:12:01.263266

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "d685420f1ecf"
down_revision: Union[str, Sequence[str], None] = "ae0eecbafb13"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # 2. ADD the Partial Index for the Worker (The "Pending" Queue)
    op.create_index(
        "idx_webhook_events_pending",
        "webhook_events",
        ["created_at"],
        postgresql_where=sa.text("is_delivered IS FALSE"),
    )

    # 3. ADD a Full Index on created_at for the Fallback Sweeper
    op.create_index("ix_webhook_events_created_at", "webhook_events", ["created_at"])


def downgrade():
    # Reverse the steps to roll back if needed
    op.drop_index("ix_webhook_events_created_at", table_name="webhook_events")
    op.drop_index("idx_webhook_events_pending", table_name="webhook_events")
