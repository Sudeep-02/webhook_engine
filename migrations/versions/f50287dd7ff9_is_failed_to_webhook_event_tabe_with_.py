"""is_failed to webhook_event tabe with new change

Revision ID: f50287dd7ff9
Revises: 2454bd1ac959
Create Date: 2026-04-02 22:36:35.723270

"""

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "f50287dd7ff9"
down_revision: Union[str, Sequence[str], None] = "2454bd1ac959"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
