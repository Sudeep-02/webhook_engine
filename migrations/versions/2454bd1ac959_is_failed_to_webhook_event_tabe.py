"""is_failed to webhook_event tabe

Revision ID: 2454bd1ac959
Revises: a3cfee2d26a7
Create Date: 2026-04-02 22:35:46.338143

"""

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "2454bd1ac959"
down_revision: Union[str, Sequence[str], None] = "a3cfee2d26a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
