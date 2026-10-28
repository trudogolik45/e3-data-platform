"""Отметка об очистке `staging/uploads/{id}/` Upload по сроку хранения.

Revision ID: 0015
Revises: 0014
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "catalog"


def upgrade() -> None:
    op.add_column(
        "uploads",
        sa.Column("staging_purged_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    # Очистка ищет только ещё не очищенные Upload.
    op.create_index(
        "ix_uploads_staging_to_purge",
        "uploads",
        ["created_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("staging_purged_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_uploads_staging_to_purge", "uploads", schema=SCHEMA)
    op.drop_column("uploads", "staging_purged_at", schema=SCHEMA)
