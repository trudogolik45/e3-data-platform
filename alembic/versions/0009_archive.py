"""Архив Dataset и Pipeline: физического удаления нет, только признак.

Revision ID: 0009
Revises: 0008
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ARCHIVABLE = (("catalog", "datasets"), ("pipelines", "pipelines"))


def upgrade() -> None:
    for schema, table in ARCHIVABLE:
        op.add_column(
            table,
            sa.Column("archived", sa.Boolean(), nullable=False, server_default=sa.false()),
            schema=schema,
        )


def downgrade() -> None:
    for schema, table in ARCHIVABLE:
        op.drop_column(table, "archived", schema=schema)
