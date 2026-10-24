"""Схема catalog: DatasetVersion помнит опубликованный NodeOutput.

Revision ID: 0004
Revises: 0003
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "catalog"
TABLE = "dataset_versions"
CONSTRAINT = "dataset_versions_source_node_run_id_source_port_key"


def upgrade() -> None:
    op.add_column(TABLE, sa.Column("source_node_run_id", sa.Uuid(), nullable=True), schema=SCHEMA)
    op.add_column(TABLE, sa.Column("source_port", sa.String(), nullable=True), schema=SCHEMA)
    op.create_unique_constraint(
        CONSTRAINT, TABLE, ["source_node_run_id", "source_port"], schema=SCHEMA
    )


def downgrade() -> None:
    op.drop_constraint(CONSTRAINT, TABLE, schema=SCHEMA)
    op.drop_column(TABLE, "source_port", schema=SCHEMA)
    op.drop_column(TABLE, "source_node_run_id", schema=SCHEMA)
