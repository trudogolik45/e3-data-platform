"""Схема pipelines: Pipeline с Draft и PipelineVersion.

Revision ID: 0003
Revises: 0002
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "pipelines"


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    op.create_table(
        "pipelines",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("draft", JSONB(), nullable=False),
        sa.Column("last_version_number", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        schema=SCHEMA,
    )
    op.create_table(
        "pipeline_versions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "pipeline_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.pipelines.id"), nullable=False
        ),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("graph", JSONB(), nullable=False),
        sa.Column(
            "published_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("pipeline_id", "version_number"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
