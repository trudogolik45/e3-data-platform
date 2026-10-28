"""Время завершения PipelineRun и отметка об очистке staging его попыток.

У запусков, завершившихся до миграции, `finished_at` остаётся NULL, и их staging не чистится.

Revision ID: 0013
Revises: 0012
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "runs"


def upgrade() -> None:
    op.add_column(
        "pipeline_runs",
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "pipeline_runs",
        sa.Column("staging_purged_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    # Очистка ищет только завершённые и ещё не очищенные запуски.
    op.create_index(
        "ix_pipeline_runs_staging_to_purge",
        "pipeline_runs",
        ["finished_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("finished_at IS NOT NULL AND staging_purged_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_pipeline_runs_staging_to_purge", "pipeline_runs", schema=SCHEMA)
    op.drop_column("pipeline_runs", "staging_purged_at", schema=SCHEMA)
    op.drop_column("pipeline_runs", "finished_at", schema=SCHEMA)
