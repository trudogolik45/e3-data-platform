"""Отметка об очистке Diagnostic Artifact попыток PipelineRun.

Revision ID: 0014
Revises: 0013
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "runs"


def upgrade() -> None:
    op.add_column(
        "pipeline_runs",
        sa.Column("artifacts_purged_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    # Очистка ищет только завершённые запуски с ещё не удалёнными логами.
    op.create_index(
        "ix_pipeline_runs_artifacts_to_purge",
        "pipeline_runs",
        ["finished_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("finished_at IS NOT NULL AND artifacts_purged_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_pipeline_runs_artifacts_to_purge", "pipeline_runs", schema=SCHEMA)
    op.drop_column("pipeline_runs", "artifacts_purged_at", schema=SCHEMA)
