"""Первопричина Fail-fast у PipelineRun: первый упавший NodeRun.

Revision ID: 0008
Revises: 0007
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "runs"


def upgrade() -> None:
    op.add_column(
        "pipeline_runs", sa.Column("failed_node_run_id", sa.Uuid(), nullable=True), schema=SCHEMA
    )
    # Запуски с уже упавшим узлом: иначе отмена приняла бы идущий Fail-fast за обычный запуск.
    op.execute(
        f"""
        UPDATE {SCHEMA}.pipeline_runs r
        SET failed_node_run_id = (
            SELECT n.id FROM {SCHEMA}.node_runs n
            WHERE n.pipeline_run_id = r.id AND n.state = 'FAILED'
            LIMIT 1
        )
        """
    )


def downgrade() -> None:
    op.drop_column("pipeline_runs", "failed_node_run_id", schema=SCHEMA)
