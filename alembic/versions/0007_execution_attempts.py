"""История ExecutionAttempt NodeRun: номер, итог и проверки reconciliation.

Revision ID: 0007
Revises: 0006
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "runs"


def upgrade() -> None:
    op.create_table(
        "execution_attempts",
        sa.Column("attempt_id", sa.Uuid(), primary_key=True),
        sa.Column(
            "node_run_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.node_runs.id"), nullable=False
        ),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("reason", sa.String(32), nullable=True),
        sa.Column("error", sa.String(), nullable=True),
        sa.Column("command", JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconcile_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconcile_count", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("node_run_id", "number"),
        sa.CheckConstraint("number BETWEEN 1 AND 4", name="execution_attempts_number"),
        schema=SCHEMA,
    )
    op.create_index(
        "execution_attempts_live",
        "execution_attempts",
        ["created_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("state IN ('SUBMITTED', 'RUNNING')"),
    )


def downgrade() -> None:
    op.drop_table("execution_attempts", schema=SCHEMA)
