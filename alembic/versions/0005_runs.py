"""Схема runs: PipelineRun, NodeRun, NodeOutput, outbox и inbox.

Revision ID: 0005
Revises: 0004
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "runs"


def _now(name: str) -> sa.Column[datetime]:
    return sa.Column(name, sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("pipeline_id", sa.Uuid(), nullable=False),
        sa.Column("pipeline_version", sa.Integer(), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("inputs", JSONB(), nullable=False),
        sa.Column("outputs", JSONB(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("request_hash", sa.String(), nullable=False),
        _now("created_at"),
        sa.UniqueConstraint("project_id", "idempotency_key"),
        schema=SCHEMA,
    )
    op.create_index(
        "pipeline_runs_pending",
        "pipeline_runs",
        ["created_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("state = 'PENDING'"),
    )
    op.create_table(
        "node_runs",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "pipeline_run_id",
            sa.Uuid(),
            sa.ForeignKey(f"{SCHEMA}.pipeline_runs.id"),
            nullable=False,
        ),
        sa.Column("node_id", sa.String(), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("inputs", JSONB(), nullable=False),
        sa.Column("parameters", JSONB(), nullable=False),
        sa.Column("executable_ref", sa.String(), nullable=True),
        sa.Column("output_ports", JSONB(), nullable=False),
        sa.Column("target_dataset_id", sa.Uuid(), nullable=True),
        sa.Column("current_attempt_id", sa.Uuid(), nullable=True),
        sa.Column("result", JSONB(), nullable=True),
        sa.Column("published_version_id", sa.Uuid(), nullable=True),
        sa.Column("error", sa.String(), nullable=True),
        sa.UniqueConstraint("pipeline_run_id", "node_id"),
        schema=SCHEMA,
    )
    op.create_index("node_runs_state", "node_runs", ["state"], schema=SCHEMA)
    op.create_table(
        "node_outputs",
        sa.Column(
            "node_run_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.node_runs.id"), primary_key=True
        ),
        sa.Column("port", sa.String(), primary_key=True),
        sa.Column("manifest_uri", sa.String(), nullable=False),
        sa.Column("format", sa.String(16), nullable=False),
        sa.Column("data_schema", JSONB(), nullable=False),
        _now("accepted_at"),
        schema=SCHEMA,
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("topic", sa.String(), nullable=False),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("payload", JSONB(), nullable=False),
        _now("created_at"),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.create_index(
        "outbox_unpublished",
        "outbox",
        ["created_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "inbox",
        sa.Column("event_id", sa.Uuid(), primary_key=True),
        _now("received_at"),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
