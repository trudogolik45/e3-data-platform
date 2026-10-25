"""Схема identity: Organization, Project, User, Membership; Initiator у PipelineRun.

Запуски, созданные до Initiator, достаются технической идентичности
`legacy_migration`: войти ею нельзя, Membership у неё нет (ADR-0004).

Revision ID: 0010
Revises: 0009
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "identity"
LEGACY_MIGRATION_USER_ID = "00000000-0000-4000-8000-0000000000ff"


def _created_at() -> "sa.Column[datetime]":
    return sa.Column(
        "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
    )


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    op.create_table(
        "organizations",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        _created_at(),
        schema=SCHEMA,
    )
    op.create_table(
        "projects",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "organization_id",
            sa.Uuid(),
            sa.ForeignKey(f"{SCHEMA}.organizations.id"),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=False),
        _created_at(),
        schema=SCHEMA,
    )
    op.create_table(
        "users",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("issuer", sa.String(), nullable=False),
        sa.Column("subject", sa.String(), nullable=False),
        sa.Column("email", sa.String(), nullable=True),
        sa.Column("email_verified", sa.Boolean(), nullable=False, server_default=sa.false()),
        _created_at(),
        sa.UniqueConstraint("issuer", "subject"),
        schema=SCHEMA,
    )
    op.create_index("users_email", "users", ["email"], schema=SCHEMA)
    op.create_table(
        "memberships",
        sa.Column(
            "project_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.projects.id"), primary_key=True
        ),
        sa.Column("user_id", sa.Uuid(), sa.ForeignKey(f"{SCHEMA}.users.id"), primary_key=True),
        sa.Column("role", sa.String(16), nullable=False),
        sa.CheckConstraint("role IN ('Viewer', 'Editor', 'Admin')", name="memberships_role"),
        schema=SCHEMA,
    )
    op.execute(
        f"INSERT INTO {SCHEMA}.users (id, issuer, subject)"
        f" VALUES ('{LEGACY_MIGRATION_USER_ID}', 'urn:e3:system', 'legacy_migration')"
    )
    op.add_column(
        "pipeline_runs", sa.Column("initiated_by_user_id", sa.Uuid(), nullable=True), schema="runs"
    )
    op.execute(f"UPDATE runs.pipeline_runs SET initiated_by_user_id = '{LEGACY_MIGRATION_USER_ID}'")
    op.alter_column("pipeline_runs", "initiated_by_user_id", nullable=False, schema="runs")


def downgrade() -> None:
    op.drop_column("pipeline_runs", "initiated_by_user_id", schema="runs")
    op.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
