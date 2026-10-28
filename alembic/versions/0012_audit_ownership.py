"""Audit Event изменений владения и доступа: Organization, Project и Membership.

`user_id = NULL` — действие Platform Team, `project_id = NULL` — создание Organization,
`object_id = NULL` — отказ в выдаче Membership по email. `role` и `previous_role` —
Role после и до изменения Membership. Старые записи не меняются, откат невозможен.

Revision ID: 0012
Revises: 0011
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "audit"
NULLABLE = ("user_id", "project_id", "object_id")


def upgrade() -> None:
    for column in NULLABLE:
        op.alter_column("events", column, nullable=True, schema=SCHEMA)
    op.add_column("events", sa.Column("role", sa.String(16), nullable=True), schema=SCHEMA)
    op.add_column("events", sa.Column("previous_role", sa.String(16), nullable=True), schema=SCHEMA)


def downgrade() -> None:
    # Записи без актора, Project или объекта в прежнюю схему не помещаются, а Audit Event
    # неизменяем: удалять их ради отката нельзя.
    raise NotImplementedError("0012 необратима: Audit Event не удаляются")
