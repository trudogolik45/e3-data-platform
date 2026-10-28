"""Таблица схемы `audit`. Снаружи модуля её не импортируют.

Код приложения в неё только добавляет: UPDATE и DELETE нет.
"""

from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, Index, MetaData, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = MetaData(schema="audit")
    type_annotation_map = {datetime: DateTime(timezone=True)}  # noqa: RUF012


class AuditEventRow(Base):
    __tablename__ = "events"
    # NULL в `dedup_key` не конфликтует (NULLS DISTINCT): каждый отказ — своя запись.
    __table_args__ = (Index("events_action_dedup_key", "action", "dedup_key", unique=True),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(server_default=func.now())
    action: Mapped[str] = mapped_column(String(32))
    outcome: Mapped[str] = mapped_column(String(16))
    # NULL — Platform Team.
    user_id: Mapped[UUID | None]
    # NULL — создание Organization.
    project_id: Mapped[UUID | None]
    # NULL — отказ в выдаче Membership по email: пользователя не ищут.
    object_id: Mapped[UUID | None]
    role: Mapped[str | None] = mapped_column(String(16))
    previous_role: Mapped[str | None] = mapped_column(String(16))
    dedup_key: Mapped[str | None]
    trace_id: Mapped[str | None]
