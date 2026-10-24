"""Таблицы схемы `blocks`. Снаружи модуля их не импортируют."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, MetaData, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = MetaData(schema="blocks")
    type_annotation_map = {datetime: DateTime(timezone=True), dict[str, Any]: JSONB}  # noqa: RUF012


class BlockSpecRow(Base):
    __tablename__ = "block_specs"
    __table_args__ = (UniqueConstraint("name", "version"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    name: Mapped[str]
    version: Mapped[str]
    spec: Mapped[dict[str, Any]]
    registered_at: Mapped[datetime] = mapped_column(server_default=func.now())
