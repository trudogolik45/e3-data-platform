"""Таблицы схемы `pipelines`. Снаружи модуля их не импортируют."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, MetaData, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = MetaData(schema="pipelines")
    type_annotation_map = {datetime: DateTime(timezone=True), dict[str, Any]: JSONB}  # noqa: RUF012


class PipelineRow(Base):
    __tablename__ = "pipelines"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    project_id: Mapped[UUID]
    name: Mapped[str]
    draft: Mapped[dict[str, Any]]
    last_version_number: Mapped[int] = mapped_column(default=0)
    archived: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class PipelineVersionRow(Base):
    __tablename__ = "pipeline_versions"
    __table_args__ = (UniqueConstraint("pipeline_id", "version_number"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    pipeline_id: Mapped[UUID] = mapped_column(ForeignKey("pipelines.id"))
    version_number: Mapped[int]
    graph: Mapped[dict[str, Any]]
    published_at: Mapped[datetime] = mapped_column(server_default=func.now())
