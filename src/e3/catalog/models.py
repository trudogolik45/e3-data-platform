"""Таблицы схемы `catalog`. Снаружи модуля их не импортируют."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, MetaData, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = MetaData(schema="catalog")
    type_annotation_map = {datetime: DateTime(timezone=True), dict[str, Any]: JSONB}  # noqa: RUF012


class DatasetRow(Base):
    __tablename__ = "datasets"
    __table_args__ = (UniqueConstraint("project_id", "normalized_name"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    project_id: Mapped[UUID]
    name: Mapped[str]
    normalized_name: Mapped[str]
    last_version_number: Mapped[int] = mapped_column(default=0)
    archived: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class UploadRow(Base):
    __tablename__ = "uploads"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    dataset_id: Mapped[UUID] = mapped_column(ForeignKey("datasets.id"))
    state: Mapped[str] = mapped_column(String(16))
    declared_checksum_sha256: Mapped[str]
    version_id: Mapped[UUID | None]
    claim_token: Mapped[UUID | None]
    lease_expires_at: Mapped[datetime | None]
    error: Mapped[str | None]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    # Удалён `staging/uploads/{id}/`: срок хранения от `created_at` истёк.
    staging_purged_at: Mapped[datetime | None]


class DatasetVersionRow(Base):
    __tablename__ = "dataset_versions"
    __table_args__ = (
        UniqueConstraint("dataset_id", "version_number"),
        # Один NodeOutput публикуется не больше одного раза.
        UniqueConstraint("source_node_run_id", "source_port"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    dataset_id: Mapped[UUID] = mapped_column(ForeignKey("datasets.id"))
    version_number: Mapped[int]
    format: Mapped[str] = mapped_column(String(16))
    data_schema: Mapped[dict[str, Any]]
    manifest_uri: Mapped[str]
    upload_id: Mapped[UUID | None] = mapped_column(ForeignKey("uploads.id"), unique=True)
    source_node_run_id: Mapped[UUID | None]
    source_port: Mapped[str | None]
    committed_at: Mapped[datetime] = mapped_column(server_default=func.now())
