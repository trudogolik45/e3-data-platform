"""Регистрация и чтение DatasetVersion."""

from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from e3.catalog.models import DatasetRow, DatasetVersionRow
from e3.shared.contracts import NodeOutputRef
from e3.shared.manifest import DataFormat, DataSchema


class DatasetVersionNotFound(Exception):
    """DatasetVersion с таким id нет."""


@dataclass(frozen=True)
class DatasetVersion:
    version_id: UUID
    dataset_id: UUID
    version_number: int
    format: DataFormat
    data_schema: DataSchema
    manifest_uri: str
    committed_at: datetime


async def register(
    session: AsyncSession,
    *,
    dataset_id: UUID,
    version_id: UUID,
    format: DataFormat,
    data_schema: DataSchema,
    manifest_uri: str,
    upload_id: UUID | None = None,
    source: NodeOutputRef | None = None,
) -> DatasetVersion:
    """Фиксирует DatasetVersion со следующим `version_number` внутри Dataset.

    Счётчик увеличивается под блокировкой строки Dataset, поэтому номера идут
    без пропусков в порядке фиксации. Транзакцией управляет вызывающий.
    """
    version_number = await session.scalar(
        update(DatasetRow)
        .where(DatasetRow.id == dataset_id)
        .values(last_version_number=DatasetRow.last_version_number + 1)
        .returning(DatasetRow.last_version_number)
    )
    row = DatasetVersionRow(
        id=version_id,
        dataset_id=dataset_id,
        version_number=version_number,
        format=format,
        data_schema=data_schema.model_dump(),
        manifest_uri=manifest_uri,
        upload_id=upload_id,
        source_node_run_id=source.node_run_id if source else None,
        source_port=source.port if source else None,
    )
    session.add(row)
    await session.flush()
    await session.refresh(row)
    return _to_version(row)


async def resolve(session: AsyncSession, version_id: UUID) -> DatasetVersion:
    row = await session.get(DatasetVersionRow, version_id)
    if row is None:
        raise DatasetVersionNotFound(str(version_id))
    return _to_version(row)


async def find_published(session: AsyncSession, source: NodeOutputRef) -> DatasetVersion | None:
    row = await session.scalar(
        select(DatasetVersionRow).where(
            DatasetVersionRow.source_node_run_id == source.node_run_id,
            DatasetVersionRow.source_port == source.port,
        )
    )
    return _to_version(row) if row else None


async def list_versions(session: AsyncSession, dataset_id: UUID) -> list[DatasetVersion]:
    rows = await session.scalars(
        select(DatasetVersionRow)
        .where(DatasetVersionRow.dataset_id == dataset_id)
        .order_by(DatasetVersionRow.version_number)
    )
    return [_to_version(row) for row in rows]


def _to_version(row: DatasetVersionRow) -> DatasetVersion:
    return DatasetVersion(
        version_id=row.id,
        dataset_id=row.dataset_id,
        version_number=row.version_number,
        format=cast(DataFormat, row.format),
        data_schema=DataSchema.model_validate(row.data_schema),
        manifest_uri=row.manifest_uri,
        committed_at=row.committed_at,
    )
