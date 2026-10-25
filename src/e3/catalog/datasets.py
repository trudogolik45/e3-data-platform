"""Создание, поиск и архивирование Dataset внутри Project."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from e3.catalog.models import DatasetRow


class DatasetNameTaken(Exception):
    """В Project уже есть Dataset с таким нормализованным именем."""


class DatasetNotFound(Exception):
    """Dataset с таким id нет."""


@dataclass(frozen=True)
class Dataset:
    dataset_id: UUID
    project_id: UUID
    name: str
    created_at: datetime
    archived: bool


def normalize_name(name: str) -> str:
    """Обрезает края, схлопывает внутренние пробелы и приводит к casefold."""
    return " ".join(name.split()).casefold()


async def create_dataset(session: AsyncSession, *, project_id: UUID, name: str) -> Dataset:
    row = DatasetRow(
        id=uuid4(), project_id=project_id, name=name, normalized_name=normalize_name(name)
    )
    session.add(row)
    try:
        await session.flush()
    except IntegrityError:
        raise DatasetNameTaken(name) from None
    await session.refresh(row)
    return _to_dataset(row)


async def get_dataset(session: AsyncSession, dataset_id: UUID) -> Dataset:
    row = await session.get(DatasetRow, dataset_id)
    if row is None:
        raise DatasetNotFound(str(dataset_id))
    return _to_dataset(row)


async def lock_datasets(session: AsyncSession, dataset_ids: Iterable[UUID]) -> dict[UUID, Dataset]:
    """Блокирует строки на чтение до конца транзакции: архивация ждёт её конца.

    Строки берутся по возрастанию id, чтобы встречные блокировки не сцепились.
    """
    rows = await session.scalars(
        select(DatasetRow)
        .where(DatasetRow.id.in_(set(dataset_ids)))
        .order_by(DatasetRow.id)
        .with_for_update(read=True)
    )
    return {row.id: _to_dataset(row) for row in rows}


async def archive_dataset(session: AsyncSession, dataset_id: UUID) -> Dataset:
    """Скрывает Dataset из обращения; повтор ничего не меняет."""
    await session.execute(
        update(DatasetRow).where(DatasetRow.id == dataset_id).values(archived=True)
    )
    return await get_dataset(session, dataset_id)


def _to_dataset(row: DatasetRow) -> Dataset:
    return Dataset(row.id, row.project_id, row.name, row.created_at, row.archived)
