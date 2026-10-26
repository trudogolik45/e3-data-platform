"""Публичный фасад Pipeline Service: Pipeline, его Draft и PipelineVersion."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from n3 import catalog
from n3.blocks import BlockNotFound, BlockRegistry
from n3.pipelines.defaults import check_defaults
from n3.pipelines.graph import BlockRef, PipelineGraph
from n3.pipelines.models import PipelineRow, PipelineVersionRow
from n3.pipelines.validation import validate_graph
from n3.shared.contracts import BlockSpec


class PipelineNotFound(Exception):
    """Pipeline с таким id нет."""


class PipelineVersionNotFound(Exception):
    """У Pipeline нет версии с таким номером."""


class PipelineArchived(Exception):
    """Pipeline в архиве: его нельзя менять и запускать."""


@dataclass(frozen=True)
class Pipeline:
    pipeline_id: UUID
    project_id: UUID
    name: str
    created_at: datetime
    archived: bool


@dataclass(frozen=True)
class PipelineVersion:
    version_id: UUID
    pipeline_id: UUID
    version_number: int
    graph: PipelineGraph
    published_at: datetime


class PipelineService:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        blocks: BlockRegistry,
        datasets: catalog.Catalog,
    ) -> None:
        self._sessions = sessions
        self._blocks = blocks
        self._datasets = datasets

    async def create(self, *, project_id: UUID, name: str) -> Pipeline:
        row = PipelineRow(
            id=uuid4(),
            project_id=project_id,
            name=name,
            draft=PipelineGraph().model_dump(mode="json"),
        )
        async with self._sessions.begin() as session:
            session.add(row)
            await session.flush()
            await session.refresh(row)
            return _to_pipeline(row)

    async def get(self, pipeline_id: UUID) -> Pipeline:
        async with self._sessions() as session:
            return _to_pipeline(await _pipeline_row(session, pipeline_id))

    async def lock(self, session: AsyncSession, pipeline_id: UUID) -> Pipeline:
        """Pipeline в транзакции вызывающего; до её конца архивировать его нельзя."""
        row = await session.scalar(
            select(PipelineRow).where(PipelineRow.id == pipeline_id).with_for_update(read=True)
        )
        if row is None:
            raise PipelineNotFound(str(pipeline_id))
        return _to_pipeline(row)

    async def archive(self, pipeline_id: UUID) -> Pipeline:
        """Скрывает Pipeline из обращения; повтор ничего не меняет."""
        async with self._sessions.begin() as session:
            await session.execute(
                update(PipelineRow).where(PipelineRow.id == pipeline_id).values(archived=True)
            )
            return _to_pipeline(await _pipeline_row(session, pipeline_id))

    async def save_draft(self, pipeline_id: UUID, graph: PipelineGraph) -> None:
        async with self._sessions.begin() as session:
            row = await _active_pipeline_row(session, pipeline_id)
            row.draft = graph.model_dump(mode="json")

    async def publish(self, pipeline_id: UUID) -> PipelineVersion:
        """Фиксирует Draft как PipelineVersion со следующим `version_number`.

        Строка Pipeline заблокирована до конца транзакции: Draft не меняется
        между проверкой и записью, а номера идут без пропусков.
        """
        async with self._sessions.begin() as session:
            pipeline = await _active_pipeline_row(session, pipeline_id)
            graph = PipelineGraph.model_validate(pipeline.draft)
            validate_graph(graph, await self._specs(graph.block_refs()))
            await check_defaults(self._datasets, graph, pipeline.project_id)
            pipeline.last_version_number += 1
            row = PipelineVersionRow(
                id=uuid4(),
                pipeline_id=pipeline_id,
                version_number=pipeline.last_version_number,
                graph=pipeline.draft,
            )
            session.add(row)
            await session.flush()
            await session.refresh(row)
            return _to_version(row)

    async def get_version(self, pipeline_id: UUID, version_number: int) -> PipelineVersion:
        async with self._sessions() as session:
            row = await session.scalar(
                select(PipelineVersionRow).where(
                    PipelineVersionRow.pipeline_id == pipeline_id,
                    PipelineVersionRow.version_number == version_number,
                )
            )
        if row is None:
            raise PipelineVersionNotFound(f"{pipeline_id}:{version_number}")
        return _to_version(row)

    async def _specs(self, refs: set[BlockRef]) -> dict[BlockRef, BlockSpec]:
        specs: dict[BlockRef, BlockSpec] = {}
        for ref in refs:
            try:
                specs[ref] = await self._blocks.get_spec(ref.name, ref.version)
            except BlockNotFound:
                continue
        return specs


async def _pipeline_row(
    session: AsyncSession, pipeline_id: UUID, *, lock: bool = False
) -> PipelineRow:
    query = select(PipelineRow).where(PipelineRow.id == pipeline_id)
    row = await session.scalar(query.with_for_update() if lock else query)
    if row is None:
        raise PipelineNotFound(str(pipeline_id))
    return row


async def _active_pipeline_row(session: AsyncSession, pipeline_id: UUID) -> PipelineRow:
    row = await _pipeline_row(session, pipeline_id, lock=True)
    if row.archived:
        raise PipelineArchived(str(pipeline_id))
    return row


def _to_pipeline(row: PipelineRow) -> Pipeline:
    return Pipeline(row.id, row.project_id, row.name, row.created_at, row.archived)


def _to_version(row: PipelineVersionRow) -> PipelineVersion:
    return PipelineVersion(
        version_id=row.id,
        pipeline_id=row.pipeline_id,
        version_number=row.version_number,
        graph=PipelineGraph.model_validate(row.graph),
        published_at=row.published_at,
    )
