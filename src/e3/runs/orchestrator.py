"""Публичный фасад Run Orchestrator: PipelineRun, NodeRun и NodeOutput.

Запуск продвигается шагами, каждый шаг — короткая транзакция PostgreSQL:

- `start` создаёт PipelineRun и его NodeRun в PENDING;
- `dispatch_pending` запускает готовые узлы PENDING-запусков: BLOCK — командой
  SubmitExecution через outbox, Publish Dataset — переводом в RUNNING;
- `handle_event` применяет событие Execution Plane к Current Attempt;
- `finalize_pending` проверяет manifest NodeRun в FINALIZING и принимает NodeOutput;
- `publish_pending` выполняет Publish Dataset через Dataset Catalog.

Работа с S3 идёт вне транзакций. Незавершённый шаг виден по состоянию NodeRun,
поэтому следующий проход его повторит; повтор безопасен.
"""

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from e3 import catalog
from e3.blocks import BlockRegistry
from e3.identity import Principal, authorize_project
from e3.pipelines import (
    PUBLISH_INPUT_PORT,
    BlockRef,
    PipelineGraph,
    PipelineService,
    PipelineVersionNotFound,
)
from e3.runs.bindings import (
    InputBinding,
    OutputBinding,
    RunRequest,
    RunRequestInvalid,
    check_bindings,
)
from e3.runs.finalization import OutputRejected, execution_prefix, verify_outputs
from e3.runs.models import NodeOutputRow, NodeRunRow, PipelineRunRow
from e3.runs.nodes import NodeKind, build_node_runs, node_inputs, progress
from e3.runs.outbox import OutboxRelay, Publisher, enqueue, first_delivery
from e3.runs.progress import NodeState, RunState, plan
from e3.shared.contracts import (
    BlockSpec,
    DataRef,
    DatasetVersionRef,
    ExecutionContext,
    ExecutionResult,
    NodeOutputRef,
    ResolvedInput,
)
from e3.shared.manifest import DataFormat, DataSchema, Manifest
from e3.shared.messages import (
    ExecutionEvent,
    ExecutionFailed,
    ExecutionStarted,
    ExecutionSucceeded,
    Submission,
    SubmitExecution,
)
from e3.shared.storage import ObjectStorage, StorageUnavailable

log = logging.getLogger(__name__)

DISPATCH_BATCH = 10


class PipelineRunNotFound(Exception):
    """PipelineRun с таким id нет."""


class IdempotencyConflict(Exception):
    """`Idempotency-Key` уже использован с другим телом запроса."""


@dataclass(frozen=True)
class PipelineRun:
    pipeline_run_id: UUID
    project_id: UUID
    pipeline_id: UUID
    pipeline_version: int
    state: RunState
    inputs: dict[str, InputBinding]
    outputs: dict[str, OutputBinding]
    created_at: datetime


@dataclass(frozen=True)
class NodeOutput:
    port: str
    manifest_uri: str
    format: DataFormat
    data_schema: DataSchema


@dataclass(frozen=True)
class NodeRun:
    node_run_id: UUID
    node_id: str
    kind: NodeKind
    state: NodeState
    attempt_id: UUID | None
    outputs: list[NodeOutput]
    published_version_id: UUID | None
    error: str | None


NodeChange = Callable[[AsyncSession, NodeRunRow], None]


class RunOrchestrator:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        datasets: catalog.Catalog,
        pipelines: PipelineService,
        blocks: BlockRegistry,
        storage: ObjectStorage,
    ) -> None:
        self._sessions = sessions
        self._datasets = datasets
        self._pipelines = pipelines
        self._blocks = blocks
        self._storage = storage

    async def start(
        self, principal: Principal, request: RunRequest, *, idempotency_key: str
    ) -> PipelineRun:
        """Создаёт PipelineRun; повтор с тем же ключом и телом отдаёт уже созданный."""
        pipeline = await self._pipelines.get(request.pipeline_id)
        authorize_project(principal, pipeline.project_id)
        async with self._sessions() as session:
            existing = await _find_by_key(session, pipeline.project_id, idempotency_key)
        if existing is not None:
            return _same_request(existing, request)
        try:
            version = await self._pipelines.get_version(
                pipeline.pipeline_id, request.pipeline_version
            )
        except PipelineVersionNotFound:
            raise RunRequestInvalid(f"у Pipeline нет версии {request.pipeline_version}") from None
        await check_bindings(
            self._datasets,
            principal,
            project_id=pipeline.project_id,
            graph=version.graph,
            request=request,
        )
        specs = await self._specs(version.graph)
        return await self._create(
            pipeline.project_id, version.graph, request, idempotency_key, specs
        )

    def outbox_relay(self, publisher: Publisher) -> OutboxRelay:
        return OutboxRelay(self._sessions, publisher)

    async def get_run(self, pipeline_run_id: UUID) -> PipelineRun:
        async with self._sessions() as session:
            row = await session.get(PipelineRunRow, pipeline_run_id)
        if row is None:
            raise PipelineRunNotFound(str(pipeline_run_id))
        return _to_run(row)

    async def list_nodes(self, pipeline_run_id: UUID) -> list[NodeRun]:
        async with self._sessions() as session:
            nodes = (
                await session.scalars(
                    select(NodeRunRow)
                    .where(NodeRunRow.pipeline_run_id == pipeline_run_id)
                    .order_by(NodeRunRow.node_id)
                )
            ).all()
            outputs = (
                await session.scalars(
                    select(NodeOutputRow)
                    .where(NodeOutputRow.node_run_id.in_([node.id for node in nodes]))
                    .order_by(NodeOutputRow.port)
                )
            ).all()
        return [
            _to_node_run(node, [o for o in outputs if o.node_run_id == node.id]) for node in nodes
        ]

    async def dispatch_pending(self) -> int:
        """Переводит PENDING-запуски в RUNNING и запускает их готовые узлы."""
        async with self._sessions.begin() as session:
            runs = (
                await session.scalars(
                    select(PipelineRunRow)
                    .where(PipelineRunRow.state == RunState.PENDING)
                    .order_by(PipelineRunRow.created_at)
                    .limit(DISPATCH_BATCH)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for run in runs:
                await self._advance(session, run)
            return len(runs)

    async def handle_event(self, event: ExecutionEvent) -> None:
        """Применяет событие один раз и только к Current Attempt и допустимому переходу."""
        async with self._sessions.begin() as session:
            if not await first_delivery(session, event.event_id):
                return
            locked = await _lock_node(session, event.node_run_id)
            if locked is None:
                return
            run, node = locked
            if node.current_attempt_id != event.attempt_id:
                return
            await self._apply(session, run, node, event)

    async def finalize_pending(self) -> int:
        ids = await self._node_ids(NodeKind.BLOCK, NodeState.FINALIZING)
        for node_run_id in ids:
            await self._retrying(self._finalize, node_run_id)
        return len(ids)

    async def publish_pending(self) -> int:
        ids = await self._node_ids(NodeKind.SYSTEM, NodeState.RUNNING)
        for node_run_id in ids:
            await self._retrying(self._publish, node_run_id)
        return len(ids)

    async def _create(
        self,
        project_id: UUID,
        graph: PipelineGraph,
        request: RunRequest,
        idempotency_key: str,
        specs: Mapping[BlockRef, BlockSpec],
    ) -> PipelineRun:
        run_id = uuid4()
        async with self._sessions.begin() as session:
            inserted = await session.scalar(
                insert(PipelineRunRow)
                .values(
                    id=run_id,
                    project_id=project_id,
                    pipeline_id=request.pipeline_id,
                    pipeline_version=request.pipeline_version,
                    state=RunState.PENDING,
                    inputs=_dump(request.inputs),
                    outputs=_dump(request.outputs),
                    idempotency_key=idempotency_key,
                    request_hash=request.fingerprint(),
                )
                .on_conflict_do_nothing(index_elements=["project_id", "idempotency_key"])
                .returning(PipelineRunRow.id)
            )
            if inserted is None:
                existing = await _find_by_key(session, project_id, idempotency_key)
                assert existing is not None
                return _same_request(existing, request)
            session.add_all(build_node_runs(run_id, graph, request, specs))
            row = await session.get_one(PipelineRunRow, run_id)
            return _to_run(row)

    async def _specs(self, graph: PipelineGraph) -> dict[BlockRef, BlockSpec]:
        return {
            ref: await self._blocks.get_spec(ref.name, ref.version) for ref in graph.block_refs()
        }

    async def _advance(self, session: AsyncSession, run: PipelineRunRow) -> None:
        nodes = (
            await session.scalars(
                select(NodeRunRow).where(NodeRunRow.pipeline_run_id == run.id).with_for_update()
            )
        ).all()
        decision = plan([progress(node) for node in nodes])
        by_id = {node.id: node for node in nodes}
        for node_run_id in decision.skip:
            by_id[node_run_id].state = NodeState.SKIPPED
        for node_run_id in decision.start:
            await self._start_node(session, run, by_id[node_run_id])
        run.state = decision.run_state

    async def _start_node(
        self, session: AsyncSession, run: PipelineRunRow, node: NodeRunRow
    ) -> None:
        if node.kind == NodeKind.SYSTEM:
            node.state = NodeState.RUNNING
            return
        assert node.executable_ref is not None
        attempt_id = uuid4()
        context = ExecutionContext(
            node_run_id=node.id,
            attempt_id=attempt_id,
            inputs=await self._resolve_inputs(session, node),
            parameters=node.parameters,
            output_prefix=self._storage.uri(execution_prefix(attempt_id)),
        )
        enqueue(
            session,
            SubmitExecution(
                node_run_id=node.id,
                attempt_id=attempt_id,
                trace_id=str(run.id),
                payload=Submission(executable_ref=node.executable_ref, context=context),
            ),
        )
        node.current_attempt_id = attempt_id
        node.state = NodeState.QUEUED

    async def _resolve_inputs(
        self, session: AsyncSession, node: NodeRunRow
    ) -> dict[str, ResolvedInput]:
        """DataRef → ResolvedInput с `manifest_uri`: Execution Plane в каталог не ходит."""
        return {port: await self._resolve(session, ref) for port, ref in node_inputs(node).items()}

    async def _resolve(self, session: AsyncSession, ref: DataRef) -> ResolvedInput:
        if isinstance(ref, DatasetVersionRef):
            version = await self._datasets.resolve(ref.version_id)
            return ResolvedInput(
                manifest_uri=version.manifest_uri,
                format=version.format,
                data_schema=version.data_schema,
                source=ref,
            )
        output = await session.get_one(NodeOutputRow, (ref.node_run_id, ref.port))
        return ResolvedInput(
            manifest_uri=output.manifest_uri,
            format=_format(output.format),
            data_schema=DataSchema.model_validate(output.data_schema),
            source=ref,
        )

    async def _apply(
        self, session: AsyncSession, run: PipelineRunRow, node: NodeRunRow, event: ExecutionEvent
    ) -> None:
        match event:
            case ExecutionStarted() if node.state == NodeState.QUEUED:
                node.state = NodeState.RUNNING
            case ExecutionSucceeded() if node.state == NodeState.RUNNING:
                node.state = NodeState.FINALIZING
                node.result = event.payload.model_dump(mode="json")
            case ExecutionFailed() if node.state in {NodeState.QUEUED, NodeState.RUNNING}:
                node.state = NodeState.FAILED
                node.error = event.payload.error
                await self._advance(session, run)
            case _:
                log.info("событие %s для NodeRun в %s пропущено", event.type, node.state)

    async def _finalize(self, node_run_id: UUID) -> None:
        async with self._sessions() as session:
            node = await session.get_one(NodeRunRow, node_run_id)
        result = ExecutionResult.model_validate(node.result)
        try:
            manifests = await verify_outputs(self._storage, result, node.output_ports)
        except OutputRejected as rejection:
            await self._transition(node_run_id, NodeState.FINALIZING, _failed(str(rejection)))
            return
        await self._transition(node_run_id, NodeState.FINALIZING, _accepted(result, manifests))

    async def _publish(self, node_run_id: UUID) -> None:
        async with self._sessions() as session:
            node = await session.get_one(NodeRunRow, node_run_id)
            run = await session.get_one(PipelineRunRow, node.pipeline_run_id)
            source = node_inputs(node)[PUBLISH_INPUT_PORT]
            output = None
            if isinstance(source, NodeOutputRef):
                output = await session.get(NodeOutputRow, (source.node_run_id, source.port))
        if output is None or not isinstance(source, NodeOutputRef):
            await self._transition(node_run_id, NodeState.RUNNING, _failed("нет NodeOutput"))
            return
        assert node.target_dataset_id is not None
        try:
            version = await self._datasets.publish(
                project_id=run.project_id,
                dataset_id=node.target_dataset_id,
                source=source,
                manifest_uri=output.manifest_uri,
            )
        except (catalog.PublicationRejected, catalog.DatasetNotFound) as error:
            await self._transition(node_run_id, NodeState.RUNNING, _failed(str(error)))
            return
        await self._transition(node_run_id, NodeState.RUNNING, _published(version.version_id))

    async def _transition(self, node_run_id: UUID, expected: NodeState, change: NodeChange) -> None:
        """Меняет NodeRun, если он всё ещё в `expected`, и продвигает запуск."""
        async with self._sessions.begin() as session:
            locked = await _lock_node(session, node_run_id)
            if locked is None:
                return
            run, node = locked
            if node.state != expected:
                return
            change(session, node)
            await self._advance(session, run)

    async def _node_ids(self, kind: NodeKind, state: NodeState) -> list[UUID]:
        async with self._sessions() as session:
            ids = await session.scalars(
                select(NodeRunRow.id).where(NodeRunRow.kind == kind, NodeRunRow.state == state)
            )
            return list(ids)

    async def _retrying(self, step: Callable[[UUID], Any], node_run_id: UUID) -> None:
        """Недоступное хранилище не роняет проход: узел повторится в следующем."""
        try:
            await step(node_run_id)
        except StorageUnavailable:
            log.warning("хранилище недоступно, NodeRun %s повторится", node_run_id, exc_info=True)


def _failed(error: str) -> NodeChange:
    def change(_session: AsyncSession, node: NodeRunRow) -> None:
        node.state = NodeState.FAILED
        node.error = error

    return change


def _accepted(result: ExecutionResult, manifests: Mapping[str, Manifest]) -> NodeChange:
    def change(session: AsyncSession, node: NodeRunRow) -> None:
        for port, manifest in manifests.items():
            session.add(
                NodeOutputRow(
                    node_run_id=node.id,
                    port=port,
                    manifest_uri=result.outputs[port],
                    format=manifest.format,
                    data_schema=manifest.data_schema.model_dump(),
                )
            )
        node.state = NodeState.SUCCEEDED

    return change


def _published(version_id: UUID) -> NodeChange:
    def change(_session: AsyncSession, node: NodeRunRow) -> None:
        node.state = NodeState.SUCCEEDED
        node.published_version_id = version_id

    return change


async def _lock_node(
    session: AsyncSession, node_run_id: UUID
) -> tuple[PipelineRunRow, NodeRunRow] | None:
    """Блокирует запуск, затем узел: везде один порядок, чтобы не было взаимоблокировок."""
    run_id = await session.scalar(
        select(NodeRunRow.pipeline_run_id).where(NodeRunRow.id == node_run_id)
    )
    if run_id is None:
        return None
    run = await session.scalar(
        select(PipelineRunRow).where(PipelineRunRow.id == run_id).with_for_update()
    )
    node = await session.scalar(
        select(NodeRunRow).where(NodeRunRow.id == node_run_id).with_for_update()
    )
    assert run is not None and node is not None
    return run, node


async def _find_by_key(
    session: AsyncSession, project_id: UUID, idempotency_key: str
) -> PipelineRunRow | None:
    return await session.scalar(
        select(PipelineRunRow).where(
            PipelineRunRow.project_id == project_id,
            PipelineRunRow.idempotency_key == idempotency_key,
        )
    )


def _same_request(row: PipelineRunRow, request: RunRequest) -> PipelineRun:
    if row.request_hash != request.fingerprint():
        raise IdempotencyConflict(row.idempotency_key)
    return _to_run(row)


def _dump(bindings: Mapping[str, InputBinding] | Mapping[str, OutputBinding]) -> dict[str, Any]:
    return {slot: binding.model_dump(mode="json") for slot, binding in bindings.items()}


def _format(value: str) -> DataFormat:
    return cast(DataFormat, value)


def _to_run(row: PipelineRunRow) -> PipelineRun:
    return PipelineRun(
        pipeline_run_id=row.id,
        project_id=row.project_id,
        pipeline_id=row.pipeline_id,
        pipeline_version=row.pipeline_version,
        state=RunState(row.state),
        inputs={slot: InputBinding.model_validate(b) for slot, b in row.inputs.items()},
        outputs={slot: OutputBinding.model_validate(b) for slot, b in row.outputs.items()},
        created_at=row.created_at,
    )


def _to_node_run(row: NodeRunRow, outputs: list[NodeOutputRow]) -> NodeRun:
    return NodeRun(
        node_run_id=row.id,
        node_id=row.node_id,
        kind=NodeKind(row.kind),
        state=NodeState(row.state),
        attempt_id=row.current_attempt_id,
        outputs=[
            NodeOutput(
                port=output.port,
                manifest_uri=output.manifest_uri,
                format=_format(output.format),
                data_schema=DataSchema.model_validate(output.data_schema),
            )
            for output in outputs
        ],
        published_version_id=row.published_version_id,
        error=row.error,
    )
