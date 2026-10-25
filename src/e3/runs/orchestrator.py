"""Публичный фасад Run Orchestrator: PipelineRun, NodeRun и NodeOutput.

Запуск продвигается шагами, каждый шаг — короткая транзакция PostgreSQL:

- `start` создаёт PipelineRun и его NodeRun в PENDING;
- `dispatch_pending` запускает готовые узлы PENDING-запусков: BLOCK — командой
  SubmitExecution через outbox, Publish Dataset — переводом в RUNNING;
- `cancel` запрашивает отмену: работающие попытки получают CancelExecution, итог
  CANCELLED фиксируется только по доказанной остановке (так же гасит узлы Fail-fast);
- `handle_event` применяет событие Execution Plane к Current Attempt; провал
  с ретраебельной причиной сразу назначает следующую попытку (ADR-0003);
- `finalize_pending` проверяет manifest NodeRun в FINALIZING и принимает NodeOutput;
- `publish_pending` выполняет Publish Dataset через Dataset Catalog;
- `reconcile_pending` просит Execution Controller проверить попытки, о которых
  давно нет событий. Молчание — не смерть: `LOST` ставит только доказательство
  от Controller.

Работа с S3 идёт вне транзакций. Незавершённый шаг виден по состоянию NodeRun,
поэтому следующий проход его повторит; повтор безопасен.
"""

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import func, select
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
from e3.runs.attempts import LIVE, AttemptReason, AttemptState, should_retry
from e3.runs.bindings import (
    InputBinding,
    OutputBinding,
    RunRequest,
    RunRequestInvalid,
    check_bindings,
)
from e3.runs.finalization import OutputRejected, execution_prefix, verify_outputs
from e3.runs.models import ExecutionAttemptRow, NodeOutputRow, NodeRunRow, PipelineRunRow
from e3.runs.nodes import NodeKind, build_node_runs, node_inputs, progress
from e3.runs.outbox import OutboxRelay, Publisher, enqueue, first_delivery, unsent
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
    ExecutionCancelled,
    ExecutionEvent,
    ExecutionFailed,
    ExecutionLost,
    ExecutionStarted,
    ExecutionSucceeded,
    Submission,
    SubmitExecution,
    cancel,
    cancel_id,
    reconcile,
    reconcile_id,
)
from e3.shared.storage import ObjectStorage, StorageUnavailable

log = logging.getLogger(__name__)

DISPATCH_BATCH = 10
# NodeRun, ждущий итога Current Attempt от Execution Plane.
AWAITING_OUTCOME = frozenset({NodeState.QUEUED, NodeState.RUNNING})
# NodeRun, у которого Current Attempt ещё может прислать итог.
OPEN_ATTEMPT = AWAITING_OUTCOME | {NodeState.CANCELLING}
# Запуск закончился: отменять нечего.
FINISHED = frozenset({RunState.SUCCEEDED, RunState.FAILED})


class PipelineRunNotFound(Exception):
    """PipelineRun с таким id нет."""


class IdempotencyConflict(Exception):
    """`Idempotency-Key` уже использован с другим телом запроса."""


class RunNotCancellable(Exception):
    """PipelineRun уже завершился SUCCEEDED или FAILED."""


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
    failed_node_run_id: UUID | None


@dataclass(frozen=True)
class NodeOutput:
    port: str
    manifest_uri: str
    format: DataFormat
    data_schema: DataSchema


@dataclass(frozen=True)
class Attempt:
    attempt_id: UUID
    number: int
    state: AttemptState
    reason: AttemptReason | None
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


@dataclass(frozen=True)
class NodeRun:
    node_run_id: UUID
    node_id: str
    kind: NodeKind
    state: NodeState
    attempt_id: UUID | None
    attempts: list[Attempt]
    outputs: list[NodeOutput]
    published_version_id: UUID | None
    error: str | None


NodeChange = Callable[[AsyncSession, NodeRunRow], Awaitable[None]]


class RunOrchestrator:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        datasets: catalog.Catalog,
        pipelines: PipelineService,
        blocks: BlockRegistry,
        storage: ObjectStorage,
        reconcile_after: timedelta,
    ) -> None:
        self._sessions = sessions
        self._datasets = datasets
        self._pipelines = pipelines
        self._blocks = blocks
        self._storage = storage
        self._reconcile_after = reconcile_after

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
            ids = [node.id for node in nodes]
            outputs = (
                await session.scalars(
                    select(NodeOutputRow)
                    .where(NodeOutputRow.node_run_id.in_(ids))
                    .order_by(NodeOutputRow.port)
                )
            ).all()
            attempts = (
                await session.scalars(
                    select(ExecutionAttemptRow)
                    .where(ExecutionAttemptRow.node_run_id.in_(ids))
                    .order_by(ExecutionAttemptRow.number)
                )
            ).all()
        return [
            _to_node_run(
                node,
                [a for a in attempts if a.node_run_id == node.id],
                [o for o in outputs if o.node_run_id == node.id],
            )
            for node in nodes
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

    async def cancel(self, pipeline_run_id: UUID) -> PipelineRun:
        """Запрашивает отмену; повтор и отмена во время Fail-fast ничего не меняют."""
        async with self._sessions.begin() as session:
            run = await session.get(PipelineRunRow, pipeline_run_id, with_for_update=True)
            if run is None:
                raise PipelineRunNotFound(pipeline_run_id)
            if run.state in FINISHED:
                raise RunNotCancellable(pipeline_run_id)
            if run.state not in {RunState.PENDING, RunState.RUNNING} or run.failed_node_run_id:
                return _to_run(run)
            run.state = RunState.CANCELLING
            await self._advance(session, run)
            return _to_run(run)

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
            attempt = await session.get_one(
                ExecutionAttemptRow, event.attempt_id, with_for_update=True
            )
            await self._apply(session, run, node, attempt, event)

    async def finalize_pending(self) -> int:
        """Возвращает число NodeRun, по которым проход продвинулся."""
        ids = await self._node_ids(NodeKind.BLOCK, NodeState.FINALIZING)
        return sum([await _isolated(self._finalize, node_run_id) for node_run_id in ids])

    async def publish_pending(self) -> int:
        """Возвращает число NodeRun, по которым проход продвинулся."""
        ids = await self._node_ids(NodeKind.SYSTEM, NodeState.RUNNING)
        return sum([await _isolated(self._publish, node_run_id) for node_run_id in ids])

    async def reconcile_pending(self) -> int:
        """Ставит в outbox ReconcileExecution молчащим попыткам; возвращает число поставленных.

        Попытка молчит, если с её последней смены состояния или проверки прошло
        больше `reconcile_after`. Следующая проверка ставится, только когда
        предыдущая уже ушла в Kafka: неотправленные не копятся. Ответа «жива» нет,
        поэтому, пока попытка не терминальна, проверки повторяются по таймауту.
        """
        async with self._sessions.begin() as session:
            quiet = await self._quiet_attempts(session)
            sent = [_last_check_id(a, s) for a, s in quiet if a.reconcile_count]
            waiting = await unsent(session, sent)
            due = [(a, s) for a, s in quiet if _last_check_id(a, s) not in waiting]
            for attempt, node_state in due:
                _request_check(session, attempt, node_state)
            return len(due)

    async def _quiet_attempts(
        self, session: AsyncSession
    ) -> list[tuple[ExecutionAttemptRow, NodeState]]:
        last_news = func.greatest(
            ExecutionAttemptRow.created_at,
            ExecutionAttemptRow.started_at,
            ExecutionAttemptRow.reconcile_requested_at,
        )
        rows = await session.execute(
            select(ExecutionAttemptRow, NodeRunRow.state)
            .join(NodeRunRow, NodeRunRow.current_attempt_id == ExecutionAttemptRow.attempt_id)
            .where(
                ExecutionAttemptRow.state.in_(LIVE),
                NodeRunRow.state.in_(OPEN_ATTEMPT),
                last_news < func.now() - self._reconcile_after,
            )
            .with_for_update(of=ExecutionAttemptRow, skip_locked=True)
        )
        return [(attempt, NodeState(state)) for attempt, state in rows]

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
                if existing is None:
                    raise RuntimeError(f"PipelineRun с ключом {idempotency_key} исчез")
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
        cancelling = run.state in {RunState.CANCELLING, RunState.CANCELLED}
        decision = plan([progress(node) for node in nodes], cancelling=cancelling)
        by_id = {node.id: node for node in nodes}
        for node_run_id in decision.skip:
            by_id[node_run_id].state = NodeState.SKIPPED
        for node_run_id in decision.cancel:
            await _cancel_node(session, by_id[node_run_id])
        for node_run_id in decision.start:
            await self._start_node(session, run, by_id[node_run_id])
        run.state = decision.run_state
        if run.failed_node_run_id is None:
            run.failed_node_run_id = _first_failed(nodes)

    async def _start_node(
        self, session: AsyncSession, run: PipelineRunRow, node: NodeRunRow
    ) -> None:
        if node.kind == NodeKind.SYSTEM:
            node.state = NodeState.RUNNING
            return
        await self._submit_attempt(session, run, node, number=1)

    async def _submit_attempt(
        self, session: AsyncSession, run: PipelineRunRow, node: NodeRunRow, *, number: int
    ) -> None:
        """Попытка, её номер, Current Attempt и SubmitExecution в outbox — одной транзакцией."""
        if node.executable_ref is None:
            raise RuntimeError(f"у BLOCK NodeRun {node.id} нет executable_ref")
        attempt_id = uuid4()
        context = ExecutionContext(
            node_run_id=node.id,
            attempt_id=attempt_id,
            inputs=await self._resolve_inputs(session, node),
            parameters=node.parameters,
            output_prefix=self._storage.uri(execution_prefix(attempt_id)),
        )
        command = SubmitExecution(
            node_run_id=node.id,
            attempt_id=attempt_id,
            trace_id=str(run.id),
            payload=Submission(executable_ref=node.executable_ref, context=context),
        )
        session.add(
            ExecutionAttemptRow(
                attempt_id=attempt_id,
                node_run_id=node.id,
                number=number,
                state=AttemptState.SUBMITTED,
                command=command.model_dump(mode="json"),
            )
        )
        enqueue(session, command)
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
        output = _to_node_output(await session.get_one(NodeOutputRow, (ref.node_run_id, ref.port)))
        return ResolvedInput(
            manifest_uri=output.manifest_uri,
            format=output.format,
            data_schema=output.data_schema,
            source=ref,
        )

    async def _apply(
        self,
        session: AsyncSession,
        run: PipelineRunRow,
        node: NodeRunRow,
        attempt: ExecutionAttemptRow,
        event: ExecutionEvent,
    ) -> None:
        """Первый принятый итог попытки от Controller окончателен: следующие не применяются.

        `Succeeded` попытку не завершает: итог ставит проверка manifest в FINALIZING.
        """
        if node.state not in OPEN_ATTEMPT or attempt.state not in LIVE:
            log.info(
                "событие %s пропущено: NodeRun в %s, попытка в %s",
                event.type,
                node.state,
                attempt.state,
            )
            return
        match event:
            case ExecutionStarted():
                _mark_running(node, attempt)
            case ExecutionSucceeded():
                _mark_running(node, attempt)
                node.state = NodeState.FINALIZING
                node.result = event.payload.model_dump(mode="json")
            case ExecutionFailed():
                reason = AttemptReason(event.payload.reason)
                await self._end_attempt(session, run, node, attempt, reason, event.payload.error)
            case ExecutionLost() | ExecutionCancelled() if node.state == NodeState.CANCELLING:
                _mark_cancelled(node, attempt)
                await self._advance(session, run)
            case ExecutionLost():
                error = "попытка потеряна: SparkApplication пропал, pod'ов не осталось"
                await self._end_attempt(session, run, node, attempt, AttemptReason.LOST, error)
            case ExecutionCancelled():
                log.warning("CancelExecution попытке %s не посылали", attempt.attempt_id)

    async def _end_attempt(
        self,
        session: AsyncSession,
        run: PipelineRunRow,
        node: NodeRunRow,
        attempt: ExecutionAttemptRow,
        reason: AttemptReason,
        error: str,
    ) -> None:
        """Провал доказуемо мёртвой попытки: следующая попытка или FAILED узла.

        Отменяемый узел не ретраится: запуск уже останавливается.
        """
        _fail(attempt, reason, error)
        node.error = error
        if node.state != NodeState.CANCELLING and should_retry(attempt.number, reason):
            await self._submit_attempt(session, run, node, number=attempt.number + 1)
            return
        node.state = NodeState.FAILED
        await self._advance(session, run)

    async def _finalize(self, node_run_id: UUID) -> None:
        async with self._sessions() as session:
            node = await session.get_one(NodeRunRow, node_run_id)
        result = ExecutionResult.model_validate(node.result)
        try:
            manifests = await verify_outputs(self._storage, result, node.output_ports)
        except OutputRejected as rejection:
            await self._transition(node_run_id, NodeState.FINALIZING, _rejected(str(rejection)))
            return
        await self._transition(node_run_id, NodeState.FINALIZING, _accepted(result, manifests))

    async def _publish(self, node_run_id: UUID) -> None:
        publication = await self._publication(node_run_id)
        if publication is None:
            await self._transition(node_run_id, NodeState.RUNNING, _failed("нет NodeOutput"))
            return
        try:
            version = await self._datasets.publish(
                project_id=publication.project_id,
                dataset_id=publication.dataset_id,
                source=publication.source,
                manifest_uri=publication.manifest_uri,
            )
        except (catalog.PublicationRejected, catalog.DatasetNotFound) as error:
            await self._transition(node_run_id, NodeState.RUNNING, _failed(str(error)))
            return
        await self._transition(node_run_id, NodeState.RUNNING, _published(version.version_id))

    async def _publication(self, node_run_id: UUID) -> "_Publication | None":
        """Что публикует узел Publish Dataset; `None` — входа NodeOutput нет."""
        async with self._sessions() as session:
            node = await session.get_one(NodeRunRow, node_run_id)
            source = node_inputs(node)[PUBLISH_INPUT_PORT]
            if not isinstance(source, NodeOutputRef):
                return None
            output = await session.get(NodeOutputRow, (source.node_run_id, source.port))
            if output is None:
                return None
            run = await session.get_one(PipelineRunRow, node.pipeline_run_id)
        if node.target_dataset_id is None:
            raise RuntimeError(f"у Publish Dataset NodeRun {node.id} нет целевого Dataset")
        return _Publication(run.project_id, node.target_dataset_id, source, output.manifest_uri)

    async def _transition(self, node_run_id: UUID, expected: NodeState, change: NodeChange) -> None:
        """Меняет NodeRun, если он всё ещё в `expected`, и продвигает запуск."""
        async with self._sessions.begin() as session:
            locked = await _lock_node(session, node_run_id)
            if locked is None:
                return
            run, node = locked
            if node.state != expected:
                return
            await change(session, node)
            await self._advance(session, run)

    async def _node_ids(self, kind: NodeKind, state: NodeState) -> list[UUID]:
        async with self._sessions() as session:
            ids = await session.scalars(
                select(NodeRunRow.id).where(NodeRunRow.kind == kind, NodeRunRow.state == state)
            )
            return list(ids)


@dataclass(frozen=True)
class _Publication:
    project_id: UUID
    dataset_id: UUID
    source: NodeOutputRef
    manifest_uri: str


async def _isolated(advance: Callable[[UUID], Awaitable[None]], node_run_id: UUID) -> bool:
    """Сбой одного NodeRun не роняет проход: узел повторится в следующем.

    Возвращает, продвинулся ли узел; сбой — не продвижение, чтобы цикл взял паузу.
    """
    try:
        await advance(node_run_id)
    except StorageUnavailable:
        log.warning("хранилище недоступно, NodeRun %s повторится", node_run_id, exc_info=True)
        return False
    except Exception:
        log.exception("NodeRun %s не продвинулся, повторится", node_run_id)
        return False
    return True


def _cancelling(node_state: NodeState) -> bool:
    """Отменяемому узлу по таймауту повторяется CancelExecution, а не проверка."""
    return node_state == NodeState.CANCELLING


def _last_check_id(attempt: ExecutionAttemptRow, node_state: NodeState) -> UUID:
    check_id = cancel_id if _cancelling(node_state) else reconcile_id
    return check_id(attempt.attempt_id, attempt.reconcile_count)


def _request_check(
    session: AsyncSession, attempt: ExecutionAttemptRow, node_state: NodeState
) -> None:
    """ReconcileExecution или CancelExecution с общим счётчиком: повтор по тому же таймауту."""
    attempt.reconcile_count += 1
    attempt.reconcile_requested_at = func.now()
    command = SubmitExecution.model_validate(attempt.command)
    check = cancel if _cancelling(node_state) else reconcile
    enqueue(session, check(command, attempt.reconcile_count))


async def _cancel_node(session: AsyncSession, node: NodeRunRow) -> None:
    node.state = NodeState.CANCELLING
    _request_check(session, await _current_attempt(session, node), NodeState.CANCELLING)


def _first_failed(nodes: Sequence[NodeRunRow]) -> UUID | None:
    return next((node.id for node in nodes if node.state == NodeState.FAILED), None)


def _mark_cancelled(node: NodeRunRow, attempt: ExecutionAttemptRow) -> None:
    attempt.state = AttemptState.CANCELLED
    attempt.finished_at = func.now()
    node.state = NodeState.CANCELLED


def _mark_running(node: NodeRunRow, attempt: ExecutionAttemptRow) -> None:
    if attempt.state == AttemptState.SUBMITTED:
        attempt.state = AttemptState.RUNNING
        attempt.started_at = func.now()
    if node.state != NodeState.CANCELLING:
        node.state = NodeState.RUNNING


def _succeed(attempt: ExecutionAttemptRow) -> None:
    attempt.state = AttemptState.SUCCEEDED
    attempt.finished_at = func.now()


def _fail(attempt: ExecutionAttemptRow, reason: AttemptReason, error: str) -> None:
    attempt.state = AttemptState.LOST if reason == AttemptReason.LOST else AttemptState.FAILED
    attempt.reason = reason
    attempt.error = error
    attempt.finished_at = func.now()


async def _current_attempt(session: AsyncSession, node: NodeRunRow) -> ExecutionAttemptRow:
    return await session.get_one(ExecutionAttemptRow, node.current_attempt_id, with_for_update=True)


def _failed(error: str) -> NodeChange:
    async def change(_session: AsyncSession, node: NodeRunRow) -> None:
        node.state = NodeState.FAILED
        node.error = error

    return change


def _rejected(error: str) -> NodeChange:
    """Manifest отвергнут: попытка провалена без ретрая, причину ставит сам Orchestrator."""

    async def change(session: AsyncSession, node: NodeRunRow) -> None:
        _fail(await _current_attempt(session, node), AttemptReason.OUTPUT_REJECTED, error)
        node.state = NodeState.FAILED
        node.error = error

    return change


def _accepted(result: ExecutionResult, manifests: Mapping[str, Manifest]) -> NodeChange:
    async def change(session: AsyncSession, node: NodeRunRow) -> None:
        _succeed(await _current_attempt(session, node))
        node.error = None
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
    async def change(_session: AsyncSession, node: NodeRunRow) -> None:
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
    if run is None or node is None:
        return None
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
        failed_node_run_id=row.failed_node_run_id,
    )


def _to_node_run(
    row: NodeRunRow, attempts: list[ExecutionAttemptRow], outputs: list[NodeOutputRow]
) -> NodeRun:
    return NodeRun(
        node_run_id=row.id,
        node_id=row.node_id,
        kind=NodeKind(row.kind),
        state=NodeState(row.state),
        attempt_id=row.current_attempt_id,
        attempts=[_to_attempt(attempt) for attempt in attempts],
        outputs=[_to_node_output(output) for output in outputs],
        published_version_id=row.published_version_id,
        error=row.error,
    )


def _to_attempt(row: ExecutionAttemptRow) -> Attempt:
    return Attempt(
        attempt_id=row.attempt_id,
        number=row.number,
        state=AttemptState(row.state),
        reason=AttemptReason(row.reason) if row.reason else None,
        error=row.error,
        created_at=row.created_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
    )


def _to_node_output(row: NodeOutputRow) -> NodeOutput:
    return NodeOutput(
        port=row.port,
        manifest_uri=row.manifest_uri,
        format=_format(row.format),
        data_schema=DataSchema.model_validate(row.data_schema),
    )
