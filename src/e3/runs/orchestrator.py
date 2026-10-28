"""Публичный фасад Run Orchestrator: PipelineRun, NodeRun и NodeOutput.

Запуск продвигается шагами, каждый шаг — короткая транзакция PostgreSQL:

- `start` создаёт PipelineRun и его NodeRun в PENDING;
- `dispatch_pending` по каждому Project под его замком переводит PENDING-запуски
  в RUNNING (не больше `ACTIVE_RUNS_LIMIT` активных, остальные ждут по FIFO) и даёт
  готовым BLOCK-узлам RUNNING-запусков место в Project Quota: попытка уходит
  командой SubmitExecution через outbox, а без места узел ждёт в PENDING, тоже
  по FIFO запусков (ADR-0005). Publish Dataset квоту не занимает и стартует сразу;
- `cancel` запрашивает отмену: работающие попытки получают CancelExecution, итог
  CANCELLED фиксируется только по доказанной остановке (так же гасит узлы Fail-fast);
- `handle_event` применяет событие Execution Plane к Current Attempt; после провала
  с ретраебельной причиной узел снова ждёт места в PENDING (ADR-0003). Итог попытки
  и принятый результат сразу запускают тот же выбор по Project, что и `dispatch_pending`;
- `finalize_pending` проверяет manifest NodeRun в FINALIZING и принимает NodeOutput;
- `publish_pending` выполняет Publish Dataset через Dataset Catalog;
- `reconcile_pending` просит Execution Controller проверить попытки, о которых
  давно нет событий. Молчание — не смерть: `LOST` ставит только доказательство
  от Controller;
- `purge_staging` удаляет staging попыток запусков, завершённых раньше срока хранения.

Работа с S3 идёт вне транзакций. Незавершённый шаг виден по состоянию NodeRun,
поэтому следующий проход его повторит; повтор безопасен.
"""

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import Select, SQLColumnExpression, Uuid, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from e3 import audit, catalog
from e3.blocks import BlockRegistry
from e3.identity import IdentityService, Principal, Role, authorize
from e3.pipelines import (
    PUBLISH_INPUT_PORT,
    BlockRef,
    PipelineArchived,
    PipelineGraph,
    PipelineService,
    PipelineVersionNotFound,
)
from e3.runs.attempts import LIVE, AttemptReason, AttemptState, should_retry
from e3.runs.bindings import (
    Bindings,
    InputBinding,
    OutputBinding,
    RunRequest,
    RunRequestInvalid,
    check_not_archived,
    resolve_bindings,
)
from e3.runs.finalization import OutputRejected, execution_prefix, verify_outputs
from e3.runs.models import ExecutionAttemptRow, NodeOutputRow, NodeRunRow, PipelineRunRow
from e3.runs.nodes import NodeKind, build_node_runs, node_inputs, progress
from e3.runs.outbox import OutboxRelay, Publisher, enqueue, first_delivery, unsent
from e3.runs.progress import NodeState, RunState, plan
from e3.runs.quota import ProjectQuota
from e3.runs.retention import StagingRetention
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
    PodSize,
    Submission,
    SubmitExecution,
    cancel,
    cancel_id,
    reconcile,
    reconcile_id,
)
from e3.shared.storage import ObjectStorage, StorageUnavailable

log = logging.getLogger(__name__)

# Project за проход `dispatch_pending`; каждый стартует до `ACTIVE_RUNS_LIMIT` запусков.
DISPATCH_BATCH = 10
# Активный PipelineRun держит узлы и ресурсы; у Project их не больше лимита (PRD §5).
ACTIVE_RUNS = frozenset({RunState.RUNNING, RunState.CANCELLING})
ACTIVE_RUNS_LIMIT = 5
# NodeRun, ждущий итога Current Attempt от Execution Plane.
AWAITING_OUTCOME = frozenset({NodeState.QUEUED, NodeState.RUNNING})
# NodeRun, у которого Current Attempt ещё может прислать итог.
OPEN_ATTEMPT = AWAITING_OUTCOME | {NodeState.CANCELLING}
# Запуск закончился: отменять нечего.
FINISHED = frozenset({RunState.SUCCEEDED, RunState.FAILED})
# Итог запуска: дальше состояние не меняется.
TERMINAL_RUNS = FINISHED | {RunState.CANCELLED}
# Терминальная причина Publish Dataset без ретрая: дальше Fail-fast.
ACCESS_REVOKED = "access_revoked: у Initiator нет Role Editor в Project целевого Dataset"


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
    finished_at: datetime | None
    initiated_by_user_id: UUID
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
        identity: IdentityService,
        reconcile_after: timedelta,
        pod: PodSize,
        quota: ProjectQuota,
        staging_retention: timedelta,
    ) -> None:
        self._sessions = sessions
        self._datasets = datasets
        self._pipelines = pipelines
        self._blocks = blocks
        self._storage = storage
        self._identity = identity
        self._reconcile_after = reconcile_after
        self._pod = pod
        self._quota = quota
        self._staging = StagingRetention(sessions, storage, retention=staging_retention)

    async def start(
        self, principal: Principal, request: RunRequest, *, idempotency_key: str
    ) -> PipelineRun:
        """Создаёт PipelineRun; повтор с тем же ключом и телом отдаёт уже созданный."""
        pipeline = await self._pipelines.get(request.pipeline_id)
        authorize(principal, pipeline.project_id, Role.EDITOR)
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
        bindings = await resolve_bindings(
            self._datasets,
            principal,
            project_id=pipeline.project_id,
            graph=version.graph,
            request=request,
        )
        specs = await self._specs(version.graph)
        return await self._create(
            pipeline.project_id,
            principal.user_id,
            version.graph,
            request,
            bindings,
            idempotency_key,
            specs,
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
        """Стартует ждущие запуски и попытки; возвращает их общее число."""
        started = 0
        for project_id in await self._projects_to_dispatch():
            started += await self._dispatch_isolated(project_id)
        return started

    async def _projects_to_dispatch(self) -> list[UUID]:
        """Project, где есть что стартовать: ждущий запуск или готовый узел, и есть место.

        Заполненный Project сюда не попадает и не забивает пачку остальным. Это
        подсказка без замка: лимит и квоту решает пересчёт в `_dispatch_project`.
        """
        async with self._sessions.begin() as session:
            runs = await session.scalars(_projects_with_waiting_runs())
            nodes = await session.scalars(_projects_with_waiting_nodes(self._quota.capacity))
            return list(dict.fromkeys([*runs, *nodes]))

    async def _dispatch_project(self, project_id: UUID) -> int:
        """Пересчёт и старт — под замком Project: параллельный проход ждёт."""
        async with self._sessions.begin() as session:
            await session.execute(select(func.pg_advisory_xact_lock(_project_lock_key(project_id))))
            started = await self._start_runs(session, project_id)
            return started + await self._grant(session, project_id)

    async def _start_runs(self, session: AsyncSession, project_id: UUID) -> int:
        active = await session.scalar(
            select(func.count()).where(
                PipelineRunRow.project_id == project_id,
                PipelineRunRow.state.in_(ACTIVE_RUNS),
            )
        )
        room = ACTIVE_RUNS_LIMIT - (active or 0)
        if room <= 0:
            return 0
        runs = (
            await session.scalars(
                select(PipelineRunRow)
                .where(
                    PipelineRunRow.project_id == project_id,
                    PipelineRunRow.state == RunState.PENDING,
                )
                .order_by(PipelineRunRow.created_at, PipelineRunRow.id)
                .limit(room)
                .with_for_update()
            )
        ).all()
        for run in runs:
            await self._advance(session, run)
        return len(runs)

    async def _grant(self, session: AsyncSession, project_id: UUID) -> int:
        """Места Project Quota готовым BLOCK-узлам по FIFO запусков; ретраи без приоритета.

        Первый узел без места останавливает очередь: все попытки одного размера.
        """
        live = await session.scalar(_live_attempts(project_id)) or 0
        runs = (
            await session.scalars(
                select(PipelineRunRow)
                .where(
                    PipelineRunRow.project_id == project_id,
                    PipelineRunRow.state == RunState.RUNNING,
                )
                .order_by(PipelineRunRow.created_at, PipelineRunRow.id)
                .with_for_update()
            )
        ).all()
        granted = 0
        for run in runs:
            for node in await _ready_blocks(session, run):
                if live >= self._quota.capacity:
                    self._log_exceeded(project_id, live)
                    return granted
                await self._submit_attempt(session, run, node)
                live += 1
                granted += 1
        return granted

    def _log_exceeded(self, project_id: UUID, live: int) -> None:
        used, ceiling = self._quota.used(live), self._quota.ceiling
        log.info(
            "project_quota_exceeded: project_id=%s, CPU %sm из %sm, RAM %sMi из %sMi",
            project_id,
            used.cpu_millicores,
            ceiling.cpu_millicores,
            used.memory_mib,
            ceiling.memory_mib,
        )

    async def _dispatch_isolated(self, project_id: UUID) -> int:
        """Сбой одного Project не роняет проход и не держит остальные: повторится позже.

        Так же зовётся быстрый путь после итога: при сбое место раздаст `dispatch_pending`.
        """
        try:
            return await self._dispatch_project(project_id)
        except Exception:
            log.exception("Project %s не продвинулся, повторится", project_id)
            return 0

    async def cancel(self, pipeline_run_id: UUID, *, by: UUID) -> PipelineRun:
        """Запрашивает отмену от имени `by`; запрос оставляет Audit Event.

        Повтор и отмена во время Fail-fast запуск не меняют, но запрос всё равно записан.
        """
        async with self._sessions.begin() as session:
            run = await session.get(PipelineRunRow, pipeline_run_id, with_for_update=True)
            if run is None:
                raise PipelineRunNotFound(pipeline_run_id)
            if run.state in FINISHED:
                raise RunNotCancellable(pipeline_run_id)
            await audit.record(
                session,
                audit.AuditAction.PIPELINE_RUN_CANCEL,
                user_id=by,
                project_id=run.project_id,
                object_id=run.id,
                dedup_key=str(run.id),
                trace_id=str(run.id),
            )
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
            ended = attempt.state not in LIVE
        if ended:
            await self._dispatch_isolated(run.project_id)

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

    async def purge_staging(self) -> int:
        """Возвращает число запусков, staging которых удалён целиком."""
        return await self._staging.purge_expired()

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
        initiator: UUID,
        graph: PipelineGraph,
        request: RunRequest,
        bindings: Bindings,
        idempotency_key: str,
        specs: Mapping[BlockRef, BlockSpec],
    ) -> PipelineRun:
        run_id = uuid4()
        async with self._sessions.begin() as session:
            # Сначала Pipeline, затем Dataset по возрастанию id: единый порядок исключает deadlock.
            pipeline = await self._pipelines.lock(session, request.pipeline_id)
            datasets = await self._datasets.lock_datasets(session, bindings.dataset_ids())
            inserted = await session.scalar(
                insert(PipelineRunRow)
                .values(
                    id=run_id,
                    project_id=project_id,
                    pipeline_id=request.pipeline_id,
                    pipeline_version=request.pipeline_version,
                    state=RunState.PENDING,
                    inputs=_dump(bindings.inputs),
                    outputs=_dump(bindings.outputs),
                    idempotency_key=idempotency_key,
                    request_hash=request.fingerprint(),
                    initiated_by_user_id=initiator,
                )
                .on_conflict_do_nothing(index_elements=["project_id", "idempotency_key"])
                .returning(PipelineRunRow.id)
            )
            if inserted is None:
                existing = await _find_by_key(session, project_id, idempotency_key)
                if existing is None:
                    raise RuntimeError(f"PipelineRun с ключом {idempotency_key} исчез")
                return _same_request(existing, request)
            # Архив проверяется после вставки: повтор с тем же ключом отдаёт исходный
            # запуск, а исключение откатывает только что вставленный.
            if pipeline.archived:
                raise PipelineArchived(str(pipeline.pipeline_id))
            check_not_archived(bindings, datasets)
            session.add_all(build_node_runs(run_id, graph, bindings, specs))
            await audit.record(
                session,
                audit.AuditAction.PIPELINE_RUN_CREATE,
                user_id=initiator,
                project_id=project_id,
                object_id=run_id,
                dedup_key=str(run_id),
                trace_id=str(run_id),
            )
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
        # BLOCK-узлы ждут в PENDING места в Project Quota: его выдаёт `_grant`.
        for node_run_id in decision.start:
            if by_id[node_run_id].kind == NodeKind.SYSTEM:
                by_id[node_run_id].state = NodeState.RUNNING
        run.state = decision.run_state
        if run.failed_node_run_id is None:
            run.failed_node_run_id = _first_failed(nodes)
        if run.state in TERMINAL_RUNS and run.finished_at is None:
            # Время транзакции, как у `func.now()`, но значением: его сразу отдаёт `_to_run`.
            run.finished_at = await session.scalar(select(func.now()))

    async def _submit_attempt(
        self, session: AsyncSession, run: PipelineRunRow, node: NodeRunRow
    ) -> None:
        """Попытка, её номер, Current Attempt и SubmitExecution в outbox — одной транзакцией.

        Номер — следующий за последней попыткой узла: ожидание места его не тратит.
        """
        if node.executable_ref is None:
            raise RuntimeError(f"у BLOCK NodeRun {node.id} нет executable_ref")
        number = await session.scalar(
            select(func.coalesce(func.max(ExecutionAttemptRow.number), 0) + 1).where(
                ExecutionAttemptRow.node_run_id == node.id
            )
        )
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
            payload=Submission(executable_ref=node.executable_ref, context=context, pod=self._pod),
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
        await _record_reads(session, run, node)
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
        """Провал доказуемо мёртвой попытки: узел ждёт места для следующей или FAILED.

        Отменяемый узел не ретраится: запуск уже останавливается.
        """
        _fail(attempt, reason, error)
        node.error = error
        if node.state != NodeState.CANCELLING and should_retry(attempt.number, reason):
            node.state = NodeState.PENDING
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
        if not await self._initiator_may_publish(publication):
            await self._transition(node_run_id, NodeState.RUNNING, _failed(ACCESS_REVOKED))
            return
        try:
            version = await self._datasets.publish(
                project_id=publication.project_id,
                dataset_id=publication.dataset_id,
                source=publication.source,
                manifest_uri=publication.manifest_uri,
                by=publication.initiator,
                trace_id=publication.trace_id,
            )
        except (catalog.PublicationRejected, catalog.DatasetNotFound) as error:
            await self._transition(node_run_id, NodeState.RUNNING, _failed(str(error)))
            return
        await self._transition(node_run_id, NodeState.RUNNING, _published(version.version_id))

    async def _initiator_may_publish(self, publication: "_Publication") -> bool:
        """Текущий Membership Initiator, а не права на момент запуска (ADR-0004)."""
        try:
            dataset = await self._datasets.get_dataset(publication.dataset_id)
        except catalog.DatasetNotFound:
            return True  # отказ с причиной даст сама публикация
        return await self._identity.has_role(publication.initiator, dataset.project_id, Role.EDITOR)

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
        return _Publication(
            run.project_id,
            run.initiated_by_user_id,
            node.target_dataset_id,
            source,
            output.manifest_uri,
            str(run.id),
        )

    async def _transition(self, node_run_id: UUID, expected: NodeState, change: NodeChange) -> None:
        """Меняет NodeRun, если он всё ещё в `expected`, и продвигает запуск.

        Итог узла освобождает место в квоте и готовит downstream: сразу выбор по Project.
        """
        async with self._sessions.begin() as session:
            locked = await _lock_node(session, node_run_id)
            if locked is None:
                return
            run, node = locked
            if node.state != expected:
                return
            await change(session, node)
            await self._advance(session, run)
        await self._dispatch_isolated(run.project_id)

    async def _node_ids(self, kind: NodeKind, state: NodeState) -> list[UUID]:
        async with self._sessions() as session:
            ids = await session.scalars(
                select(NodeRunRow.id).where(NodeRunRow.kind == kind, NodeRunRow.state == state)
            )
            return list(ids)


@dataclass(frozen=True)
class _Publication:
    project_id: UUID
    initiator: UUID
    dataset_id: UUID
    source: NodeOutputRef
    manifest_uri: str
    trace_id: str


async def _record_reads(session: AsyncSession, run: PipelineRunRow, node: NodeRunRow) -> None:
    """Initiator читает каждую входную DatasetVersion узла; повтор попытки записи не добавляет."""
    for port, ref in node_inputs(node).items():
        if not isinstance(ref, DatasetVersionRef):
            continue
        await audit.record(
            session,
            audit.AuditAction.DATASET_VERSION_READ,
            user_id=run.initiated_by_user_id,
            project_id=run.project_id,
            object_id=ref.version_id,
            dedup_key=f"{node.id}/{port}",
            trace_id=str(run.id),
        )


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


def _projects_with_waiting_runs() -> Select[UUID]:
    """Project с PENDING-запусками и местом под `ACTIVE_RUNS_LIMIT`, по самому давнему."""
    pending = PipelineRunRow.state == RunState.PENDING
    active = PipelineRunRow.state.in_(ACTIVE_RUNS)
    return (
        select(PipelineRunRow.project_id)
        .where(PipelineRunRow.state.in_(ACTIVE_RUNS | {RunState.PENDING}))
        .group_by(PipelineRunRow.project_id)
        .having(
            func.count().filter(pending) > 0,
            func.count().filter(active) < ACTIVE_RUNS_LIMIT,
        )
        .order_by(func.min(PipelineRunRow.created_at).filter(pending))
        .limit(DISPATCH_BATCH)
    )


def _projects_with_waiting_nodes(capacity: int) -> Select[UUID]:
    """Project с готовым PENDING BLOCK-узлом в RUNNING-запуске и местом в квоте.

    Готов — все upstream SUCCEEDED; узел за ещё работающим upstream Project сюда не тянет.
    """
    upstream = aliased(NodeRunRow)
    inputs = func.jsonb_each(NodeRunRow.inputs).table_valued("value")
    unfinished_upstream = (
        select(1)
        .select_from(inputs)
        .join(upstream, upstream.id == inputs.c.value.op("->>")("node_run_id").cast(Uuid))
        .where(upstream.state != NodeState.SUCCEEDED)
    )
    return (
        select(PipelineRunRow.project_id)
        .join(NodeRunRow, NodeRunRow.pipeline_run_id == PipelineRunRow.id)
        .where(
            PipelineRunRow.state == RunState.RUNNING,
            PipelineRunRow.failed_node_run_id.is_(None),
            NodeRunRow.kind == NodeKind.BLOCK,
            NodeRunRow.state == NodeState.PENDING,
            ~unfinished_upstream.exists(),
            _live_attempts(PipelineRunRow.project_id).scalar_subquery() < capacity,
        )
        .group_by(PipelineRunRow.project_id)
        .order_by(func.min(PipelineRunRow.created_at))
        .limit(DISPATCH_BATCH)
    )


def _live_attempts(project_id: UUID | SQLColumnExpression[UUID]) -> Select[int]:
    """Число LIVE-попыток Project: столько мест квоты занято, отменённая держит своё до итога."""
    node, run = aliased(NodeRunRow), aliased(PipelineRunRow)
    return (
        select(func.count())
        .select_from(ExecutionAttemptRow)
        .join(node, node.id == ExecutionAttemptRow.node_run_id)
        .join(run, run.id == node.pipeline_run_id)
        .where(run.project_id == project_id, ExecutionAttemptRow.state.in_(LIVE))
    )


async def _ready_blocks(session: AsyncSession, run: PipelineRunRow) -> list[NodeRunRow]:
    """BLOCK-узлы запуска, готовые к попытке; остановка запуска готовых не даёт."""
    nodes = (
        await session.scalars(
            select(NodeRunRow)
            .where(NodeRunRow.pipeline_run_id == run.id)
            .order_by(NodeRunRow.node_id)
            .with_for_update()
        )
    ).all()
    ready = set(plan([progress(node) for node in nodes]).start)
    return [node for node in nodes if node.id in ready and node.kind == NodeKind.BLOCK]


def _project_lock_key(project_id: UUID) -> int:
    """Ключ advisory lock Project: совпадение у разных Project лишь сериализует их."""
    return int.from_bytes(project_id.bytes[:8], signed=True)


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
        finished_at=row.finished_at,
        initiated_by_user_id=row.initiated_by_user_id,
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
