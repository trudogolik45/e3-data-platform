"""Продвижение PipelineRun по графу: какие узлы запускать, пропускать и чем кончился запуск.

Чистая логика без БД: на вход — состояния NodeRun и их upstream.
"""

from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from uuid import UUID


class NodeState(StrEnum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    FINALIZING = "FINALIZING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


class RunState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


ACTIVE = frozenset(
    {NodeState.QUEUED, NodeState.RUNNING, NodeState.FINALIZING, NodeState.CANCELLING}
)
STOPPABLE = frozenset({NodeState.QUEUED, NodeState.RUNNING})


@dataclass(frozen=True)
class NodeProgress:
    node_run_id: UUID
    state: NodeState
    upstream: frozenset[UUID]
    # Начатую публикацию не прерываем: Spark-попытки у неё нет.
    stoppable: bool = True


@dataclass(frozen=True)
class Plan:
    run_state: RunState
    start: list[UUID] = field(default_factory=list[UUID])
    skip: list[UUID] = field(default_factory=list[UUID])
    cancel: list[UUID] = field(default_factory=list[UUID])


def plan(nodes: Sequence[NodeProgress], *, cancelling: bool = False) -> Plan:
    failed = any(node.state == NodeState.FAILED for node in nodes)
    if cancelling or failed:
        return _stopping(nodes, RunState.CANCELLING if cancelling else RunState.RUNNING)
    if all(node.state == NodeState.SUCCEEDED for node in nodes):
        return Plan(RunState.SUCCEEDED)
    succeeded = {node.node_run_id for node in nodes if node.state == NodeState.SUCCEEDED}
    pending = [node for node in nodes if node.state == NodeState.PENDING]
    ready = [node.node_run_id for node in pending if node.upstream <= succeeded]
    return Plan(RunState.RUNNING, start=ready)


def _stopping(nodes: Sequence[NodeProgress], in_progress: RunState) -> Plan:
    """Остановка запуска (Fail-fast или отмена): ничего не стартует, ожидающие пропускаются,
    работающие попытки отменяются. Итог — когда активных узлов не осталось.
    """
    skip = [node.node_run_id for node in nodes if node.state == NodeState.PENDING]
    cancel = [node.node_run_id for node in nodes if node.stoppable and node.state in STOPPABLE]
    if any(node.state in ACTIVE for node in nodes):
        return Plan(in_progress, skip=skip, cancel=cancel)
    failed = any(node.state == NodeState.FAILED for node in nodes)
    return Plan(RunState.FAILED if failed else RunState.CANCELLED, skip=skip)
