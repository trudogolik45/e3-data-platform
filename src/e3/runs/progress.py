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


class RunState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


ACTIVE = frozenset({NodeState.QUEUED, NodeState.RUNNING, NodeState.FINALIZING})


@dataclass(frozen=True)
class NodeProgress:
    node_run_id: UUID
    state: NodeState
    upstream: frozenset[UUID]


@dataclass(frozen=True)
class Plan:
    run_state: RunState
    start: list[UUID] = field(default_factory=list[UUID])
    skip: list[UUID] = field(default_factory=list[UUID])


def plan(nodes: Sequence[NodeProgress]) -> Plan:
    pending = [node for node in nodes if node.state == NodeState.PENDING]
    if any(node.state == NodeState.FAILED for node in nodes):
        return _failing(nodes, pending)
    if all(node.state == NodeState.SUCCEEDED for node in nodes):
        return Plan(RunState.SUCCEEDED)
    succeeded = {node.node_run_id for node in nodes if node.state == NodeState.SUCCEEDED}
    ready = [node.node_run_id for node in pending if node.upstream <= succeeded]
    return Plan(RunState.RUNNING, start=ready)


def _failing(nodes: Sequence[NodeProgress], pending: Sequence[NodeProgress]) -> Plan:
    """Упавший узел: ожидающие пропускаются, запуск падает, когда активных не осталось."""
    skip = [node.node_run_id for node in pending]
    active = any(node.state in ACTIVE for node in nodes)
    return Plan(RunState.RUNNING if active else RunState.FAILED, skip=skip)
