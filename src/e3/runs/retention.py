"""Сроки хранения после завершения PipelineRun (PRD §8, ADR-0002): staging попыток
`staging/executions/{attempt_id}/` — 7 суток, их Diagnostic Artifact
`artifacts/{node_run_id}/{attempt_id}/` — 30.

Какие запуски чистить, решает PostgreSQL (ADR-0001). У каждого правила своя
отметка на запуске: она ставится, только когда удалены префиксы всех его
попыток, поэтому сбой одной попытки повторится следующим проходом, а очищенный
запуск больше не трогается. Записи в БД остаются: staging и логи — не публичный
результат. Опубликованные данные лежат в `datasets/`, а загрузки — в
`staging/uploads/`: сюда они не попадают.
"""

import logging
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import SQLColumnExpression, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from e3.runs.finalization import execution_prefix
from e3.runs.models import ExecutionAttemptRow, NodeRunRow, PipelineRunRow
from e3.shared.contracts import diagnostic_artifact_prefix
from e3.shared.storage import ObjectStorage
from e3.shared.tracing import traced

log = logging.getLogger(__name__)

# Префикс попытки по (node_run_id, attempt_id).
AttemptPrefix = Callable[[UUID, UUID], str]


@dataclass(frozen=True)
class RetentionRule:
    """Что удалять у попыток запуска и какой отметкой это записать."""

    name: str
    retention: timedelta
    prefix: AttemptPrefix
    purged_at: SQLColumnExpression[datetime | None]


def staging_rule(retention: timedelta) -> RetentionRule:
    return RetentionRule(
        name="staging",
        retention=retention,
        prefix=lambda _node_run_id, attempt_id: execution_prefix(attempt_id),
        purged_at=PipelineRunRow.staging_purged_at,
    )


def artifacts_rule(retention: timedelta) -> RetentionRule:
    return RetentionRule(
        name="artifacts",
        retention=retention,
        prefix=diagnostic_artifact_prefix,
        purged_at=PipelineRunRow.artifacts_purged_at,
    )


class Retention:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        storage: ObjectStorage,
        rule: RetentionRule,
    ) -> None:
        self._sessions = sessions
        self._storage = storage
        self._rule = rule

    async def purge_expired(self) -> int:
        """Возвращает число запусков, очищенных по правилу целиком."""
        expired = await self._expired()
        return sum([await self._purge(run_id, attempts) for run_id, attempts in expired.items()])

    async def _expired(self) -> dict[UUID, list[tuple[UUID, UUID]]]:
        """Неочищенные запуски старше срока и попытки их NodeRun, включая неуспешные."""
        async with self._sessions() as session:
            rows = await session.execute(
                select(PipelineRunRow.id, NodeRunRow.id, ExecutionAttemptRow.attempt_id)
                .join(NodeRunRow, NodeRunRow.pipeline_run_id == PipelineRunRow.id)
                .outerjoin(ExecutionAttemptRow, ExecutionAttemptRow.node_run_id == NodeRunRow.id)
                .where(
                    self._rule.purged_at.is_(None),
                    PipelineRunRow.finished_at < func.now() - self._rule.retention,
                )
            )
        attempts: defaultdict[UUID, list[tuple[UUID, UUID]]] = defaultdict(list)
        for run_id, node_run_id, attempt_id in rows:
            attempts[run_id] += [(node_run_id, attempt_id)] if attempt_id else []
        return attempts

    async def _purge(self, run_id: UUID, attempts: list[tuple[UUID, UUID]]) -> bool:
        """Логи очистки несут `trace_id` запуска."""
        with traced(str(run_id)):
            deleted = [await self._try_delete(*attempt) for attempt in attempts]
            if not all(deleted):
                return False
            async with self._sessions.begin() as session:
                await session.execute(
                    update(PipelineRunRow)
                    .where(PipelineRunRow.id == run_id)
                    .values({self._rule.purged_at: func.now()})
                )
            log.info("%s PipelineRun %s удалён", self._rule.name, run_id)
            return True

    async def _try_delete(self, node_run_id: UUID, attempt_id: UUID) -> bool:
        """Сбой одной попытки не останавливает остальные: повторится следующим проходом."""
        try:
            await self._storage.delete_prefix(f"{self._rule.prefix(node_run_id, attempt_id)}/")
        except Exception:
            log.exception("%s попытки %s не удалён, повторится", self._rule.name, attempt_id)
            return False
        return True
