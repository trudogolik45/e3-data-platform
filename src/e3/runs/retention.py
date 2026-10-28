"""Срок хранения staging: `staging/executions/{attempt_id}/` всех попыток запуска
удаляется через `retention` после завершения PipelineRun (PRD §8, ADR-0002).

Какие запуски чистить, решает PostgreSQL (ADR-0001). Запуск помечается
`staging_purged_at`, только когда удалены префиксы всех его попыток, поэтому
сбой одной попытки повторится следующим проходом, а очищенный запуск больше
не трогается. Записи в БД остаются: staging — не публичный результат.
Опубликованные данные лежат в `datasets/`, а загрузки — в `staging/uploads/`:
сюда они не попадают.
"""

import logging
from collections import defaultdict
from datetime import timedelta
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from e3.runs.finalization import execution_prefix
from e3.runs.models import ExecutionAttemptRow, NodeRunRow, PipelineRunRow
from e3.shared.storage import ObjectStorage

log = logging.getLogger(__name__)


class StagingRetention:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        storage: ObjectStorage,
        *,
        retention: timedelta,
    ) -> None:
        self._sessions = sessions
        self._storage = storage
        self._retention = retention

    async def purge_expired(self) -> int:
        """Возвращает число запусков, staging которых удалён целиком."""
        expired = await self._expired()
        return sum([await self._purge(run_id, attempts) for run_id, attempts in expired.items()])

    async def _expired(self) -> dict[UUID, list[UUID]]:
        """Неочищенные запуски старше срока и попытки их NodeRun, включая неуспешные."""
        async with self._sessions() as session:
            rows = await session.execute(
                select(PipelineRunRow.id, ExecutionAttemptRow.attempt_id)
                .join(NodeRunRow, NodeRunRow.pipeline_run_id == PipelineRunRow.id)
                .outerjoin(ExecutionAttemptRow, ExecutionAttemptRow.node_run_id == NodeRunRow.id)
                .where(
                    PipelineRunRow.staging_purged_at.is_(None),
                    PipelineRunRow.finished_at < func.now() - self._retention,
                )
            )
        attempts: defaultdict[UUID, list[UUID]] = defaultdict(list)
        for run_id, attempt_id in rows:
            attempts[run_id] += [attempt_id] if attempt_id else []
        return attempts

    async def _purge(self, run_id: UUID, attempts: list[UUID]) -> bool:
        deleted = [await self._try_delete(attempt_id) for attempt_id in attempts]
        if not all(deleted):
            return False
        async with self._sessions.begin() as session:
            await session.execute(
                update(PipelineRunRow)
                .where(PipelineRunRow.id == run_id)
                .values(staging_purged_at=func.now())
            )
        return True

    async def _try_delete(self, attempt_id: UUID) -> bool:
        """Сбой одной попытки не останавливает остальные: повторится следующим проходом."""
        try:
            await self._storage.delete_prefix(f"{execution_prefix(attempt_id)}/")
        except Exception:
            log.exception("staging попытки %s не удалён, повторится", attempt_id)
            return False
        return True
