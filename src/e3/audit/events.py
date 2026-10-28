"""Запись Audit Event.

Разрешённое действие пишется в его же транзакции: откат действия откатывает и запись.
Повтор действия приходит с тем же `dedup_key` и второй записи не даёт.
Отказ пишется своей транзакцией: транзакция запроса, которому отказали, откатывается.
"""

import logging
from enum import StrEnum
from uuid import UUID, uuid4

from fastapi import Request
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from e3.audit.models import AuditEventRow

logger = logging.getLogger(__name__)


class AuditAction(StrEnum):
    PIPELINE_RUN_CREATE = "pipeline_run.create"
    PIPELINE_RUN_CANCEL = "pipeline_run.cancel"
    DATASET_UPLOAD = "dataset.upload"
    DATASET_VERSION_READ = "dataset_version.read"
    DATASET_PUBLISH = "dataset.publish"
    ORGANIZATION_CREATE = "organization.create"
    PROJECT_CREATE = "project.create"
    MEMBERSHIP_GRANT = "membership.grant"
    MEMBERSHIP_CHANGE_ROLE = "membership.change_role"
    MEMBERSHIP_REVOKE = "membership.revoke"


class AuditOutcome(StrEnum):
    ALLOWED = "allowed"
    DENIED = "denied"


async def record(
    session: AsyncSession,
    action: AuditAction,
    *,
    user_id: UUID | None,
    project_id: UUID | None,
    object_id: UUID,
    dedup_key: str | None,
    trace_id: str | None = None,
    role: str | None = None,
    previous_role: str | None = None,
) -> None:
    """Разрешённое действие в транзакции `session`; транзакцией управляет вызывающий.

    `user_id = None` — только действие Platform Team, а не «неизвестно кто».
    `dedup_key = None` — каждое действие своей записью (смена Role туда и обратно).
    `role` — Role после изменения Membership, `previous_role` — до него.
    """
    await session.execute(
        insert(AuditEventRow)
        .values(
            id=uuid4(),
            action=action,
            outcome=AuditOutcome.ALLOWED,
            user_id=user_id,
            project_id=project_id,
            object_id=object_id,
            dedup_key=dedup_key,
            trace_id=trace_id,
            role=role,
            previous_role=previous_role,
        )
        .on_conflict_do_nothing(index_elements=["action", "dedup_key"])
    )


class AuditLog:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def denied(
        self,
        action: AuditAction,
        *,
        user_id: UUID,
        project_id: UUID,
        object_id: UUID | None,
        role: str | None = None,
        previous_role: str | None = None,
    ) -> None:
        """Отказ в доступе; сбой записи только в лог — ответ клиенту он не меняет.

        `role` — запрошенная Role, `previous_role` — текущая, если известна.
        """
        try:
            async with self._sessions.begin() as session:
                session.add(
                    AuditEventRow(
                        id=uuid4(),
                        action=action,
                        outcome=AuditOutcome.DENIED,
                        user_id=user_id,
                        project_id=project_id,
                        object_id=object_id,
                        role=role,
                        previous_role=previous_role,
                    )
                )
        except (SQLAlchemyError, OSError):
            logger.exception("Audit Event об отказе не записан: %s %s", action, object_id)


def audit_log(request: Request) -> AuditLog:
    log: AuditLog = request.app.state.audit
    return log
