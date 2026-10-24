"""Transactional outbox и inbox (ADR-0001).

Команда пишется в outbox в той же транзакции, что и смена состояния, а
`OutboxRelay` отдельно отправляет её в Kafka. Отметка об отправке
фиксируется только после подтверждения брокера, поэтому сбой между записью
и отправкой не теряет команду: следующий проход relay её дошлёт (at-least-once).
Повторы гасит inbox на стороне получателя.
"""

import json
from typing import Protocol
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from e3.runs.models import InboxRow, OutboxRow
from e3.shared.messages import COMMANDS_TOPIC, SubmitExecution

RELAY_BATCH = 100


class Publisher(Protocol):
    async def send(self, topic: str, *, key: bytes, value: bytes) -> None:
        """Возвращается после подтверждения брокера."""
        ...


def enqueue(session: AsyncSession, command: SubmitExecution) -> None:
    session.add(
        OutboxRow(
            id=command.event_id,
            topic=COMMANDS_TOPIC,
            key=str(command.node_run_id),
            payload=command.model_dump(mode="json"),
        )
    )


async def first_delivery(session: AsyncSession, event_id: UUID) -> bool:
    """Отмечает событие в inbox; `False` — его уже обработали."""
    inserted = await session.scalar(
        insert(InboxRow)
        .values(event_id=event_id)
        .on_conflict_do_nothing(index_elements=["event_id"])
        .returning(InboxRow.event_id)
    )
    return inserted is not None


class OutboxRelay:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], publisher: Publisher) -> None:
        self._sessions = sessions
        self._publisher = publisher

    async def publish_pending(self) -> int:
        """Отправляет неотправленное в порядке записи; возвращает число отправленных."""
        async with self._sessions.begin() as session:
            rows = (
                await session.scalars(
                    select(OutboxRow)
                    .where(OutboxRow.published_at.is_(None))
                    .order_by(OutboxRow.created_at)
                    .limit(RELAY_BATCH)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for row in rows:
                await self._publisher.send(
                    row.topic, key=row.key.encode(), value=json.dumps(row.payload).encode()
                )
            await session.execute(
                update(OutboxRow)
                .where(OutboxRow.id.in_([row.id for row in rows]))
                .values(published_at=func.now())
            )
            return len(rows)
