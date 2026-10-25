"""Transactional outbox и inbox (ADR-0001).

Команда пишется в outbox в той же транзакции, что и смена состояния, а
`OutboxRelay` отдельно отправляет её в Kafka. Relay короткой транзакцией
берёт строки в аренду, шлёт их без транзакции и блокировок, а отметку об
отправке фиксирует только после подтверждения брокера. Сбой отправки
возвращает неотправленное сразу, падение процесса — по истечении аренды:
следующий проход relay команду дошлёт (at-least-once). Повторы гасит
получатель.
"""

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol
from uuid import UUID

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from e3.runs.models import InboxRow, OutboxRow
from e3.shared.messages import COMMANDS_TOPIC, ReconcileExecution, SubmitExecution

RELAY_BATCH = 100
RELAY_LEASE = timedelta(minutes=1)


class Publisher(Protocol):
    async def send(self, topic: str, *, key: bytes, value: bytes) -> None:
        """Возвращается после подтверждения брокера."""
        ...


def enqueue(session: AsyncSession, command: SubmitExecution | ReconcileExecution) -> None:
    session.add(
        OutboxRow(
            id=command.event_id,
            topic=COMMANDS_TOPIC,
            key=str(command.node_run_id),
            payload=command.model_dump(mode="json"),
        )
    )


async def unsent(session: AsyncSession, ids: list[UUID]) -> set[UUID]:
    """Те из `ids`, что лежат в outbox и ещё не отправлены."""
    if not ids:
        return set()
    found = await session.scalars(
        select(OutboxRow.id).where(OutboxRow.id.in_(ids), OutboxRow.published_at.is_(None))
    )
    return set(found)


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
        messages = await self._claim()
        if not messages:
            return 0
        sent: list[UUID] = []
        try:
            for message in messages:
                await self._publisher.send(message.topic, key=message.key, value=message.value)
                sent.append(message.id)
        finally:
            await self._settle(sent, [m.id for m in messages if m.id not in sent])
        return len(sent)

    async def _claim(self) -> list["_Message"]:
        async with self._sessions.begin() as session:
            rows = (
                await session.scalars(
                    select(OutboxRow)
                    .where(
                        OutboxRow.published_at.is_(None),
                        or_(
                            OutboxRow.claimed_until.is_(None),
                            OutboxRow.claimed_until < func.now(),
                        ),
                    )
                    .order_by(OutboxRow.created_at)
                    .limit(RELAY_BATCH)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            for row in rows:
                row.claimed_until = func.now() + RELAY_LEASE
            return [_Message.of(row) for row in rows]

    async def _settle(self, sent: list[UUID], unsent: list[UUID]) -> None:
        """Отмечает отправленное и сразу возвращает неотправленное следующему проходу."""
        async with self._sessions.begin() as session:
            await session.execute(
                update(OutboxRow).where(OutboxRow.id.in_(sent)).values(published_at=func.now())
            )
            await session.execute(
                update(OutboxRow).where(OutboxRow.id.in_(unsent)).values(claimed_until=None)
            )


@dataclass(frozen=True)
class _Message:
    id: UUID
    topic: str
    key: bytes
    value: bytes

    @classmethod
    def of(cls, row: OutboxRow) -> "_Message":
        return cls(row.id, row.topic, row.key.encode(), json.dumps(row.payload).encode())
