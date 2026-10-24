"""Фоновые циклы Run Orchestrator в процессе Control Plane.

Каждый цикл повторяется независимо: пока есть работа — сразу, без работы или
после сбоя — после паузы. Сбой пишется в лог и не останавливает остальные
циклы. Сбой Kafka пересоздаёт связь с ней, а недоступная Kafka не мешает API
принимать запуски.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager

from aiokafka.errors import KafkaError

from e3.runs.kafka import ensure_topics, event_consumer, kafka_publisher
from e3.runs.orchestrator import RunOrchestrator

log = logging.getLogger(__name__)

Tick = Callable[[], Awaitable[int]]

KAFKA_RETRY_SECONDS = 5


@asynccontextmanager
async def background(
    orchestrator: RunOrchestrator, *, bootstrap_servers: str, group_id: str, idle_seconds: float
) -> AsyncGenerator[None]:
    """Циклы по PostgreSQL и S3 идут всегда; циклы через Kafka — пока она доступна."""
    loops = [
        _repeat(orchestrator.dispatch_pending, idle_seconds),
        _repeat(orchestrator.finalize_pending, idle_seconds),
        _repeat(orchestrator.publish_pending, idle_seconds),
        _with_kafka(orchestrator, bootstrap_servers, group_id, idle_seconds),
    ]
    tasks = [asyncio.create_task(loop) for loop in loops]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _with_kafka(
    orchestrator: RunOrchestrator, bootstrap_servers: str, group_id: str, idle_seconds: float
) -> None:
    """Держит связь с Kafka; без неё команды копятся в outbox и уйдут после переподключения."""
    while True:
        try:
            await _kafka_session(orchestrator, bootstrap_servers, group_id, idle_seconds)
        except* KafkaError:
            log.warning("Kafka недоступна, переподключение через %s с", KAFKA_RETRY_SECONDS)
        await asyncio.sleep(KAFKA_RETRY_SECONDS)


async def _kafka_session(
    orchestrator: RunOrchestrator, bootstrap_servers: str, group_id: str, idle_seconds: float
) -> None:
    """Сбой Kafka в любом цикле останавливает оба и закрывает клиентов."""
    await ensure_topics(bootstrap_servers)
    async with (
        kafka_publisher(bootstrap_servers) as publisher,
        event_consumer(bootstrap_servers, group_id, orchestrator) as events,
        asyncio.TaskGroup() as group,
    ):
        group.create_task(
            _repeat(orchestrator.outbox_relay(publisher).publish_pending, idle_seconds)
        )
        group.create_task(_repeat(events.poll, idle_seconds))


async def _repeat(tick: Tick, idle_seconds: float) -> None:
    """Сбой Kafka пробрасывается, чтобы связь пересоздали; остальные сбои — в лог."""
    while True:
        try:
            done = await tick()
        except KafkaError:
            raise
        except Exception:
            log.exception("цикл Run Orchestrator упал, повторим")
            done = 0
        if not done:
            await asyncio.sleep(idle_seconds)
