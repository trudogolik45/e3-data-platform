"""Фоновые шаги Run Orchestrator в процессе Control Plane.

Каждый шаг повторяется независимо: пока есть работа — сразу, без работы —
после паузы. Ошибка шага пишется в лог и не останавливает остальные, а
недоступная Kafka не мешает API принимать запуски.
"""

import asyncio
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager

from aiokafka.errors import KafkaError

from e3.runs.kafka import ensure_topics, event_consumer, kafka_publisher
from e3.runs.orchestrator import RunOrchestrator

log = logging.getLogger(__name__)

Step = Callable[[], Awaitable[int]]

KAFKA_RETRY_SECONDS = 5


@asynccontextmanager
async def background(
    orchestrator: RunOrchestrator, *, bootstrap_servers: str, group_id: str, idle_seconds: float
) -> AsyncGenerator[None]:
    """Шаги по PostgreSQL и S3 идут всегда; шаги через Kafka — пока она доступна."""
    jobs = [
        _repeat(orchestrator.dispatch_pending, idle_seconds),
        _repeat(orchestrator.finalize_pending, idle_seconds),
        _repeat(orchestrator.publish_pending, idle_seconds),
        _with_kafka(orchestrator, bootstrap_servers, group_id, idle_seconds),
    ]
    tasks = [asyncio.create_task(job) for job in jobs]
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
            await ensure_topics(bootstrap_servers)
            async with (
                kafka_publisher(bootstrap_servers) as publisher,
                event_consumer(bootstrap_servers, group_id, orchestrator) as events,
            ):
                await asyncio.gather(
                    _repeat(orchestrator.outbox_relay(publisher).publish_pending, idle_seconds),
                    _repeat(events.poll, idle_seconds),
                )
        except KafkaError:
            log.warning("Kafka недоступна, переподключение через %s с", KAFKA_RETRY_SECONDS)
            await asyncio.sleep(KAFKA_RETRY_SECONDS)


async def _repeat(step: Step, idle_seconds: float) -> None:
    while True:
        try:
            done = await step()
        except Exception:
            log.exception("шаг Run Orchestrator упал, повторим")
            done = 0
        if not done:
            await asyncio.sleep(idle_seconds)
