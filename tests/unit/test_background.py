"""Фоновые циклы Run Orchestrator: сбой Kafka во время работы пересоздаёт связь."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest
from aiokafka.errors import KafkaConnectionError

from e3.runs import RunOrchestrator, background, workers

WAIT_SECONDS = 5


class IdleOrchestrator:
    async def dispatch_pending(self) -> int:
        return 0

    async def finalize_pending(self) -> int:
        return 0

    async def publish_pending(self) -> int:
        return 0

    async def reconcile_pending(self) -> int:
        return 0

    async def purge_staging(self) -> int:
        return 0

    async def purge_artifacts(self) -> int:
        return 0

    def outbox_relay(self, publisher: object) -> "IdleOrchestrator":
        return self


class BrokerLostOnce:
    """Kafka, которая пропадает в первой сессии и работает во второй."""

    def __init__(self) -> None:
        self.sessions = 0
        self.reconnected = asyncio.Event()

    @asynccontextmanager
    async def consumer(self, *_args: object) -> AsyncGenerator["BrokerLostOnce"]:
        self.sessions += 1
        if self.sessions > 1:
            self.reconnected.set()
        yield self

    async def poll(self) -> int:
        if self.sessions == 1:
            raise KafkaConnectionError("брокер пропал")
        return 0


@asynccontextmanager
async def publisher(_bootstrap_servers: str) -> AsyncGenerator[object]:
    yield object()


async def no_topics(_bootstrap_servers: str) -> None:
    return None


async def test_kafka_failure_while_running_rebuilds_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = BrokerLostOnce()
    monkeypatch.setattr(workers, "KAFKA_RETRY_SECONDS", 0)
    monkeypatch.setattr(workers, "ensure_topics", no_topics)
    monkeypatch.setattr(workers, "kafka_publisher", publisher)
    monkeypatch.setattr(workers, "event_consumer", broker.consumer)
    orchestrator = cast(RunOrchestrator, cast(Any, IdleOrchestrator()))

    async with background(
        orchestrator, bootstrap_servers="kafka", group_id="g", idle_seconds=0.01, purge_seconds=60
    ):
        await asyncio.wait_for(broker.reconnected.wait(), WAIT_SECONDS)

    assert broker.sessions >= 2
