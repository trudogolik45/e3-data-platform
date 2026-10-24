"""Доставка событий `events.v1` в Run Orchestrator: сбой обработки не теряет событие."""

from uuid import UUID, uuid4

import pytest
from aiokafka import AIOKafkaProducer

from e3.runs import event_consumer
from e3.shared.messages import EVENTS_TOPIC, ExecutionEvent, ExecutionStarted

pytestmark = pytest.mark.integration

POLLS = 20


class FailsOnceOn:
    """Обработчик, который падает при первой встрече с событием `event_id`."""

    def __init__(self, event_id: UUID) -> None:
        self._event_id = event_id
        self._failed = False
        self.handled: list[UUID] = []

    async def handle_event(self, event: ExecutionEvent) -> None:
        if event.event_id == self._event_id and not self._failed:
            self._failed = True
            raise ConnectionError("PostgreSQL недоступен")
        self.handled.append(event.event_id)


async def send(kafka: str, event: ExecutionEvent) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=kafka, acks="all")
    await producer.start()
    try:
        await producer.send_and_wait(EVENTS_TOPIC, event.to_bytes(), key=event.key())
    finally:
        await producer.stop()


async def test_event_whose_handling_failed_is_handled_on_next_poll(kafka: str) -> None:
    event = ExecutionStarted(node_run_id=uuid4(), attempt_id=uuid4(), trace_id="trace")
    handler = FailsOnceOn(event.event_id)
    await send(kafka, event)

    async with event_consumer(kafka, f"test-{uuid4()}", handler) as events:
        for _ in range(POLLS):
            if event.event_id in handler.handled:
                break
            try:
                await events.poll(timeout_ms=500)
            except ConnectionError:
                continue

    assert handler.handled.count(event.event_id) == 1
