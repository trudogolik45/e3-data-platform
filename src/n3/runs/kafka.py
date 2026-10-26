"""Kafka-транспорт Run Orchestrator: отправка команд и чтение событий.

Offset события коммитится только после его обработки (at-least-once, см.
`n3.shared.kafka`): повторную доставку гасит inbox.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Protocol

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from pydantic import ValidationError

from n3.shared.kafka import Record, consume_batch, manual_commit_consumer, reliable_producer
from n3.shared.messages import COMMANDS_TOPIC, EVENTS_TOPIC, ExecutionEvent, parse_event

log = logging.getLogger(__name__)

# Порядок важен только внутри NodeRun: ключ сообщения — `node_run_id`.
TOPIC_PARTITIONS = 3


class EventHandler(Protocol):
    async def handle_event(self, event: ExecutionEvent) -> None: ...


async def ensure_topics(bootstrap_servers: str, *, partitions: int = TOPIC_PARTITIONS) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        for topic in (COMMANDS_TOPIC, EVENTS_TOPIC):
            await _create_topic(admin, NewTopic(topic, partitions, replication_factor=1))
    finally:
        await admin.close()


async def _create_topic(admin: AIOKafkaAdminClient, topic: NewTopic) -> None:
    try:
        await admin.create_topics([topic])
    except TopicAlreadyExistsError:
        return


class KafkaPublisher:
    def __init__(self, producer: AIOKafkaProducer) -> None:
        self._producer = producer

    async def send(self, topic: str, *, key: bytes, value: bytes) -> None:
        await self._producer.send_and_wait(topic, value, key=key)


@asynccontextmanager
async def kafka_publisher(bootstrap_servers: str) -> AsyncGenerator[KafkaPublisher]:
    producer = reliable_producer(bootstrap_servers)
    await producer.start()
    try:
        yield KafkaPublisher(producer)
    finally:
        await producer.stop()


class EventConsumer:
    def __init__(self, consumer: AIOKafkaConsumer, handler: EventHandler) -> None:
        self._consumer = consumer
        self._handler = handler

    async def poll(self, *, max_records: int = 100, timeout_ms: int = 500) -> int:
        """Обрабатывает пачку событий; возвращает число обработанных."""
        return await consume_batch(
            self._consumer, self._handle, timeout_ms=timeout_ms, max_records=max_records
        )

    async def _handle(self, record: Record) -> None:
        try:
            event = parse_event(record.value or b"")
        except ValidationError:
            log.error("непонятное сообщение в %s, offset %s пропущен", record.topic, record.offset)
            return
        await self._handler.handle_event(event)


@asynccontextmanager
async def event_consumer(
    bootstrap_servers: str, group_id: str, handler: EventHandler
) -> AsyncGenerator[EventConsumer]:
    consumer = manual_commit_consumer(EVENTS_TOPIC, bootstrap_servers, group_id)
    await consumer.start()
    try:
        yield EventConsumer(consumer, handler)
    finally:
        await consumer.stop()
