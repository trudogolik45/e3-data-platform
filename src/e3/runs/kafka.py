"""Kafka-транспорт Run Orchestrator: отправка команд и чтение событий.

Offset события коммитится только после его обработки (at-least-once):
падение между обработкой и коммитом даёт повторную доставку, её гасит inbox.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Protocol, cast

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from pydantic import ValidationError

from e3.shared.messages import COMMANDS_TOPIC, EVENTS_TOPIC, ExecutionEvent, parse_event

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
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers, acks="all", enable_idempotence=True
    )
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
        """Обрабатывает пачку событий и коммитит offset; возвращает размер пачки."""
        batches = cast(
            "dict[TopicPartition, list[ConsumerRecord[bytes, bytes]]]",
            await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records),
        )
        records = [record for batch in batches.values() for record in batch]
        for record in records:
            await self._handle(record)
        if records:
            await self._consumer.commit()
        return len(records)

    async def _handle(self, record: "ConsumerRecord[bytes, bytes]") -> None:
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
    consumer = AIOKafkaConsumer(
        EVENTS_TOPIC,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        yield EventConsumer(consumer, handler)
    finally:
        await consumer.stop()
