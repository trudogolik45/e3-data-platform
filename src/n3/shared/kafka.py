"""Kafka-клиенты обеих плоскостей: надёжная отправка и чтение пачками (at-least-once).

Offset записи коммитится только после её обработки. Сбой обработки возвращает
партицию к упавшей записи: она придёт снова в следующей пачке, а не
потеряется. Повторы гасит inbox или идемпотентность получателя.
"""

from collections.abc import Awaitable, Callable
from typing import cast

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition

type Record = ConsumerRecord[bytes, bytes]
type RecordHandler = Callable[[Record], Awaitable[None]]


def reliable_producer(bootstrap_servers: str) -> AIOKafkaProducer:
    return AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers, acks="all", enable_idempotence=True
    )


def manual_commit_consumer(topic: str, bootstrap_servers: str, group_id: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


async def consume_batch(
    consumer: AIOKafkaConsumer, handle: RecordHandler, *, timeout_ms: int, max_records: int
) -> int:
    """Обрабатывает пачку и коммитит обработанное; возвращает число обработанных записей.

    Первый сбой обработки пробрасывается после коммита, чтобы вызывающий взял паузу.
    """
    batches = cast(
        "dict[TopicPartition, list[Record]]",
        await consumer.getmany(timeout_ms=timeout_ms, max_records=max_records),
    )
    offsets: dict[TopicPartition, int] = {}
    errors: list[Exception] = []
    handled = 0
    for partition, records in batches.items():
        done, error = await _consume_partition(records, handle)
        handled += done
        offsets[partition] = records[done].offset if error else records[-1].offset + 1
        if error is not None:
            consumer.seek(partition, offsets[partition])
            errors.append(error)
    if offsets:
        await consumer.commit(offsets)
    if errors:
        raise errors[0]
    return handled


async def _consume_partition(
    records: list[Record], handle: RecordHandler
) -> tuple[int, Exception | None]:
    """Обрабатывает записи партиции по порядку до первого сбоя."""
    for done, record in enumerate(records):
        try:
            await handle(record)
        except Exception as error:
            return done, error
    return len(records), None
