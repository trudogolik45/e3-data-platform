"""`e3-execution-controller` — процесс Execution Plane.

Читает `SubmitExecution` из `commands.v1`, создаёт SparkApplication и шлёт
события попыток в `events.v1`. Offset команды коммитится только после создания
ресурса: падение между ними даёт повтор, а повтор упирается в то же имя ресурса.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

import boto3
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from botocore.config import Config
from kubernetes_asyncio import config as kube_config
from kubernetes_asyncio.client import ApiClient, CustomObjectsApi
from kubernetes_asyncio.config.config_exception import ConfigException
from pydantic import ValidationError

from e3.execution.controller import ExecutionController
from e3.execution.kubernetes import KubernetesSparkApplications
from e3.execution.results import StoredResults
from e3.execution.settings import ExecutionSettings
from e3.shared.messages import COMMANDS_TOPIC, EVENTS_TOPIC, ExecutionEvent, parse_command
from e3.shared.storage import ObjectStorage

log = logging.getLogger(__name__)

RETRY_SECONDS = 5


class KafkaEvents:
    def __init__(self, producer: AIOKafkaProducer) -> None:
        self._producer = producer

    async def send(self, event: ExecutionEvent) -> None:
        await self._producer.send_and_wait(EVENTS_TOPIC, event.to_bytes(), key=event.key())


class CommandConsumer:
    def __init__(self, consumer: AIOKafkaConsumer, controller: ExecutionController) -> None:
        self._consumer = consumer
        self._controller = controller

    async def poll(self, timeout_ms: int) -> int:
        batches = cast(
            "dict[TopicPartition, list[ConsumerRecord[bytes, bytes]]]",
            await self._consumer.getmany(timeout_ms=timeout_ms),
        )
        records = [record for batch in batches.values() for record in batch]
        for record in records:
            await self._submit(record)
        if records:
            await self._consumer.commit()
        return len(records)

    async def _submit(self, record: "ConsumerRecord[bytes, bytes]") -> None:
        try:
            command = parse_command(record.value or b"")
        except ValidationError:
            log.error("непонятная команда в %s, offset %s пропущен", record.topic, record.offset)
            return
        await self._controller.submit(command)
        log.info("попытка %s отправлена в Spark", command.attempt_id)


async def serve(settings: ExecutionSettings) -> None:
    """Работает, пока не отменят; сбой Kafka или Kubernetes — переподключение."""
    while True:
        try:
            async with _commands(settings) as commands:
                await _loop(commands, settings.execution_poll_seconds)
        except Exception:
            log.exception("Execution Controller упал, переподключение через %s с", RETRY_SECONDS)
            await asyncio.sleep(RETRY_SECONDS)


async def _loop(commands: tuple[CommandConsumer, ExecutionController], poll: float) -> None:
    consumer, controller = commands
    while True:
        await consumer.poll(timeout_ms=int(poll * 1000))
        await controller.report()


@asynccontextmanager
async def _commands(
    settings: ExecutionSettings,
) -> AsyncGenerator[tuple[CommandConsumer, ExecutionController]]:
    await _load_kube_config()
    consumer = AIOKafkaConsumer(
        COMMANDS_TOPIC,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.execution_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers, acks="all", enable_idempotence=True
    )
    api = ApiClient()
    try:
        await producer.start()
        await consumer.start()
        controller = ExecutionController(
            KubernetesSparkApplications(CustomObjectsApi(api), settings.spark_namespace),
            KafkaEvents(producer),
            StoredResults(_storage(settings)),
            settings.workload(),
        )
        yield CommandConsumer(consumer, controller), controller
    finally:
        await consumer.stop()
        await producer.stop()
        await api.close()


async def _load_kube_config() -> None:
    try:
        kube_config.load_incluster_config()
    except ConfigException:
        await kube_config.load_kube_config()


def _storage(settings: ExecutionSettings) -> ObjectStorage:
    client = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    return ObjectStorage(client, client, settings.s3_bucket)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(ExecutionSettings()))
