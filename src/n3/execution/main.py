"""`n3-execution-controller` — процесс Execution Plane.

Читает `SubmitExecution`, `ReconcileExecution` и `CancelExecution` из `commands.v1`,
создаёт или удаляет SparkApplication, ставит Attempt Fence и шлёт события попыток
в `events.v1`. Offset команды коммитится только после её обработки: падение
посреди даёт повтор, а повтор упирается в то же имя ресурса.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import boto3
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from botocore.config import Config
from kubernetes_asyncio import config as kube_config
from kubernetes_asyncio.client import ApiClient, CoreV1Api, CustomObjectsApi
from kubernetes_asyncio.config.config_exception import ConfigException
from pydantic import ValidationError

from n3.execution.controller import ExecutionController
from n3.execution.kubernetes import KubernetesSparkApplications
from n3.execution.results import StoredResults
from n3.execution.settings import ExecutionSettings
from n3.shared.kafka import Record, consume_batch, manual_commit_consumer, reliable_producer
from n3.shared.messages import COMMANDS_TOPIC, EVENTS_TOPIC, ExecutionEvent, parse_command
from n3.shared.storage import ObjectStorage

log = logging.getLogger(__name__)

RETRY_SECONDS = 5
COMMAND_BATCH = 100


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
        return await consume_batch(
            self._consumer, self._handle, timeout_ms=timeout_ms, max_records=COMMAND_BATCH
        )

    async def _handle(self, record: Record) -> None:
        try:
            command = parse_command(record.value or b"")
        except ValidationError:
            log.error("непонятная команда в %s, offset %s пропущен", record.topic, record.offset)
            return
        await self._controller.handle(command)
        log.info("%s попытки %s обработана", command.type, command.attempt_id)


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
    consumer = manual_commit_consumer(
        COMMANDS_TOPIC, settings.kafka_bootstrap_servers, settings.execution_group_id
    )
    producer = reliable_producer(settings.kafka_bootstrap_servers)
    api = ApiClient()
    try:
        await producer.start()
        await consumer.start()
        controller = ExecutionController(
            KubernetesSparkApplications(
                CustomObjectsApi(api), CoreV1Api(api), settings.spark_namespace
            ),
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
