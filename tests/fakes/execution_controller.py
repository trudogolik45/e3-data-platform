"""Fake Execution Controller: заменяет Spark в integration-тестах Control Plane.

Живёт только в `tests/`: код продукта его не импортирует (закреплено `lint-imports`).
Читает SubmitExecution из `commands.v1` и пишет выход в
`staging/executions/{attempt_id}/`: CSV на входе `source` превращает, как блок
`Read`, в настоящий Parquet с manifest v1 на выходе `data`, а Parquet любого
другого блока копирует как есть на выход `result`. Вычисления Filter и Aggregate
он не повторяет: их проверяют тесты блоков на Spark. Только после записи отвечает
событиями в `events.v1`. Тест сам решает, чем ответить:
успехом, падением до записи или повторной доставкой события.

Для сценариев сбоев есть и настоящий `ExecutionController` над кластером в
памяти (`cluster`: SparkApplication и pod'ы): тест доставляет ему команды,
двигает состояния Spark и pod'ов, а итог Controller выводит сам. Новый
`controller()` над тем же `cluster` — рестарт Controller.
"""

import asyncio
import base64
import csv
import hashlib
import io
from types import TracebackType
from typing import Any, Self, cast
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from mypy_boto3_s3 import S3Client

from e3.execution.controller import ExecutionController
from e3.execution.results import StoredResults
from e3.execution.workload import Pod, WorkloadConfig, workload_name
from e3.shared.contracts import RESULT_FILE, ExecutionResult, ResolvedInput
from e3.shared.manifest import Column, DataSchema, Manifest, ManifestFile
from e3.shared.messages import (
    COMMANDS_TOPIC,
    EVENTS_TOPIC,
    Command,
    ExecutionEvent,
    ExecutionFailed,
    ExecutionSucceeded,
    FailureReason,
    SubmitExecution,
    failed,
    parse_command,
    started,
    succeeded,
)
from e3.shared.storage import ObjectStorage
from tests.fakes.spark_applications import InMemorySparkApplications

DATA_FILE = "data/part-00000.parquet"
COMMAND_TIMEOUT_SECONDS = 15
QUIET_SECONDS = 1
WORKLOAD = WorkloadConfig(
    namespace="default",
    service_account="spark-operator-spark",
    spark_version="3.5.9",
    s3_endpoint_url="http://minio:9000",
    s3_secret="e3-s3",
)


class FakeExecutionController:
    def __init__(self, bootstrap_servers: str, s3: S3Client, bucket: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._s3 = s3
        self._bucket = bucket
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers, enable_auto_commit=False
        )
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers, acks="all")
        self.cluster = InMemorySparkApplications()

    async def __aenter__(self) -> Self:
        """Слушает только команды, отправленные после старта."""
        await self._producer.start()
        await self._consumer.start()
        partitions = await self._command_partitions()
        self._consumer.assign(partitions)
        await self._consumer.seek_to_end(*partitions)
        for partition in partitions:
            await self._consumer.position(partition)
        return self

    async def _command_partitions(self) -> list[TopicPartition]:
        admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
        await admin.start()
        try:
            described: Any = await admin.describe_topics([COMMANDS_TOPIC])
        finally:
            await admin.close()
        return [TopicPartition(COMMANDS_TOPIC, p["partition"]) for p in described[0]["partitions"]]

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._consumer.stop()
        await self._producer.stop()

    async def next_command(self) -> Command:
        async with asyncio.timeout(COMMAND_TIMEOUT_SECONDS):
            return await self._command()

    async def submission(self, node_run_id: UUID) -> SubmitExecution:
        """Ближайшая SubmitExecution для NodeRun; чужие команды пропускает."""
        while True:
            command = await self.next_command()
            if isinstance(command, SubmitExecution) and command.node_run_id == node_run_id:
                return command

    async def quiet_commands(self) -> list[Command]:
        """Все команды, пришедшие до паузы в `QUIET_SECONDS`."""
        commands: list[Command] = []
        while True:
            try:
                async with asyncio.timeout(QUIET_SECONDS):
                    commands.append(await self._command())
            except TimeoutError:
                return commands

    async def _command(self) -> Command:
        message = cast("ConsumerRecord[bytes, bytes]", await self._consumer.getone())
        assert message.value is not None
        return parse_command(message.value)

    def controller(self) -> ExecutionController:
        """Настоящий Execution Controller над `cluster`; новый вызов — рестарт Controller."""
        results = StoredResults(ObjectStorage(self._s3, self._s3, self._bucket))
        return ExecutionController(self.cluster, _Events(self), results, WORKLOAD)

    def run_spark(self, command: SubmitExecution) -> None:
        """Spark Operator запустил попытку: driver работает."""
        self.cluster.set_state(workload_name(command.attempt_id), "RUNNING")
        self.cluster.set_pods(command.attempt_id, Pod(phase="Running"))

    def complete_spark(self, command: SubmitExecution) -> None:
        """Блок записал выходы и ExecutionResult, driver завершился."""
        result = self.execute(command)
        key = self._key(f"{command.payload.context.output_prefix}/{RESULT_FILE}")
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=result.model_dump_json().encode())
        self.cluster.set_state(workload_name(command.attempt_id), "COMPLETED")
        self.cluster.set_pods(command.attempt_id, Pod(phase="Succeeded"))

    def execute(self, command: SubmitExecution) -> ExecutionResult:
        """CSV на `source` → Parquet на `data`, как `Read`; Parquet → копия на `result`."""
        context = command.payload.context
        source = context.inputs["source"]
        if source.format == "CSV":
            port, manifest_key = "data", self._read(source, f"{context.output_prefix}/data")
        else:
            port, manifest_key = "result", self._copy(source, f"{context.output_prefix}/result")
        return ExecutionResult(
            node_run_id=context.node_run_id,
            attempt_id=context.attempt_id,
            outputs={port: f"s3://{self._bucket}/{manifest_key}"},
        )

    def _read(self, source: ResolvedInput, output: str) -> str:
        columns = self._read_csv(source)
        return write_parquet_output(self._s3, self._bucket, self._key(output), columns)

    def _copy(self, source: ResolvedInput, output: str) -> str:
        """Копирует файлы и manifest входа под `output`; возвращает ключ manifest."""
        manifest_key = self._key(source.manifest_uri)
        manifest = self._get(manifest_key)
        directory = manifest_key.rpartition("/")[0]
        prefix = self._key(output)
        for file in Manifest.model_validate_json(manifest).files:
            data = self._get(f"{directory}/{file.path}")
            self._s3.put_object(
                Bucket=self._bucket,
                Key=f"{prefix}/{file.path}",
                Body=data,
                ChecksumSHA256=file.checksum_sha256,
            )
        self._s3.put_object(Bucket=self._bucket, Key=f"{prefix}/manifest.json", Body=manifest)
        return f"{prefix}/manifest.json"

    async def succeed(self, command: SubmitExecution) -> ExecutionSucceeded:
        """Пишет данные, затем шлёт `Started` и `Succeeded`."""
        result = self.execute(command)
        await self.send(started(command))
        return await self.send(succeeded(command, result))

    async def fail(
        self, command: SubmitExecution, error: str, reason: FailureReason = "SPARK_FAILED"
    ) -> ExecutionFailed:
        """Падает до записи данных."""
        return await self.send(failed(command, error, reason))

    async def send[E: ExecutionEvent](self, event: E) -> E:
        """Шлёт событие; повторный вызов с тем же событием — повторная доставка."""
        await self._producer.send_and_wait(EVENTS_TOPIC, event.to_bytes(), key=event.key())
        return event

    def _read_csv(self, source: ResolvedInput) -> dict[str, list[str]]:
        manifest_key = self._key(source.manifest_uri)
        manifest = Manifest.model_validate_json(self._get(manifest_key))
        directory = manifest_key.rpartition("/")[0]
        text = self._get(f"{directory}/{manifest.files[0].path}").decode("utf-8-sig")
        header, *body = list(csv.reader(io.StringIO(text)))
        return {name: [row[i] for row in body] for i, name in enumerate(header)}

    def _get(self, key: str) -> bytes:
        return self._s3.get_object(Bucket=self._bucket, Key=key)["Body"].read()

    def _key(self, uri: str) -> str:
        return uri.removeprefix(f"s3://{self._bucket}/")


class _Events:
    """События настоящего Controller уходят в `events.v1` через Fake."""

    def __init__(self, fake: FakeExecutionController) -> None:
        self._fake = fake

    async def send(self, event: ExecutionEvent) -> None:
        await self._fake.send(event)


def write_parquet_output(
    s3: S3Client, bucket: str, prefix: str, columns: dict[str, list[str]]
) -> str:
    """Пишет настоящий Parquet и manifest v1 под `prefix`; возвращает ключ manifest."""
    data = _parquet(columns)
    checksum = base64.b64encode(hashlib.sha256(data).digest()).decode()
    s3.put_object(Bucket=bucket, Key=f"{prefix}/{DATA_FILE}", Body=data, ChecksumSHA256=checksum)
    manifest = Manifest(
        format="PARQUET",
        data_schema=DataSchema(columns=[Column(name=name, type="string") for name in columns]),
        files=[ManifestFile(path=DATA_FILE, size_bytes=len(data), checksum_sha256=checksum)],
        size_bytes=len(data),
        row_count=len(next(iter(columns.values()), list[str]())),
    )
    manifest_key = f"{prefix}/manifest.json"
    s3.put_object(Bucket=bucket, Key=manifest_key, Body=manifest.model_dump_json(indent=2).encode())
    return manifest_key


def _parquet(columns: dict[str, list[str]]) -> bytes:
    # У pyarrow нет типов для pyright.
    arrays: dict[str, Any] = {
        name: pa.array(values, pa.string()) for name, values in columns.items()
    }
    table: Any = pa.table(arrays)  # pyright: ignore[reportUnknownVariableType]
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()
