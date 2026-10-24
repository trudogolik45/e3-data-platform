"""Fake Execution Controller: заменяет Spark в integration-тестах Control Plane.

Живёт только в `tests/`: код продукта его не импортирует (закреплено `lint-imports`).
Читает SubmitExecution из `commands.v1`, как блок `Read` превращает входной CSV
в настоящий Parquet с manifest v1 в `staging/executions/{attempt_id}/` и только
после записи отвечает событиями в `events.v1`. Тест сам решает, чем ответить:
успехом, падением до записи или повторной доставкой события.
"""

import asyncio
import base64
import csv
import hashlib
import io
from types import TracebackType
from typing import Any, Self, cast

import pyarrow as pa
import pyarrow.parquet as pq
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient
from mypy_boto3_s3 import S3Client

from e3.shared.contracts import ExecutionResult, ResolvedInput
from e3.shared.manifest import Column, DataSchema, Manifest, ManifestFile
from e3.shared.messages import (
    COMMANDS_TOPIC,
    EVENTS_TOPIC,
    ExecutionEvent,
    ExecutionFailed,
    ExecutionStarted,
    ExecutionSucceeded,
    Failure,
    SubmitExecution,
    parse_command,
)

DATA_FILE = "data/part-00000.parquet"
COMMAND_TIMEOUT_SECONDS = 15


class FakeExecutionController:
    def __init__(self, bootstrap_servers: str, s3: S3Client, bucket: str) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._s3 = s3
        self._bucket = bucket
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers, enable_auto_commit=False
        )
        self._producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers, acks="all")

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

    async def next_command(self) -> SubmitExecution:
        async with asyncio.timeout(COMMAND_TIMEOUT_SECONDS):
            message = cast("ConsumerRecord[bytes, bytes]", await self._consumer.getone())
        assert message.value is not None
        return parse_command(message.value)

    def execute(self, command: SubmitExecution) -> ExecutionResult:
        """Выполняет `Read`: CSV со входа `source` → Parquet и manifest на выходе `data`."""
        context = command.payload.context
        columns = self._read_csv(context.inputs["source"])
        manifest_key = write_parquet_output(
            self._s3, self._bucket, self._key(f"{context.output_prefix}/data"), columns
        )
        return ExecutionResult(
            node_run_id=context.node_run_id,
            attempt_id=context.attempt_id,
            outputs={"data": f"s3://{self._bucket}/{manifest_key}"},
        )

    async def succeed(self, command: SubmitExecution) -> ExecutionSucceeded:
        """Пишет данные, затем шлёт `Started` и `Succeeded`."""
        result = self.execute(command)
        await self.send(started(command))
        return await self.send(succeeded(command, result))

    async def fail(self, command: SubmitExecution, error: str) -> ExecutionFailed:
        """Падает до записи данных."""
        return await self.send(
            ExecutionFailed(
                node_run_id=command.node_run_id,
                attempt_id=command.attempt_id,
                trace_id=command.trace_id,
                payload=Failure(error=error),
            )
        )

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


def started(command: SubmitExecution) -> ExecutionStarted:
    return ExecutionStarted(
        node_run_id=command.node_run_id, attempt_id=command.attempt_id, trace_id=command.trace_id
    )


def succeeded(command: SubmitExecution, result: ExecutionResult) -> ExecutionSucceeded:
    return ExecutionSucceeded(
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
        payload=result,
    )


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
