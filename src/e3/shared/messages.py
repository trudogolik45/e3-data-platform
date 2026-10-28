"""Сообщения Kafka между Control Plane и Execution Plane (PRD §6).

У каждого сообщения общий envelope; `type` выбирает модель `payload`.
Ключ сообщения — `node_run_id`: события одного NodeRun идут по порядку.
`event_id` события попытки выводится из `attempt_id` и `type`: повторно
отправленное событие получает тот же id, и inbox получателя его отбрасывает.
`ReconcileExecution` и `CancelExecution` получают id из `attempt_id` и номера проверки.
"""

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from e3.shared.contracts import OCI_DIGEST_PATTERN, ExecutionContext, ExecutionResult
from e3.shared.tracing import propagation_headers

COMMANDS_TOPIC = "e3.execution.commands.v1"
EVENTS_TOPIC = "e3.execution.events.v1"

_EVENT_ID_NAMESPACE = UUID("6f1d3c52-8a4e-4c1b-9f0e-3d2a7b5c9e10")

# Заголовок Kafka-сообщения: `trace_id` envelope виден без разбора тела.
TRACE_HEADER = "trace_id"


def trace_headers(trace_id: str) -> list[tuple[str, bytes]]:
    """`trace_id` запуска и W3C `traceparent` спана, который шлёт сообщение."""
    return [(TRACE_HEADER, trace_id.encode()), *propagation_headers()]


class _Envelope(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    schema_version: Literal[1] = 1
    node_run_id: UUID
    attempt_id: UUID
    trace_id: str
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def key(self) -> bytes:
        return str(self.node_run_id).encode()

    def headers(self) -> list[tuple[str, bytes]]:
        return trace_headers(self.trace_id)

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class PodSize(BaseModel):
    """Заявка одного pod'а попытки, driver и executor одинаковы; размер задаёт Control Plane.

    Значения по умолчанию — размер до появления поля: так читаются сообщения старого формата.
    """

    model_config = ConfigDict(frozen=True)

    cpu_millicores: int = Field(default=1000, gt=0, description="CPU-запрос, тысячные ядра")
    memory_mib: int = Field(default=512, gt=0, description="Память Spark, MiB")


class Submission(BaseModel):
    model_config = ConfigDict(frozen=True)

    executable_ref: str = Field(pattern=OCI_DIGEST_PATTERN, description="OCI image digest блока")
    context: ExecutionContext
    pod: PodSize = Field(default_factory=PodSize)


class SubmitExecution(_Envelope):
    type: Literal["SubmitExecution"] = "SubmitExecution"
    payload: Submission


class ReconcileExecution(_Envelope):
    """Просьба проверить попытку, о которой давно нет событий; `payload` — исходная Submission."""

    type: Literal["ReconcileExecution"] = "ReconcileExecution"
    payload: Submission


class CancelExecution(_Envelope):
    """Просьба остановить попытку и доказать её смерть; `payload` — исходная Submission."""

    type: Literal["CancelExecution"] = "CancelExecution"
    payload: Submission


type Command = SubmitExecution | ReconcileExecution | CancelExecution

ExecutionCommand = Annotated[Command, Field(discriminator="type")]

_commands = TypeAdapter[ExecutionCommand](ExecutionCommand)


class Empty(BaseModel):
    model_config = ConfigDict(frozen=True)


# Причина провала, которую Execution Controller выводит из терминального состояния Kubernetes.
# `BLOCK_FAILED` — блок сам сообщил ошибку в `error.json`.
FailureReason = Literal["SPARK_FAILED", "SUBMISSION_FAILED", "BLOCK_FAILED", "NO_RESULT", "UNKNOWN"]


class DriverLogs(BaseModel):
    """Где искать логи driver попытки: URL нет, бэкенда логов нет."""

    model_config = ConfigDict(frozen=True)

    namespace: str
    spark_application: str
    attempt_id: UUID


class Failure(BaseModel):
    model_config = ConfigDict(frozen=True)

    error: str
    # Сообщения без причины, кода и логов пришли от Controller старого формата.
    reason: FailureReason = "UNKNOWN"
    code: str | None = Field(default=None, description="`code` из error.json блока")
    driver_logs: DriverLogs | None = None


class ExecutionStarted(_Envelope):
    type: Literal["ExecutionStarted"] = "ExecutionStarted"
    payload: Empty = Field(default_factory=Empty)


class ExecutionSucceeded(_Envelope):
    type: Literal["ExecutionSucceeded"] = "ExecutionSucceeded"
    payload: ExecutionResult


class ExecutionFailed(_Envelope):
    type: Literal["ExecutionFailed"] = "ExecutionFailed"
    payload: Failure


class ExecutionLost(_Envelope):
    """Попытка доказуемо мертва без итога: `SparkApplication` пропал, на его месте Attempt Fence."""

    type: Literal["ExecutionLost"] = "ExecutionLost"
    payload: Empty = Field(default_factory=Empty)


class ExecutionCancelled(_Envelope):
    """Попытка остановлена по CancelExecution и доказуемо мертва: на её месте Attempt Fence."""

    type: Literal["ExecutionCancelled"] = "ExecutionCancelled"
    payload: Empty = Field(default_factory=Empty)


ExecutionEvent = Annotated[
    ExecutionStarted | ExecutionSucceeded | ExecutionFailed | ExecutionLost | ExecutionCancelled,
    Field(discriminator="type"),
]

_events = TypeAdapter[ExecutionEvent](ExecutionEvent)


def _attempt_event_id(command: Command, event: type[_Envelope]) -> UUID:
    """Одно и то же событие попытки всегда получает один и тот же id."""
    return uuid5(_EVENT_ID_NAMESPACE, f"{command.attempt_id}/{event.__name__}")


def original_submission(command: ReconcileExecution | CancelExecution) -> SubmitExecution:
    """SubmitExecution проверяемой попытки: те же NodeRun, попытка и Submission."""
    return SubmitExecution(
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
        payload=command.payload,
    )


def reconcile_id(attempt_id: UUID, check: int) -> UUID:
    return uuid5(attempt_id, f"reconcile/{check}")


def reconcile(command: Command, check: int) -> ReconcileExecution:
    """Проверка номер `check` попытки: у каждой свой id, повтор той же — тот же."""
    return ReconcileExecution(
        event_id=reconcile_id(command.attempt_id, check),
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
        payload=command.payload,
    )


def cancel_id(attempt_id: UUID, check: int) -> UUID:
    return uuid5(attempt_id, f"cancel/{check}")


def cancel(command: Command, check: int) -> CancelExecution:
    """Отмена номер `check` попытки: у каждой свой id, повтор той же — тот же."""
    return CancelExecution(
        event_id=cancel_id(command.attempt_id, check),
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
        payload=command.payload,
    )


def started(command: Command) -> ExecutionStarted:
    return ExecutionStarted(
        event_id=_attempt_event_id(command, ExecutionStarted),
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
    )


def succeeded(command: Command, result: ExecutionResult) -> ExecutionSucceeded:
    return ExecutionSucceeded(
        event_id=_attempt_event_id(command, ExecutionSucceeded),
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
        payload=result,
    )


def failed(
    command: Command,
    error: str,
    reason: FailureReason,
    *,
    code: str | None = None,
    driver_logs: DriverLogs | None = None,
) -> ExecutionFailed:
    return ExecutionFailed(
        event_id=_attempt_event_id(command, ExecutionFailed),
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
        payload=Failure(error=error, reason=reason, code=code, driver_logs=driver_logs),
    )


def lost(command: Command) -> ExecutionLost:
    return ExecutionLost(
        event_id=_attempt_event_id(command, ExecutionLost),
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
    )


def cancelled(command: Command) -> ExecutionCancelled:
    return ExecutionCancelled(
        event_id=_attempt_event_id(command, ExecutionCancelled),
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
    )


def parse_event(raw: bytes) -> ExecutionEvent:
    return _events.validate_json(raw)


def parse_command(raw: bytes) -> Command:
    return _commands.validate_json(raw)
