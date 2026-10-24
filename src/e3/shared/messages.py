"""Сообщения Kafka между Control Plane и Execution Plane (PRD §6).

У каждого сообщения общий envelope; `type` выбирает модель `payload`.
Ключ сообщения — `node_run_id`: события одного NodeRun идут по порядку.
"""

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from e3.shared.contracts import OCI_DIGEST_PATTERN, ExecutionContext, ExecutionResult

COMMANDS_TOPIC = "e3.execution.commands.v1"
EVENTS_TOPIC = "e3.execution.events.v1"


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

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()


class Submission(BaseModel):
    model_config = ConfigDict(frozen=True)

    executable_ref: str = Field(pattern=OCI_DIGEST_PATTERN, description="OCI image digest блока")
    context: ExecutionContext


class SubmitExecution(_Envelope):
    type: Literal["SubmitExecution"] = "SubmitExecution"
    payload: Submission


class Empty(BaseModel):
    model_config = ConfigDict(frozen=True)


class Failure(BaseModel):
    model_config = ConfigDict(frozen=True)

    error: str


class ExecutionStarted(_Envelope):
    type: Literal["ExecutionStarted"] = "ExecutionStarted"
    payload: Empty = Field(default_factory=Empty)


class ExecutionSucceeded(_Envelope):
    type: Literal["ExecutionSucceeded"] = "ExecutionSucceeded"
    payload: ExecutionResult


class ExecutionFailed(_Envelope):
    type: Literal["ExecutionFailed"] = "ExecutionFailed"
    payload: Failure


ExecutionEvent = Annotated[
    ExecutionStarted | ExecutionSucceeded | ExecutionFailed, Field(discriminator="type")
]

_events = TypeAdapter[ExecutionEvent](ExecutionEvent)


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


def failed(command: SubmitExecution, error: str) -> ExecutionFailed:
    return ExecutionFailed(
        node_run_id=command.node_run_id,
        attempt_id=command.attempt_id,
        trace_id=command.trace_id,
        payload=Failure(error=error),
    )


def parse_event(raw: bytes) -> ExecutionEvent:
    return _events.validate_json(raw)


def parse_command(raw: bytes) -> SubmitExecution:
    return SubmitExecution.model_validate_json(raw)
