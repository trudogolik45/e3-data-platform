"""Таблицы схемы `runs`. Снаружи модуля их не импортируют."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, MetaData, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    metadata = MetaData(schema="runs")
    type_annotation_map = {  # noqa: RUF012
        datetime: DateTime(timezone=True),
        dict[str, Any]: JSONB,
        list[str]: JSONB,
    }


class PipelineRunRow(Base):
    __tablename__ = "pipeline_runs"
    __table_args__ = (UniqueConstraint("project_id", "idempotency_key"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    project_id: Mapped[UUID]
    pipeline_id: Mapped[UUID]
    pipeline_version: Mapped[int]
    state: Mapped[str] = mapped_column(String(16))
    inputs: Mapped[dict[str, Any]]
    outputs: Mapped[dict[str, Any]]
    idempotency_key: Mapped[str]
    request_hash: Mapped[str]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class NodeRunRow(Base):
    __tablename__ = "node_runs"
    __table_args__ = (UniqueConstraint("pipeline_run_id", "node_id"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    pipeline_run_id: Mapped[UUID] = mapped_column(ForeignKey("pipeline_runs.id"))
    node_id: Mapped[str]
    kind: Mapped[str] = mapped_column(String(16))
    state: Mapped[str] = mapped_column(String(16))
    # port → DataRef: Input Slot уже разрешён в DatasetVersionRef, upstream — NodeOutputRef.
    inputs: Mapped[dict[str, Any]]
    parameters: Mapped[dict[str, Any]]
    # Только BLOCK: точный код и ожидаемые выходы из BlockSpec.
    executable_ref: Mapped[str | None]
    output_ports: Mapped[list[str]]
    # Только Publish Dataset: целевой Dataset из Output Slot.
    target_dataset_id: Mapped[UUID | None]
    current_attempt_id: Mapped[UUID | None]
    # ExecutionResult Current Attempt, ждущий проверки в FINALIZING.
    result: Mapped[dict[str, Any] | None]
    published_version_id: Mapped[UUID | None]
    error: Mapped[str | None]


class ExecutionAttemptRow(Base):
    __tablename__ = "execution_attempts"
    __table_args__ = (UniqueConstraint("node_run_id", "number"),)

    attempt_id: Mapped[UUID] = mapped_column(primary_key=True)
    node_run_id: Mapped[UUID] = mapped_column(ForeignKey("node_runs.id"))
    number: Mapped[int]
    state: Mapped[str] = mapped_column(String(16))
    reason: Mapped[str | None] = mapped_column(String(32))
    error: Mapped[str | None]
    # SubmitExecution попытки: из неё же собирается ReconcileExecution.
    command: Mapped[dict[str, Any]]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    started_at: Mapped[datetime | None]
    finished_at: Mapped[datetime | None]
    reconcile_requested_at: Mapped[datetime | None]
    reconcile_count: Mapped[int] = mapped_column(default=0)


class NodeOutputRow(Base):
    __tablename__ = "node_outputs"

    node_run_id: Mapped[UUID] = mapped_column(ForeignKey("node_runs.id"), primary_key=True)
    port: Mapped[str] = mapped_column(primary_key=True)
    manifest_uri: Mapped[str]
    format: Mapped[str] = mapped_column(String(16))
    data_schema: Mapped[dict[str, Any]]
    accepted_at: Mapped[datetime] = mapped_column(server_default=func.now())


class OutboxRow(Base):
    __tablename__ = "outbox"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    topic: Mapped[str]
    key: Mapped[str]
    payload: Mapped[dict[str, Any]]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    published_at: Mapped[datetime | None]
    claimed_until: Mapped[datetime | None]


class InboxRow(Base):
    __tablename__ = "inbox"

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    received_at: Mapped[datetime] = mapped_column(server_default=func.now())
