"""REST API v1 Run Orchestrator."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field

from n3.identity import Principal, ProjectAccessDenied, Role, authorize, current_principal
from n3.pipelines import PipelineArchived, PipelineNotFound
from n3.runs.attempts import AttemptReason, AttemptState
from n3.runs.bindings import InputBinding, OutputBinding, RunRequest, RunRequestInvalid
from n3.runs.nodes import NodeKind
from n3.runs.orchestrator import (
    Attempt,
    IdempotencyConflict,
    NodeRun,
    PipelineRun,
    PipelineRunNotFound,
    RunNotCancellable,
    RunOrchestrator,
)
from n3.runs.progress import NodeState, RunState
from n3.shared.manifest import DataFormat, DataSchema

router = APIRouter(prefix="/api/v1/pipeline-runs", tags=["pipeline-runs"])


class PipelineRunOut(BaseModel):
    pipeline_run_id: UUID
    status_url: str
    state: RunState
    pipeline_id: UUID
    pipeline_version: int
    inputs: dict[str, InputBinding]
    outputs: dict[str, OutputBinding]
    created_at: datetime
    # Initiator: его Membership перепроверяется перед каждой Publication.
    initiated_by_user_id: UUID
    # Первопричина Fail-fast: первый упавший NodeRun.
    failed_node_run_id: UUID | None


class NodeOutputOut(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)

    port: str
    state: Literal["READY"] = "READY"
    manifest_uri: str
    format: DataFormat
    data_schema: DataSchema = Field(serialization_alias="schema")


class AttemptOut(BaseModel):
    number: int
    attempt_id: UUID
    state: AttemptState
    reason: AttemptReason | None
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class NodeRunOut(BaseModel):
    node_run_id: UUID
    node_id: str
    kind: NodeKind
    state: NodeState
    attempt_id: UUID | None
    attempts: list[AttemptOut]
    outputs: list[NodeOutputOut]
    published_version_id: UUID | None
    error: str | None


def get_orchestrator(request: Request) -> RunOrchestrator:
    orchestrator: RunOrchestrator = request.app.state.runs
    return orchestrator


OrchestratorDep = Annotated[RunOrchestrator, Depends(get_orchestrator)]
PrincipalDep = Annotated[Principal, Depends(current_principal)]


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def start_run(
    body: RunRequest,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=200)],
    orchestrator: OrchestratorDep,
    principal: PrincipalDep,
) -> PipelineRunOut:
    try:
        run = await orchestrator.start(principal, body, idempotency_key=idempotency_key)
    except PipelineNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Pipeline не найден") from None
    except PipelineArchived:
        raise HTTPException(status.HTTP_409_CONFLICT, "Pipeline в архиве") from None
    except ProjectAccessDenied:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "нет доступа к Project") from None
    except RunRequestInvalid as invalid:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(invalid)) from None
    except IdempotencyConflict:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Idempotency-Key уже использован с другим телом"
        ) from None
    return _run_out(run)


@router.get("/{pipeline_run_id}")
async def get_run(
    pipeline_run_id: UUID, orchestrator: OrchestratorDep, principal: PrincipalDep
) -> PipelineRunOut:
    return _run_out(await _accessible_run(orchestrator, principal, pipeline_run_id, Role.VIEWER))


@router.post("/{pipeline_run_id}/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_run(
    pipeline_run_id: UUID, orchestrator: OrchestratorDep, principal: PrincipalDep
) -> PipelineRunOut:
    """Запрос принят, а не Spark остановлен: CANCELLED наступит после доказанной остановки."""
    await _accessible_run(orchestrator, principal, pipeline_run_id, Role.EDITOR)
    try:
        run = await orchestrator.cancel(pipeline_run_id)
    except RunNotCancellable:
        raise HTTPException(status.HTTP_409_CONFLICT, "PipelineRun уже завершён") from None
    return _run_out(run)


@router.get("/{pipeline_run_id}/nodes")
async def list_nodes(
    pipeline_run_id: UUID, orchestrator: OrchestratorDep, principal: PrincipalDep
) -> list[NodeRunOut]:
    await _accessible_run(orchestrator, principal, pipeline_run_id, Role.VIEWER)
    return [_node_out(node) for node in await orchestrator.list_nodes(pipeline_run_id)]


async def _accessible_run(
    orchestrator: RunOrchestrator, principal: Principal, pipeline_run_id: UUID, minimum: Role
) -> PipelineRun:
    try:
        run = await orchestrator.get_run(pipeline_run_id)
    except PipelineRunNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "PipelineRun не найден") from None
    try:
        authorize(principal, run.project_id, minimum)
    except ProjectAccessDenied:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "нет доступа к Project") from None
    return run


def _run_out(run: PipelineRun) -> PipelineRunOut:
    return PipelineRunOut(
        pipeline_run_id=run.pipeline_run_id,
        status_url=f"{router.prefix}/{run.pipeline_run_id}",
        state=run.state,
        pipeline_id=run.pipeline_id,
        pipeline_version=run.pipeline_version,
        inputs=run.inputs,
        outputs=run.outputs,
        created_at=run.created_at,
        initiated_by_user_id=run.initiated_by_user_id,
        failed_node_run_id=run.failed_node_run_id,
    )


def _node_out(node: NodeRun) -> NodeRunOut:
    return NodeRunOut(
        node_run_id=node.node_run_id,
        node_id=node.node_id,
        kind=node.kind,
        state=node.state,
        attempt_id=node.attempt_id,
        attempts=[_attempt_out(attempt) for attempt in node.attempts],
        outputs=[
            NodeOutputOut(
                port=output.port,
                manifest_uri=output.manifest_uri,
                format=output.format,
                data_schema=output.data_schema,
            )
            for output in node.outputs
        ],
        published_version_id=node.published_version_id,
        error=node.error,
    )


def _attempt_out(attempt: Attempt) -> AttemptOut:
    return AttemptOut(
        number=attempt.number,
        attempt_id=attempt.attempt_id,
        state=attempt.state,
        reason=attempt.reason,
        error=attempt.error,
        created_at=attempt.created_at,
        started_at=attempt.started_at,
        finished_at=attempt.finished_at,
    )
