"""REST API v1 Run Orchestrator."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field

from e3.identity import Principal, ProjectAccessDenied, authorize_project, current_principal
from e3.pipelines import PipelineNotFound
from e3.runs.bindings import InputBinding, OutputBinding, RunRequest, RunRequestInvalid
from e3.runs.nodes import NodeKind
from e3.runs.orchestrator import (
    IdempotencyConflict,
    NodeRun,
    PipelineRun,
    PipelineRunNotFound,
    RunOrchestrator,
)
from e3.runs.progress import NodeState, RunState
from e3.shared.manifest import DataFormat, DataSchema

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


class NodeOutputOut(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)

    port: str
    state: Literal["READY"] = "READY"
    manifest_uri: str
    format: DataFormat
    data_schema: DataSchema = Field(serialization_alias="schema")


class NodeRunOut(BaseModel):
    node_run_id: UUID
    node_id: str
    kind: NodeKind
    state: NodeState
    attempt_id: UUID | None
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
    return _run_out(await _accessible_run(orchestrator, principal, pipeline_run_id))


@router.get("/{pipeline_run_id}/nodes")
async def list_nodes(
    pipeline_run_id: UUID, orchestrator: OrchestratorDep, principal: PrincipalDep
) -> list[NodeRunOut]:
    await _accessible_run(orchestrator, principal, pipeline_run_id)
    return [_node_out(node) for node in await orchestrator.list_nodes(pipeline_run_id)]


async def _accessible_run(
    orchestrator: RunOrchestrator, principal: Principal, pipeline_run_id: UUID
) -> PipelineRun:
    try:
        run = await orchestrator.get_run(pipeline_run_id)
    except PipelineRunNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "PipelineRun не найден") from None
    try:
        authorize_project(principal, run.project_id)
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
    )


def _node_out(node: NodeRun) -> NodeRunOut:
    return NodeRunOut(
        node_run_id=node.node_run_id,
        node_id=node.node_id,
        kind=node.kind,
        state=node.state,
        attempt_id=node.attempt_id,
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
