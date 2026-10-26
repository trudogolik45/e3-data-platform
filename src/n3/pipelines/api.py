"""REST API v1 Pipeline Service."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator

from n3.identity import Principal, ProjectAccessDenied, Role, authorize, current_principal
from n3.pipelines.graph import PipelineGraph
from n3.pipelines.service import (
    Pipeline,
    PipelineArchived,
    PipelineNotFound,
    PipelineService,
    PipelineVersion,
    PipelineVersionNotFound,
)
from n3.pipelines.validation import GraphInvalid

router = APIRouter(prefix="/api/v1/pipelines", tags=["pipelines"])


class CreatePipelineIn(BaseModel):
    project_id: UUID
    name: str = Field(max_length=200)

    @field_validator("name")
    @classmethod
    def name_not_blank(cls, name: str) -> str:
        if not name.strip():
            raise ValueError("имя Pipeline не может быть пустым")
        return name


class PipelineOut(BaseModel):
    pipeline_id: UUID
    project_id: UUID
    name: str
    created_at: datetime
    archived: bool


class PipelineVersionOut(BaseModel):
    version_id: UUID
    pipeline_id: UUID
    version_number: int
    graph: PipelineGraph
    published_at: datetime


def get_pipelines(request: Request) -> PipelineService:
    pipelines: PipelineService = request.app.state.pipelines
    return pipelines


PipelinesDep = Annotated[PipelineService, Depends(get_pipelines)]
PrincipalDep = Annotated[Principal, Depends(current_principal)]


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_pipeline(
    body: CreatePipelineIn, pipelines: PipelinesDep, principal: PrincipalDep
) -> PipelineOut:
    _authorize(principal, body.project_id, Role.EDITOR)
    return _pipeline_out(await pipelines.create(project_id=body.project_id, name=body.name))


@router.post("/{pipeline_id}/archive")
async def archive_pipeline(
    pipeline_id: UUID, pipelines: PipelinesDep, principal: PrincipalDep
) -> PipelineOut:
    await _accessible_pipeline(pipelines, principal, pipeline_id, Role.EDITOR)
    return _pipeline_out(await pipelines.archive(pipeline_id))


@router.put("/{pipeline_id}/draft")
async def save_draft(
    pipeline_id: UUID, graph: PipelineGraph, pipelines: PipelinesDep, principal: PrincipalDep
) -> PipelineGraph:
    await _accessible_pipeline(pipelines, principal, pipeline_id, Role.EDITOR)
    try:
        await pipelines.save_draft(pipeline_id, graph)
    except PipelineArchived:
        raise _archived() from None
    return graph


@router.post("/{pipeline_id}/versions", status_code=status.HTTP_201_CREATED)
async def publish_draft(
    pipeline_id: UUID, pipelines: PipelinesDep, principal: PrincipalDep
) -> PipelineVersionOut:
    await _accessible_pipeline(pipelines, principal, pipeline_id, Role.EDITOR)
    try:
        version = await pipelines.publish(pipeline_id)
    except PipelineArchived:
        raise _archived() from None
    except GraphInvalid as invalid:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(invalid)) from None
    return _version_out(version)


@router.get("/{pipeline_id}/versions/{version_number}")
async def get_version(
    pipeline_id: UUID, version_number: int, pipelines: PipelinesDep, principal: PrincipalDep
) -> PipelineVersionOut:
    await _accessible_pipeline(pipelines, principal, pipeline_id, Role.VIEWER)
    try:
        version = await pipelines.get_version(pipeline_id, version_number)
    except PipelineVersionNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "PipelineVersion не найдена") from None
    return _version_out(version)


async def _accessible_pipeline(
    pipelines: PipelineService, principal: Principal, pipeline_id: UUID, minimum: Role
) -> Pipeline:
    try:
        pipeline = await pipelines.get(pipeline_id)
    except PipelineNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Pipeline не найден") from None
    _authorize(principal, pipeline.project_id, minimum)
    return pipeline


def _authorize(principal: Principal, project_id: UUID, minimum: Role) -> None:
    try:
        authorize(principal, project_id, minimum)
    except ProjectAccessDenied:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "нет доступа к Project") from None


def _archived() -> HTTPException:
    return HTTPException(status.HTTP_409_CONFLICT, "Pipeline в архиве")


def _pipeline_out(pipeline: Pipeline) -> PipelineOut:
    return PipelineOut(
        pipeline_id=pipeline.pipeline_id,
        project_id=pipeline.project_id,
        name=pipeline.name,
        created_at=pipeline.created_at,
        archived=pipeline.archived,
    )


def _version_out(version: PipelineVersion) -> PipelineVersionOut:
    return PipelineVersionOut(
        version_id=version.version_id,
        pipeline_id=version.pipeline_id,
        version_number=version.version_number,
        graph=version.graph,
        published_at=version.published_at,
    )
