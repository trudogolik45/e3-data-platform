"""REST API v1 Dataset Catalog."""

import base64
import binascii
from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, Response, status
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from e3.catalog.catalog import Catalog
from e3.catalog.datasets import Dataset, DatasetNameTaken, DatasetNotFound
from e3.catalog.uploads import FileNotUploaded, UploadNotFound, UploadState, UploadStatus
from e3.catalog.versions import DatasetVersion
from e3.identity import Principal, ProjectAccessDenied, Role, authorize, current_principal
from e3.shared.manifest import DataFormat, DataSchema
from e3.shared.storage import StorageUnavailable

router = APIRouter(prefix="/api/v1/datasets", tags=["datasets"])

SHA256_BYTES = 32

COMPLETE_STATUS_CODES = {
    UploadState.COMPLETED: status.HTTP_200_OK,
    UploadState.COMPLETING: status.HTTP_202_ACCEPTED,
    UploadState.REJECTED: status.HTTP_422_UNPROCESSABLE_CONTENT,
}


class CreateDatasetIn(BaseModel):
    project_id: UUID
    name: str = Field(max_length=200)

    @field_validator("name")
    @classmethod
    def name_not_blank(cls, name: str) -> str:
        if not name.strip():
            raise ValueError("имя Dataset не может быть пустым")
        return name


class DatasetOut(BaseModel):
    dataset_id: UUID
    project_id: UUID
    name: str
    created_at: datetime
    archived: bool


class StartUploadIn(BaseModel):
    checksum_sha256: str = Field(description="SHA-256 файла в base64")

    @field_validator("checksum_sha256")
    @classmethod
    def is_base64_sha256(cls, checksum: str) -> str:
        try:
            digest = base64.b64decode(checksum, validate=True)
        except binascii.Error:
            digest = b""
        if len(digest) != SHA256_BYTES:
            raise ValueError("ожидается SHA-256 в base64")
        return checksum


class StartUploadOut(BaseModel):
    upload_id: UUID
    state: UploadState
    upload_url: str
    upload_headers: dict[str, str]


class DatasetVersionOut(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)

    version_id: UUID
    version_number: int
    format: DataFormat
    data_schema: DataSchema = Field(
        validation_alias=AliasChoices("schema", "data_schema"), serialization_alias="schema"
    )
    committed_at: datetime


class UploadStatusOut(BaseModel):
    upload_id: UUID
    state: UploadState
    version: DatasetVersionOut | None = None
    error: str | None = None


def get_catalog(request: Request) -> Catalog:
    catalog: Catalog = request.app.state.catalog
    return catalog


CatalogDep = Annotated[Catalog, Depends(get_catalog)]
PrincipalDep = Annotated[Principal, Depends(current_principal)]


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_dataset(
    body: CreateDatasetIn, catalog: CatalogDep, principal: PrincipalDep
) -> DatasetOut:
    _authorize(principal, body.project_id, Role.EDITOR)
    try:
        dataset = await catalog.create_dataset(project_id=body.project_id, name=body.name)
    except DatasetNameTaken:
        raise HTTPException(status.HTTP_409_CONFLICT, "Dataset с таким именем уже есть") from None
    return _dataset_out(dataset)


@router.post("/{dataset_id}/archive")
async def archive_dataset(
    dataset_id: UUID, catalog: CatalogDep, principal: PrincipalDep
) -> DatasetOut:
    await _accessible_dataset(catalog, principal, dataset_id, Role.EDITOR)
    return _dataset_out(await catalog.archive_dataset(dataset_id))


@router.post("/{dataset_id}/uploads", status_code=status.HTTP_201_CREATED)
async def start_upload(
    dataset_id: UUID, body: StartUploadIn, catalog: CatalogDep, principal: PrincipalDep
) -> StartUploadOut:
    await _uploadable_dataset(catalog, principal, dataset_id)
    started = await catalog.start_upload(dataset_id, checksum_sha256=body.checksum_sha256)
    return StartUploadOut(
        upload_id=started.upload_id,
        state=started.state,
        upload_url=started.presigned.url,
        upload_headers=started.presigned.headers,
    )


@router.post("/{dataset_id}/uploads/{upload_id}/complete")
async def complete_upload(
    dataset_id: UUID,
    upload_id: UUID,
    catalog: CatalogDep,
    principal: PrincipalDep,
    response: Response,
    background: BackgroundTasks,
) -> UploadStatusOut:
    await _uploadable_dataset(catalog, principal, dataset_id)
    try:
        upload = await catalog.complete_upload(dataset_id, upload_id)
    except UploadNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Upload не найден") from None
    except FileNotUploaded:
        raise HTTPException(status.HTTP_409_CONFLICT, "файл ещё не загружен") from None
    except StorageUnavailable:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "хранилище недоступно, повторите complete"
        ) from None
    if upload.state == UploadState.COMPLETED:
        background.add_task(catalog.discard_staging, upload_id)
    response.status_code = COMPLETE_STATUS_CODES.get(upload.state, status.HTTP_200_OK)
    return _upload_out(upload)


@router.get("/{dataset_id}/versions")
async def list_versions(
    dataset_id: UUID, catalog: CatalogDep, principal: PrincipalDep
) -> list[DatasetVersionOut]:
    await _accessible_dataset(catalog, principal, dataset_id, Role.VIEWER)
    return [_version_out(version) for version in await catalog.list_versions(dataset_id)]


async def _accessible_dataset(
    catalog: Catalog, principal: Principal, dataset_id: UUID, minimum: Role
) -> Dataset:
    try:
        dataset = await catalog.get_dataset(dataset_id)
    except DatasetNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Dataset не найден") from None
    _authorize(principal, dataset.project_id, minimum)
    return dataset


async def _uploadable_dataset(catalog: Catalog, principal: Principal, dataset_id: UUID) -> None:
    dataset = await _accessible_dataset(catalog, principal, dataset_id, Role.EDITOR)
    if dataset.archived:
        raise HTTPException(status.HTTP_409_CONFLICT, "Dataset в архиве")


def _authorize(principal: Principal, project_id: UUID, minimum: Role) -> None:
    try:
        authorize(principal, project_id, minimum)
    except ProjectAccessDenied:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "нет доступа к Project") from None


def _dataset_out(dataset: Dataset) -> DatasetOut:
    return DatasetOut(
        dataset_id=dataset.dataset_id,
        project_id=dataset.project_id,
        name=dataset.name,
        created_at=dataset.created_at,
        archived=dataset.archived,
    )


def _upload_out(upload: UploadStatus) -> UploadStatusOut:
    version = _version_out(upload.version) if upload.version else None
    return UploadStatusOut(
        upload_id=upload.upload_id, state=upload.state, version=version, error=upload.error
    )


def _version_out(version: DatasetVersion) -> DatasetVersionOut:
    return DatasetVersionOut(
        version_id=version.version_id,
        version_number=version.version_number,
        format=version.format,
        data_schema=version.data_schema,
        committed_at=version.committed_at,
    )
