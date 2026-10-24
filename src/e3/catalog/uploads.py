"""Upload: от presigned URL до DatasetVersion формата CSV.

Состояния: PENDING → COMPLETING → COMPLETED | REJECTED. В COMPLETING Upload
держит lease, который обработчик продлевает, пока работает: параллельный
`complete` не начинает вторую обработку. Если обработчик умер, после истечения
lease Upload захватывает следующий `complete` с новым `claim_token`, и старый
обработчик уже ничего не может записать. `version_id` назначается при первом
захвате и не меняется, поэтому продолжение пишет в те же ключи и не создаёт
второй объект.
"""

import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from e3.catalog.inspection import (
    HEADER_READ_LIMIT,
    StagedObject,
    UploadRejected,
    inspect_staged_object,
)
from e3.catalog.models import UploadRow
from e3.catalog.versions import DatasetVersion, register, resolve
from e3.shared.manifest import Column, DataSchema, Manifest, ManifestFile
from e3.shared.storage import (
    ObjectInfo,
    ObjectNotFound,
    ObjectStorage,
    PresignedPut,
    StorageUnavailable,
)

DATA_FILE = "data/source.csv"


class UploadState(StrEnum):
    PENDING = "PENDING"
    COMPLETING = "COMPLETING"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"


class UploadNotFound(Exception):
    """Upload с таким id нет в этом Dataset."""


class FileNotUploaded(Exception):
    """Файл ещё не загружен по presigned URL."""


@dataclass(frozen=True)
class StartedUpload:
    upload_id: UUID
    state: UploadState
    presigned: PresignedPut


@dataclass(frozen=True)
class UploadStatus:
    upload_id: UUID
    state: UploadState
    version: DatasetVersion | None = None
    error: str | None = None


@dataclass(frozen=True)
class _Claim:
    upload_id: UUID
    dataset_id: UUID
    version_id: UUID
    token: UUID
    declared_checksum_sha256: str


def staging_key(upload_id: UUID) -> str:
    return f"staging/uploads/{upload_id}/source.csv"


def version_prefix(dataset_id: UUID, version_id: UUID) -> str:
    return f"datasets/{dataset_id}/{version_id}"


def manifest_key(dataset_id: UUID, version_id: UUID) -> str:
    return f"{version_prefix(dataset_id, version_id)}/manifest.json"


class Uploads:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        storage: ObjectStorage,
        *,
        lease: timedelta,
        url_ttl_seconds: int,
    ) -> None:
        self._sessions = sessions
        self._storage = storage
        self._lease = lease
        self._url_ttl_seconds = url_ttl_seconds

    async def start(self, dataset_id: UUID, *, checksum_sha256: str) -> StartedUpload:
        upload_id = uuid4()
        async with self._sessions.begin() as session:
            session.add(
                UploadRow(
                    id=upload_id,
                    dataset_id=dataset_id,
                    state=UploadState.PENDING,
                    declared_checksum_sha256=checksum_sha256,
                )
            )
        presigned = self._storage.presign_put(
            staging_key(upload_id),
            checksum_sha256=checksum_sha256,
            ttl_seconds=self._url_ttl_seconds,
        )
        return StartedUpload(upload_id, UploadState.PENDING, presigned)

    async def complete(self, dataset_id: UUID, upload_id: UUID) -> UploadStatus:
        claim = await self._claim(dataset_id, upload_id)
        if isinstance(claim, UploadStatus):
            return claim
        try:
            manifest = await self._holding_lease(claim, self._materialize(claim))
        except UploadRejected as rejection:
            return await self._reject(claim, str(rejection))
        except (FileNotUploaded, ObjectNotFound, StorageUnavailable) as error:
            return await self._release(claim, error)
        return await self._commit(claim, manifest)

    async def discard_staging(self, upload_id: UUID) -> None:
        await self._storage.delete(staging_key(upload_id))

    async def _claim(self, dataset_id: UUID, upload_id: UUID) -> UploadStatus | _Claim:
        async with self._sessions.begin() as session:
            row = await _lock(session, dataset_id, upload_id)
            now = (await session.execute(select(func.now()))).scalar_one()
            if not _claimable(row, now):
                return await _status(session, row)
            claim = _Claim(
                upload_id=row.id,
                dataset_id=row.dataset_id,
                version_id=row.version_id or uuid4(),
                token=uuid4(),
                declared_checksum_sha256=row.declared_checksum_sha256,
            )
            row.state = UploadState.COMPLETING
            row.version_id = claim.version_id
            row.claim_token = claim.token
            row.lease_expires_at = now + self._lease
            return claim

    async def _holding_lease[T](self, claim: _Claim, work: Awaitable[T]) -> T:
        heartbeat = asyncio.create_task(self._keep_lease(claim))
        try:
            return await work
        finally:
            heartbeat.cancel()

    async def _keep_lease(self, claim: _Claim) -> None:
        while True:
            await asyncio.sleep(self._lease.total_seconds() / 3)
            async with self._sessions.begin() as session:
                await session.execute(
                    update(UploadRow)
                    .where(UploadRow.id == claim.upload_id, UploadRow.claim_token == claim.token)
                    .values(lease_expires_at=func.now() + self._lease)
                )

    async def _materialize(self, claim: _Claim) -> Manifest:
        source_key = staging_key(claim.upload_id)
        staged = await self._storage.head(source_key)
        if staged is None:
            raise FileNotUploaded(str(claim.upload_id))
        columns = await self._inspect(source_key, staged, claim.declared_checksum_sha256)

        prefix = version_prefix(claim.dataset_id, claim.version_id)
        data_key = f"{prefix}/{DATA_FILE}"
        await self._storage.copy(source_key, data_key)
        copied = await self._storage.head(data_key)
        if copied != staged:
            raise StorageUnavailable(f"копия {data_key} не совпадает с исходным объектом")

        manifest = _csv_manifest(columns, staged.size_bytes, claim.declared_checksum_sha256)
        await self._storage.put(
            manifest_key(claim.dataset_id, claim.version_id),
            manifest.model_dump_json(indent=2).encode(),
            content_type="application/json",
        )
        return manifest

    async def _inspect(self, key: str, staged: ObjectInfo, declared: str) -> list[Column]:
        head = await self._storage.read_prefix(key, HEADER_READ_LIMIT) if staged.size_bytes else b""
        obj = StagedObject(staged.size_bytes, staged.checksum_sha256, head)
        return inspect_staged_object(obj, declared_checksum_sha256=declared)

    async def _commit(self, claim: _Claim, manifest: Manifest) -> UploadStatus:
        async with self._sessions.begin() as session:
            row = await _lock(session, claim.dataset_id, claim.upload_id)
            if not _holds(row, claim):
                return await _status(session, row)
            version = await register(
                session,
                dataset_id=claim.dataset_id,
                version_id=claim.version_id,
                format=manifest.format,
                data_schema=manifest.data_schema,
                manifest_uri=self._storage.uri(manifest_key(claim.dataset_id, claim.version_id)),
                upload_id=claim.upload_id,
            )
            _finish(row, UploadState.COMPLETED)
            return UploadStatus(row.id, UploadState.COMPLETED, version=version)

    async def _reject(self, claim: _Claim, error: str) -> UploadStatus:
        async with self._sessions.begin() as session:
            row = await _lock(session, claim.dataset_id, claim.upload_id)
            if not _holds(row, claim):
                return await _status(session, row)
            _finish(row, UploadState.REJECTED)
            row.error = error
            return await _status(session, row)

    async def _release(self, claim: _Claim, error: Exception) -> UploadStatus:
        """Возвращает Upload в PENDING, чтобы `complete` можно было повторить.

        Если Upload уже завершил другой обработчик, отдаёт его результат.
        """
        async with self._sessions.begin() as session:
            row = await _lock(session, claim.dataset_id, claim.upload_id)
            if not _holds(row, claim):
                return await _status(session, row)
            _finish(row, UploadState.PENDING)
        if isinstance(error, ObjectNotFound):
            raise FileNotUploaded(str(claim.upload_id)) from error
        raise error


def _claimable(row: UploadRow, now: datetime) -> bool:
    if row.state == UploadState.PENDING:
        return True
    lease_expired = row.lease_expires_at is None or row.lease_expires_at <= now
    return row.state == UploadState.COMPLETING and lease_expired


def _holds(row: UploadRow, claim: _Claim) -> bool:
    return row.state == UploadState.COMPLETING and row.claim_token == claim.token


def _finish(row: UploadRow, state: UploadState) -> None:
    row.state = state
    row.claim_token = None
    row.lease_expires_at = None


async def _lock(session: AsyncSession, dataset_id: UUID, upload_id: UUID) -> UploadRow:
    row = await session.scalar(
        select(UploadRow)
        .where(UploadRow.id == upload_id, UploadRow.dataset_id == dataset_id)
        .with_for_update()
    )
    if row is None:
        raise UploadNotFound(str(upload_id))
    return row


async def _status(session: AsyncSession, row: UploadRow) -> UploadStatus:
    state = UploadState(row.state)
    version = None
    if state == UploadState.COMPLETED and row.version_id is not None:
        version = await resolve(session, row.version_id)
    return UploadStatus(row.id, state, version=version, error=row.error)


def _csv_manifest(columns: list[Column], size_bytes: int, checksum_sha256: str) -> Manifest:
    return Manifest(
        format="CSV",
        data_schema=DataSchema(columns=columns),
        files=[
            ManifestFile(path=DATA_FILE, size_bytes=size_bytes, checksum_sha256=checksum_sha256)
        ],
        size_bytes=size_bytes,
    )
