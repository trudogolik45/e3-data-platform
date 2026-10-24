"""Публичный фасад Dataset Catalog поверх сессий PostgreSQL и хранилища."""

from datetime import timedelta
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from e3.catalog import datasets, versions
from e3.catalog.datasets import Dataset
from e3.catalog.publications import Publications
from e3.catalog.uploads import StartedUpload, Uploads, UploadStatus
from e3.catalog.versions import DatasetVersion
from e3.shared.contracts import NodeOutputRef
from e3.shared.storage import ObjectStorage


class Catalog:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        storage: ObjectStorage,
        *,
        upload_lease: timedelta,
        upload_url_ttl_seconds: int,
    ) -> None:
        self._sessions = sessions
        self._uploads = Uploads(
            sessions, storage, lease=upload_lease, url_ttl_seconds=upload_url_ttl_seconds
        )
        self._publications = Publications(sessions, storage)

    async def create_dataset(self, *, project_id: UUID, name: str) -> Dataset:
        async with self._sessions.begin() as session:
            return await datasets.create_dataset(session, project_id=project_id, name=name)

    async def get_dataset(self, dataset_id: UUID) -> Dataset:
        async with self._sessions() as session:
            return await datasets.get_dataset(session, dataset_id)

    async def start_upload(self, dataset_id: UUID, *, checksum_sha256: str) -> StartedUpload:
        return await self._uploads.start(dataset_id, checksum_sha256=checksum_sha256)

    async def complete_upload(self, dataset_id: UUID, upload_id: UUID) -> UploadStatus:
        return await self._uploads.complete(dataset_id, upload_id)

    async def discard_staging(self, upload_id: UUID) -> None:
        await self._uploads.discard_staging(upload_id)

    async def list_versions(self, dataset_id: UUID) -> list[DatasetVersion]:
        async with self._sessions() as session:
            return await versions.list_versions(session, dataset_id)

    async def resolve(self, version_id: UUID) -> DatasetVersion:
        async with self._sessions() as session:
            return await versions.resolve(session, version_id)

    async def publish(
        self, *, project_id: UUID, dataset_id: UUID, source: NodeOutputRef, manifest_uri: str
    ) -> DatasetVersion:
        """Публикует NodeOutput как новую DatasetVersion; повтор отдаёт ту же версию."""
        return await self._publications.publish(
            project_id=project_id, dataset_id=dataset_id, source=source, manifest_uri=manifest_uri
        )
