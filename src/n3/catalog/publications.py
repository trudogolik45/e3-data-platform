"""Publication: NodeOutput становится DatasetVersion формата PARQUET (ADR-0002).

Файлы из manifest копируются в неизменяемое расположение версии, manifest
переносится байт в байт. `version_id` выводится из NodeOutput, поэтому
повтор после сбоя пишет в те же ключи, а уже зафиксированная публикация
возвращается как есть.
"""

import uuid
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from n3.catalog import datasets
from n3.catalog.uploads import manifest_key, version_prefix
from n3.catalog.versions import DatasetVersion, find_published, register
from n3.shared.contracts import NodeOutputRef
from n3.shared.manifest import Manifest, ManifestFile
from n3.shared.storage import ObjectStorage

PUBLICATION_NAMESPACE = UUID("6f0e8f5e-6c4b-4f5a-9a51-3d1f2c7a9b10")


class PublicationRejected(Exception):
    """NodeOutput нельзя опубликовать в этот Dataset: текст — причина."""


def publication_version_id(source: NodeOutputRef) -> UUID:
    return uuid.uuid5(PUBLICATION_NAMESPACE, f"{source.node_run_id}/{source.port}")


class Publications:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], storage: ObjectStorage) -> None:
        self._sessions = sessions
        self._storage = storage

    async def publish(
        self, *, project_id: UUID, dataset_id: UUID, source: NodeOutputRef, manifest_uri: str
    ) -> DatasetVersion:
        async with self._sessions() as session:
            published = await find_published(session, source)
            if published is not None:
                return published
            dataset = await datasets.get_dataset(session, dataset_id)
        if dataset.project_id != project_id:
            raise PublicationRejected("Dataset принадлежит другому Project")
        version_id = publication_version_id(source)
        manifest = await self._transfer(dataset_id, version_id, manifest_uri)
        async with self._sessions.begin() as session:
            return await register(
                session,
                dataset_id=dataset_id,
                version_id=version_id,
                format=manifest.format,
                data_schema=manifest.data_schema,
                manifest_uri=self._storage.uri(manifest_key(dataset_id, version_id)),
                source=source,
            )

    async def _transfer(self, dataset_id: UUID, version_id: UUID, manifest_uri: str) -> Manifest:
        source_key = self._storage.key(manifest_uri)
        raw = await self._storage.read(source_key)
        manifest = Manifest.model_validate_json(raw)
        if manifest.format != "PARQUET":
            raise PublicationRejected(f"публикуется только PARQUET, а не {manifest.format}")
        source_dir = source_key.rpartition("/")[0]
        target_dir = version_prefix(dataset_id, version_id)
        for file in manifest.files:
            await self._copy_file(file, f"{source_dir}/{file.path}", f"{target_dir}/{file.path}")
        await self._storage.put(
            manifest_key(dataset_id, version_id), raw, content_type="application/json"
        )
        return manifest

    async def _copy_file(self, file: ManifestFile, source_key: str, target_key: str) -> None:
        if file.path.startswith("/") or ".." in file.path.split("/"):
            raise PublicationRejected(f"{file.path} выходит за каталог manifest")
        await self._storage.copy(source_key, target_key)
        copied = await self._storage.head(target_key)
        if copied is None or copied.checksum_sha256 != file.checksum_sha256:
            raise PublicationRejected(f"{file.path} не совпадает с manifest")
