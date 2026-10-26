import asyncio
from uuid import UUID, uuid4

import pytest
from mypy_boto3_s3 import S3Client

from n3.settings import Settings
from n3.shared.storage import ObjectStorage, StorageUnavailable
from tests.integration.conftest import Api, client_for, object_exists, storage_like

pytestmark = pytest.mark.integration

CSV = b"id,name\n1,apple\n"


def staging(upload_id: UUID) -> str:
    return f"staging/uploads/{upload_id}/source.csv"


async def test_bad_header_is_rejected_and_rejection_is_remembered(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    data = b"id,name,id\n1,a,2\n"
    upload = await api.start_upload(dataset_id, data)
    await api.put_file(upload, data)

    first = await api.complete(dataset_id, upload.upload_id)
    again = await api.complete(dataset_id, upload.upload_id)

    assert first.status_code == 422
    assert first.json()["state"] == "REJECTED"
    assert first.json()["error"] == "повторяются имена колонок: id"
    assert again.json() == first.json()
    assert await api.versions(dataset_id) == []


async def test_object_with_other_checksum_is_rejected(
    api: Api, s3: S3Client, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)
    s3.put_object(Bucket=settings.s3_bucket, Key=staging(upload.upload_id), Body=b"id\nforged\n")

    response = await api.complete(dataset_id, upload.upload_id)

    assert response.status_code == 422
    assert "контрольная сумма" in response.json()["error"]


async def test_complete_before_file_is_uploaded_can_be_retried(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)

    early = await api.complete(dataset_id, upload.upload_id)
    await api.put_file(upload, CSV)
    later = await api.complete(dataset_id, upload.upload_id)

    assert early.status_code == 409
    assert later.json()["state"] == "COMPLETED"


async def test_repeated_complete_returns_the_same_version(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)
    await api.put_file(upload, CSV)

    first = await api.complete(dataset_id, upload.upload_id)
    again = await api.complete(dataset_id, upload.upload_id)

    assert again.status_code == 200
    assert again.json() == first.json()
    assert len(await api.versions(dataset_id)) == 1


async def test_parallel_complete_creates_exactly_one_version(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)
    await api.put_file(upload, CSV)

    responses = await asyncio.gather(
        *(api.complete(dataset_id, upload.upload_id) for _ in range(5))
    )

    assert {r.json()["state"] for r in responses} <= {"COMPLETED", "COMPLETING"}
    versions = await api.versions(dataset_id)
    assert [v["version_number"] for v in versions] == [1]


class ProcessDied(Exception):
    """Имитирует смерть процесса: код Upload это исключение не обрабатывает."""


class DiesBeforeCommit(ObjectStorage):
    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        await super().put(key, data, content_type=content_type)
        raise ProcessDied(key)


class SlowCopy(ObjectStorage):
    release: asyncio.Event

    async def copy(self, source_key: str, target_key: str) -> None:
        await self.release.wait()
        await super().copy(source_key, target_key)


class CopyFailsOnce(ObjectStorage):
    failed = False

    async def copy(self, source_key: str, target_key: str) -> None:
        if not self.failed:
            self.failed = True
            raise StorageUnavailable("503 SlowDown")
        await super().copy(source_key, target_key)


async def test_complete_after_crash_between_manifest_and_commit_finishes_once(
    api: Api, s3: S3Client, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)
    await api.put_file(upload, CSV)
    short_lease = settings.model_copy(update={"upload_lease_seconds": 0.5})
    async with client_for(short_lease, storage_like(DiesBeforeCommit, settings)) as dying:
        with pytest.raises(ProcessDied):
            await Api(dying, settings.dev_project_id).complete(dataset_id, upload.upload_id)

    while_leased = await api.complete(dataset_id, upload.upload_id)
    await asyncio.sleep(0.6)
    resumed = await api.complete(dataset_id, upload.upload_id)

    assert (while_leased.status_code, while_leased.json()["state"]) == (202, "COMPLETING")
    assert resumed.json()["state"] == "COMPLETED"
    assert [v["version_number"] for v in await api.versions(dataset_id)] == [1]
    listed = s3.list_objects_v2(Bucket=settings.s3_bucket, Prefix=f"datasets/{dataset_id}/")
    assert len({item.get("Key", "").split("/")[2] for item in listed.get("Contents", [])}) == 1


async def test_slow_processing_keeps_its_lease_and_blocks_parallel_complete(
    api: Api, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)
    await api.put_file(upload, CSV)
    slow = storage_like(SlowCopy, settings)
    slow.release = asyncio.Event()
    short_lease = settings.model_copy(update={"upload_lease_seconds": 0.3})

    async with client_for(short_lease, slow) as client:
        slow_api = Api(client, settings.dev_project_id)
        first = asyncio.create_task(slow_api.complete(dataset_id, upload.upload_id))
        await asyncio.sleep(0.9)
        parallel = await api.complete(dataset_id, upload.upload_id)
        slow.release.set()
        finished = await first

    assert parallel.status_code == 202
    assert finished.json()["state"] == "COMPLETED"
    assert [v["version_number"] for v in await api.versions(dataset_id)] == [1]


async def test_malformed_checksum_is_refused_before_upload_starts(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")

    response = await api.client.post(
        f"/api/v1/datasets/{dataset_id}/uploads", json={"checksum_sha256": "!" * 44}
    )

    assert response.status_code == 422


async def test_staging_object_is_kept_until_version_is_committed(
    api: Api, s3: S3Client, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)
    await api.put_file(upload, CSV)

    async with client_for(settings, storage_like(DiesBeforeCommit, settings)) as dying:
        with pytest.raises(ProcessDied):
            await Api(dying, settings.dev_project_id).complete(dataset_id, upload.upload_id)

    assert object_exists(s3, settings, staging(upload.upload_id))


async def test_temporary_storage_failure_does_not_reject_upload(
    api: Api, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)
    await api.put_file(upload, CSV)
    flaky = storage_like(CopyFailsOnce, settings)

    async with client_for(settings, flaky) as client:
        flaky_api = Api(client, settings.dev_project_id)
        failed = await flaky_api.complete(dataset_id, upload.upload_id)
        retried = await flaky_api.complete(dataset_id, upload.upload_id)

    assert failed.status_code == 503
    assert retried.json()["state"] == "COMPLETED"
