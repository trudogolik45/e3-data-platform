"""Срок хранения staging: данные попыток удаляются через 7 суток после завершения PipelineRun.

Время двигает тест: сдвигает назад `finished_at` запуска в PostgreSQL.
"""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from mypy_boto3_s3 import S3Client
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from e3.api.app import create_app, create_storage
from e3.settings import Settings
from e3.shared.manifest import Manifest
from e3.shared.messages import SubmitExecution
from e3.shared.storage import ObjectStorage, StorageUnavailable
from tests.integration.conftest import Plant, object_exists, read_object, storage_like
from tests.logs import TraceLogs

pytestmark = pytest.mark.integration

# Срок хранения staging по умолчанию (PRD §8).
RETENTION = timedelta(days=7)


def put(s3: S3Client, settings: Settings, *keys: str) -> None:
    for key in keys:
        s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=b"x")


async def test_delete_prefix_removes_every_object_under_it_and_nothing_beside(
    s3: S3Client, settings: Settings
) -> None:
    root = f"staging/executions/{uuid4()}"
    inside = [f"{root}/data/manifest.json", f"{root}/data/part-0.parquet"]
    beside = f"{root}-other/data/manifest.json"
    put(s3, settings, *inside, beside)

    deleted = await create_storage(settings).delete_prefix(f"{root}/")

    assert deleted == len(inside)
    assert not any(object_exists(s3, settings, key) for key in inside)
    assert object_exists(s3, settings, beside)


async def test_delete_prefix_of_nothing_deletes_nothing(settings: Settings) -> None:
    deleted = await create_storage(settings).delete_prefix(f"staging/executions/{uuid4()}/")

    assert deleted == 0


async def finished_ago(plant: Plant, run_id: str, by: timedelta) -> None:
    """Сдвигает завершение запуска назад, будто с него прошло `by`."""
    engine = create_async_engine(plant.settings.database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE runs.pipeline_runs"
                    " SET finished_at = finished_at - CAST(:by AS interval)"
                    " WHERE id = :run_id"
                ),
                {"by": by, "run_id": UUID(run_id)},
            )
    finally:
        await engine.dispose()


def attempt_keys(plant: Plant, command: SubmitExecution) -> list[str]:
    prefix = f"staging/executions/{command.attempt_id}/"
    found = plant.s3.list_objects_v2(Bucket=plant.settings.s3_bucket, Prefix=prefix)
    return [item["Key"] for item in found.get("Contents", []) if "Key" in item]


async def published_after_retry(plant: Plant) -> tuple[str, list[SubmitExecution]]:
    """`Read → Publish Dataset`: первая попытка `read` упала, оставив файлы, вторая опубликована."""
    run_id = await plant.start()
    failed = await plant.submitted(run_id)
    put(plant.s3, plant.settings, f"staging/executions/{failed.attempt_id}/data/part-0")
    await plant.fake.fail(failed, "executor lost", "SPARK_FAILED")
    await plant.consume_attempts(run_id, 2)
    succeeded = await plant.execute(run_id, "read")
    await plant.finish()
    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"
    return run_id, [failed, succeeded]


async def test_staging_of_every_attempt_is_purged_after_retention(plant: Plant) -> None:
    run_id, attempts = await published_after_retry(plant)
    assert all(attempt_keys(plant, attempt) for attempt in attempts)
    await finished_ago(plant, run_id, RETENTION + timedelta(hours=1))

    purged = await plant.orchestrator.purge_staging()

    assert purged == 1
    assert [attempt_keys(plant, attempt) for attempt in attempts] == [[], []]


def published_files(plant: Plant, dataset_id: str, version_id: str) -> list[str]:
    directory = f"datasets/{dataset_id}/{version_id}"
    uri = f"s3://{plant.settings.s3_bucket}/{directory}/manifest.json"
    manifest = Manifest.model_validate_json(read_object(plant.s3, plant.settings, uri))
    return [f"{directory}/{file.path}" for file in manifest.files]


async def test_purge_keeps_published_version_and_uploads(plant: Plant) -> None:
    run_id, _ = await published_after_retry(plant)
    upload = f"staging/uploads/{uuid4()}/data.csv"
    put(plant.s3, plant.settings, upload)
    await finished_ago(plant, run_id, RETENTION + timedelta(hours=1))

    await plant.orchestrator.purge_staging()

    target = (await plant.api.run(run_id))["outputs"]["result"]["dataset_id"]
    [version] = await plant.api.versions(UUID(target))
    files = published_files(plant, target, str(version["version_id"]))
    assert files
    assert all(object_exists(plant.s3, plant.settings, key) for key in files)
    assert object_exists(plant.s3, plant.settings, upload)


async def test_purge_keeps_staging_of_recent_and_active_runs(plant: Plant) -> None:
    recent_id, recent_attempts = await published_after_retry(plant)
    await finished_ago(plant, recent_id, RETENTION - timedelta(hours=1))
    active_id = await plant.start()
    active = await plant.execute(active_id, "read")

    await plant.orchestrator.purge_staging()

    assert all(attempt_keys(plant, attempt) for attempt in [*recent_attempts, active])
    assert (await plant.api.run(active_id))["state"] == "RUNNING"


class CountingStorage(ObjectStorage):
    prefixes: list[str]

    async def delete_prefix(self, prefix: str) -> int:
        self.prefixes.append(prefix)
        return await super().delete_prefix(prefix)


async def test_repeated_purge_touches_nothing_already_purged(plant: Plant) -> None:
    run_id, _ = await published_after_retry(plant)
    await finished_ago(plant, run_id, RETENTION + timedelta(hours=1))
    await plant.orchestrator.purge_staging()
    storage = storage_like(CountingStorage, plant.settings)
    storage.prefixes = []

    again = await create_app(plant.settings, storage=storage).state.runs.purge_staging()

    assert (again, storage.prefixes) == (0, [])


class FailingOnce(ObjectStorage):
    """Первое удаление префикса `broken` падает, остальные идут в MinIO."""

    broken: str

    async def delete_prefix(self, prefix: str) -> int:
        if prefix == self.broken:
            self.broken = ""
            raise StorageUnavailable("MinIO недоступен")
        return await super().delete_prefix(prefix)


async def test_failed_attempt_purge_spares_others_and_is_retried(plant: Plant) -> None:
    run_id, [failed, succeeded] = await published_after_retry(plant)
    await finished_ago(plant, run_id, RETENTION + timedelta(hours=1))
    storage = storage_like(FailingOnce, plant.settings)
    storage.broken = f"staging/executions/{failed.attempt_id}/"
    orchestrator = create_app(plant.settings, storage=storage).state.runs

    first = await orchestrator.purge_staging()
    left = attempt_keys(plant, failed), attempt_keys(plant, succeeded)
    await orchestrator.purge_staging()

    assert first == 0
    assert left[0] and not left[1]
    assert attempt_keys(plant, failed) == []


async def test_purge_is_logged_with_trace_id_of_its_run(
    plant: Plant, trace_logs: TraceLogs
) -> None:
    run_id, [failed, _] = await published_after_retry(plant)
    await finished_ago(plant, run_id, RETENTION + timedelta(hours=1))
    storage = storage_like(FailingOnce, plant.settings)
    storage.broken = f"staging/executions/{failed.attempt_id}/"
    orchestrator = create_app(plant.settings, storage=storage).state.runs

    await orchestrator.purge_staging()
    await orchestrator.purge_staging()

    assert [m for m in trace_logs(run_id) if "staging" in m] == [
        f"staging попытки {failed.attempt_id} не удалён, повторится",
        f"staging PipelineRun {run_id} удалён",
    ]
