"""Срок хранения `staging/uploads/{upload_id}/`: 7 суток от создания Upload.

Время двигает тест: сдвигает назад `created_at` Upload в PostgreSQL.
"""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from mypy_boto3_s3 import S3Client
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from e3.api.app import create_app
from e3.catalog import Catalog
from e3.settings import Settings
from tests.integration.conftest import Api, object_exists

pytestmark = pytest.mark.integration

# Срок хранения загрузок по умолчанию.
RETENTION = timedelta(days=7)
CSV = b"id,name\n1,apple\n"


def staging(upload_id: UUID) -> str:
    return f"staging/uploads/{upload_id}/source.csv"


def catalog(settings: Settings) -> Catalog:
    return create_app(settings).state.catalog


async def execute(settings: Settings, sql: str, **params: object) -> None:
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(sql), params)
    finally:
        await engine.dispose()


async def created_ago(settings: Settings, upload_id: UUID, by: timedelta) -> None:
    """Сдвигает создание Upload назад, будто с него прошло `by`."""
    await execute(
        settings,
        "UPDATE catalog.uploads SET created_at = created_at - CAST(:by AS interval)"
        " WHERE id = :upload_id",
        by=by,
        upload_id=upload_id,
    )


async def uploaded(api: Api) -> tuple[UUID, UUID]:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)
    await api.put_file(upload, CSV)
    return dataset_id, upload.upload_id


async def test_upload_older_than_retention_is_purged(
    api: Api, s3: S3Client, settings: Settings
) -> None:
    _, upload_id = await uploaded(api)
    await created_ago(settings, upload_id, RETENTION + timedelta(hours=1))

    purged = await catalog(settings).purge_expired_uploads()

    assert purged >= 1
    assert not object_exists(s3, settings, staging(upload_id))


async def test_recent_upload_is_kept(api: Api, s3: S3Client, settings: Settings) -> None:
    _, upload_id = await uploaded(api)
    await created_ago(settings, upload_id, RETENTION - timedelta(hours=1))

    await catalog(settings).purge_expired_uploads()

    assert object_exists(s3, settings, staging(upload_id))


async def test_upload_with_active_lease_is_kept(api: Api, s3: S3Client, settings: Settings) -> None:
    _, upload_id = await uploaded(api)
    await execute(
        settings,
        "UPDATE catalog.uploads SET state = 'COMPLETING', claim_token = :token,"
        " lease_expires_at = now() + interval '1 hour' WHERE id = :upload_id",
        token=uuid4(),
        upload_id=upload_id,
    )
    await created_ago(settings, upload_id, RETENTION + timedelta(hours=1))

    await catalog(settings).purge_expired_uploads()

    assert object_exists(s3, settings, staging(upload_id))


async def test_upload_with_expired_lease_is_purged(
    api: Api, s3: S3Client, settings: Settings
) -> None:
    _, upload_id = await uploaded(api)
    await execute(
        settings,
        "UPDATE catalog.uploads SET state = 'COMPLETING', claim_token = :token,"
        " lease_expires_at = now() - interval '1 minute' WHERE id = :upload_id",
        token=uuid4(),
        upload_id=upload_id,
    )
    await created_ago(settings, upload_id, RETENTION + timedelta(hours=1))

    await catalog(settings).purge_expired_uploads()

    assert not object_exists(s3, settings, staging(upload_id))


async def test_purge_keeps_published_version(api: Api, s3: S3Client, settings: Settings) -> None:
    dataset_id, upload_id = await uploaded(api)
    completed = (await api.complete(dataset_id, upload_id)).json()
    s3.put_object(Bucket=settings.s3_bucket, Key=staging(upload_id), Body=CSV)
    await created_ago(settings, upload_id, RETENTION + timedelta(hours=1))

    await catalog(settings).purge_expired_uploads()

    version_id = completed["version"]["version_id"]
    prefix = f"datasets/{dataset_id}/{version_id}"
    assert object_exists(s3, settings, f"{prefix}/manifest.json")
    assert object_exists(s3, settings, f"{prefix}/data/source.csv")
    assert not object_exists(s3, settings, staging(upload_id))


async def test_purged_upload_cannot_be_completed(
    api: Api, s3: S3Client, settings: Settings
) -> None:
    dataset_id, upload_id = await uploaded(api)
    await created_ago(settings, upload_id, RETENTION + timedelta(hours=1))
    await catalog(settings).purge_expired_uploads()
    s3.put_object(Bucket=settings.s3_bucket, Key=staging(upload_id), Body=CSV)

    response = await api.complete(dataset_id, upload_id)

    assert response.status_code == 409
    assert response.json()["detail"] == "срок загрузки истёк, начните новую"
    assert await api.versions(dataset_id) == []


async def test_repeated_purge_skips_already_purged(api: Api, settings: Settings) -> None:
    _, upload_id = await uploaded(api)
    await created_ago(settings, upload_id, RETENTION + timedelta(hours=1))
    await catalog(settings).purge_expired_uploads()

    again = await catalog(settings).purge_expired_uploads()

    assert again == 0
