"""Архивирование Dataset и Pipeline: из обращения уходят, уже созданные запуски работают."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from n3.api.app import create_app
from n3.settings import Settings
from tests.integration.conftest import (
    RUN_CSV,
    Api,
    Plant,
    pipeline_with_defaults,
    run_request,
    uploaded_version,
)

pytestmark = pytest.mark.integration

LOCK_WAIT_POLLS = 100


async def test_archiving_dataset_twice_succeeds(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")

    first = await api.archive_dataset(dataset_id)
    second = await api.archive_dataset(dataset_id)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert second.json()["archived"] is True


async def test_archiving_unknown_dataset_is_not_found(api: Api) -> None:
    response = await api.archive_dataset(uuid4())

    assert response.status_code == 404


async def test_upload_into_archived_dataset_is_conflict(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    await api.archive_dataset(dataset_id)

    response = await api.client.post(
        f"/api/v1/datasets/{dataset_id}/uploads", json={"checksum_sha256": "A" * 43 + "="}
    )

    assert response.status_code == 409, response.text


async def test_completing_upload_after_archiving_is_conflict(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, RUN_CSV)
    await api.put_file(upload, RUN_CSV)
    await api.archive_dataset(dataset_id)

    response = await api.complete(dataset_id, upload.upload_id)

    assert response.status_code == 409, response.text
    assert await api.versions(dataset_id) == []


async def test_archived_dataset_keeps_its_versions_readable(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    await api.upload_csv(dataset_id, RUN_CSV)

    await api.archive_dataset(dataset_id)

    assert len(await api.versions(dataset_id)) == 1


@pytest.mark.parametrize("slot", ["inputs", "outputs"])
async def test_run_with_archived_dataset_is_rejected(api: Api, slot: str) -> None:
    body = await run_request(api)
    await api.archive_dataset(next(iter(body[slot].values()))["dataset_id"])

    response = await api.start_run(body, key=str(uuid4()))

    assert response.status_code == 422, response.text
    assert "в архиве" in response.json()["detail"]


async def test_archiving_pipeline_twice_succeeds(api: Api) -> None:
    pipeline_id = await api.new_pipeline(f"pipeline-{uuid4()}")

    first = await api.archive_pipeline(pipeline_id)
    second = await api.archive_pipeline(pipeline_id)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert second.json()["archived"] is True


async def test_archived_pipeline_draft_is_frozen(api: Api) -> None:
    pipeline_id = await api.new_pipeline(f"pipeline-{uuid4()}")
    await api.archive_pipeline(pipeline_id)

    saved = await api.save_draft(pipeline_id, {})
    published = await api.publish(pipeline_id)

    assert saved.status_code == 409, saved.text
    assert published.status_code == 409, published.text


async def test_run_of_archived_pipeline_is_conflict(api: Api) -> None:
    body = await run_request(api)
    await api.archive_pipeline(body["pipeline_id"])

    response = await api.start_run(body, key=str(uuid4()))

    assert response.status_code == 409, response.text


async def test_archived_pipeline_versions_stay_readable(api: Api) -> None:
    body = await run_request(api)
    await api.archive_pipeline(body["pipeline_id"])

    response = await api.pipeline_version(UUID(body["pipeline_id"]), body["pipeline_version"])

    assert response.status_code == 200, response.text


async def test_run_created_before_archiving_still_publishes(plant: Plant) -> None:
    body = await run_request(plant.api)
    started = await plant.api.start_run(body, key=str(uuid4()))
    run_id = started.json()["pipeline_run_id"]
    target = body["outputs"]["result"]["dataset_id"]
    await plant.api.archive_dataset(target)
    await plant.api.archive_pipeline(body["pipeline_id"])

    await plant.execute(run_id, "read")
    await plant.orchestrator.publish_pending()

    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"
    assert len(await plant.api.versions(UUID(target))) == 1


async def backend_pid(connection: AsyncConnection | AsyncSession) -> int:
    pid = await connection.scalar(text("SELECT pg_backend_pid()"))
    assert pid is not None
    return pid


async def wait_until_blocked(engine: AsyncEngine, holder: int, task: asyncio.Future[Any]) -> None:
    """Ждёт, пока `task` встанет в очередь за блокировкой `holder` или закончится."""
    query = text("SELECT count(*) FROM pg_stat_activity WHERE :holder = ANY(pg_blocking_pids(pid))")
    for _ in range(LOCK_WAIT_POLLS):
        async with engine.connect() as probe:
            if task.done() or await probe.scalar(query, {"holder": holder}):
                return
        await asyncio.sleep(0.05)
    raise TimeoutError("запрос не встал за блокировкой и не ответил")


async def start_during_archiving(
    api: Api, settings: Settings, body: dict[str, Any], table: str, row_id: str
) -> httpx.Response:
    """Создаёт запуск, пока архивация строки `row_id` идёт в открытой транзакции."""
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as archiving:
            await archiving.execute(
                text(f"UPDATE {table} SET archived = true WHERE id = :id"), {"id": row_id}
            )
            start = asyncio.create_task(api.start_run(body, key=str(uuid4())))
            await wait_until_blocked(engine, await backend_pid(archiving), start)
            await archiving.commit()
        return await start
    finally:
        await engine.dispose()


RunLock = Callable[[FastAPI, AsyncSession], Awaitable[object]]


async def archive_during_run_creation(
    settings: Settings, lock: RunLock, archive: Awaitable[httpx.Response]
) -> tuple[bool, httpx.Response]:
    """Архивирует, пока транзакция создания запуска держит блокировку `lock`.

    Возвращает, успела ли архивация закончиться до конца той транзакции, и её ответ.
    """
    engine = create_async_engine(settings.database_url)
    try:
        async with async_sessionmaker(engine).begin() as creating:
            await lock(create_app(settings), creating)
            archiving = asyncio.ensure_future(archive)
            await wait_until_blocked(engine, await backend_pid(creating), archiving)
            finished_early = archiving.done()
        return finished_early, await archiving
    finally:
        await engine.dispose()


@pytest.mark.parametrize("slot", ["inputs", "outputs"])
async def test_run_racing_dataset_archiving_is_rejected(
    api: Api, settings: Settings, slot: str
) -> None:
    body = await run_request(api)
    dataset_id = next(iter(body[slot].values()))["dataset_id"]

    response = await start_during_archiving(api, settings, body, "catalog.datasets", dataset_id)

    assert response.status_code == 422, response.text
    assert "в архиве" in response.json()["detail"]


async def test_run_racing_pipeline_archiving_is_conflict(api: Api, settings: Settings) -> None:
    body = await run_request(api)

    response = await start_during_archiving(
        api, settings, body, "pipelines.pipelines", body["pipeline_id"]
    )

    assert response.status_code == 409, response.text


async def test_dataset_archiving_waits_for_run_creation(api: Api, settings: Settings) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")

    async def lock(app: FastAPI, session: AsyncSession) -> object:
        return await app.state.catalog.lock_datasets(session, [dataset_id])

    finished_early, archived = await archive_during_run_creation(
        settings, lock, api.archive_dataset(dataset_id)
    )

    assert not finished_early
    assert archived.status_code == 200, archived.text


async def test_pipeline_archiving_waits_for_run_creation(api: Api, settings: Settings) -> None:
    pipeline_id = await api.new_pipeline(f"pipeline-{uuid4()}")

    async def lock(app: FastAPI, session: AsyncSession) -> object:
        return await app.state.pipelines.lock(session, pipeline_id)

    finished_early, archived = await archive_during_run_creation(
        settings, lock, api.archive_pipeline(pipeline_id)
    )

    assert not finished_early
    assert archived.status_code == 200, archived.text


async def test_repeated_run_after_archiving_returns_original(api: Api) -> None:
    source, target = await api.new_dataset(f"s-{uuid4()}"), await api.new_dataset(f"r-{uuid4()}")
    first_version = await uploaded_version(api, source)
    body = await pipeline_with_defaults(
        api, {"dataset_id": str(source), "version": "latest"}, {"dataset_id": str(target)}
    )
    key = str(uuid4())
    original = await api.start_run(body, key=key)
    await uploaded_version(api, source)
    await api.archive_dataset(source)
    await api.archive_dataset(target)
    await api.archive_pipeline(UUID(body["pipeline_id"]))

    repeated = await api.start_run(body, key=key)

    assert repeated.status_code == 202, repeated.text
    assert repeated.json()["pipeline_run_id"] == original.json()["pipeline_run_id"]
    assert repeated.json()["inputs"]["source"]["version_id"] == first_version
    assert repeated.json()["outputs"] == original.json()["outputs"]
