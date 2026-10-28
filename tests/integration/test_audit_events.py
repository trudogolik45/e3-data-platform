"""Audit Event: доступ к содержимому данных и создание или отмена PipelineRun.

Журнал читается только через SQL, поэтому и тест смотрит в `audit.events` напрямую.
"""

from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from e3.audit import AuditLog
from e3.settings import Settings
from tests.integration.conftest import (
    RUN_CSV,
    Api,
    Plant,
    User,
    World,
    drain,
    publish_pipeline,
    run_request,
    sha256_b64,
    uploaded_version,
)
from tests.integration.graphs import block_spec, join_graph, port

pytestmark = pytest.mark.integration


async def test_created_pipeline_run_leaves_audit_event(api: Api, settings: Settings) -> None:
    started = await api.start_run(await run_request(api), key=str(uuid4()))
    run_id = started.json()["pipeline_run_id"]

    events = await audit_events(settings, "pipeline_run.create", run_id)

    assert events == [
        {
            "outcome": "allowed",
            "user_id": settings.dev_user_id,
            "project_id": settings.dev_project_id,
            "trace_id": run_id,
        }
    ]


async def test_repeated_start_with_same_idempotency_key_leaves_one_audit_event(
    api: Api, settings: Settings
) -> None:
    request, key = await run_request(api), str(uuid4())
    first = await api.start_run(request, key=key)
    repeated = await api.start_run(request, key=key)

    run_id = repeated.json()["pipeline_run_id"]

    assert run_id == first.json()["pipeline_run_id"]
    assert len(await audit_events(settings, "pipeline_run.create", run_id)) == 1


async def test_rolled_back_start_leaves_no_audit_event(api: Api, settings: Settings) -> None:
    request = await run_request(api)
    await api.archive_pipeline(request["pipeline_id"])

    rejected = await api.start_run(request, key=str(uuid4()))

    assert rejected.status_code == 409, rejected.text
    assert await audit_events(settings, "pipeline_run.create") == []


async def test_cancelled_pipeline_run_leaves_one_audit_event(api: Api, settings: Settings) -> None:
    started = await api.start_run(await run_request(api), key=str(uuid4()))
    run_id = started.json()["pipeline_run_id"]

    cancelled = await api.cancel_run(run_id)
    await api.cancel_run(run_id)

    assert cancelled.status_code == 202, cancelled.text
    assert await audit_events(settings, "pipeline_run.cancel", run_id) == [
        {
            "outcome": "allowed",
            "user_id": settings.dev_user_id,
            "project_id": settings.dev_project_id,
            "trace_id": run_id,
        }
    ]


async def test_upload_that_became_dataset_version_leaves_one_audit_event(
    api: Api, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"source-{uuid4()}")
    upload = await api.start_upload(dataset_id, RUN_CSV)
    await api.put_file(upload, RUN_CSV)

    completed = await api.complete(dataset_id, upload.upload_id)
    await api.complete(dataset_id, upload.upload_id)

    version_id = completed.json()["version"]["version_id"]
    assert await audit_events(settings, "dataset.upload", version_id) == [
        {
            "outcome": "allowed",
            "user_id": settings.dev_user_id,
            "project_id": settings.dev_project_id,
            "trace_id": None,
        }
    ]


async def test_run_reads_input_and_publishes_once_despite_retried_attempt(plant: Plant) -> None:
    run_id = await plant.start()
    await plant.fake.fail(await plant.submitted(run_id), "executor lost")
    await plant.consume_attempts(run_id, 2)
    await plant.fake.succeed(await plant.submitted(run_id))
    await plant.consume_until(run_id, "read", "FINALIZING")

    await plant.finish()

    run = await plant.api.run(run_id)
    source = run["inputs"]["source"]["version_id"]
    published = (await plant.node(run_id, "publish"))["published_version_id"]
    by_initiator = {
        "outcome": "allowed",
        "user_id": plant.settings.dev_user_id,
        "project_id": plant.settings.dev_project_id,
        "trace_id": run_id,
    }
    assert await audit_events(plant.settings, "dataset_version.read", source) == [by_initiator]
    assert await audit_events(plant.settings, "dataset.publish", published) == [by_initiator]


async def test_node_with_two_dataset_version_inputs_leaves_read_event_per_input(
    plant: Plant,
) -> None:
    api = plant.api
    name = f"join-{uuid4().hex}"
    spec = block_spec(name, [port("left"), port("right")], [port("data")])
    assert (await api.register_block(spec)).status_code == 201
    pipeline_id, version_number = await publish_pipeline(api, join_graph(name))
    inputs: dict[str, dict[str, str]] = {}
    for side in ("left", "right"):
        dataset_id = await api.new_dataset(f"{side}-{uuid4()}")
        inputs[side] = {
            "dataset_id": str(dataset_id),
            "version_id": await uploaded_version(api, dataset_id),
        }
    target = await api.new_dataset(f"result-{uuid4()}")
    request = {
        "pipeline_id": pipeline_id,
        "pipeline_version": version_number,
        "inputs": inputs,
        "outputs": {"result": {"dataset_id": str(target)}},
    }
    run_id = (await api.start_run(request, key=str(uuid4()))).json()["pipeline_run_id"]

    await drain(plant.orchestrator.dispatch_pending)

    for side in ("left", "right"):
        read = await audit_events(
            plant.settings, "dataset_version.read", inputs[side]["version_id"]
        )
        assert [event["trace_id"] for event in read] == [run_id]


async def test_viewer_starting_run_is_denied_and_audited(
    world: World, oidc_settings: Settings
) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    viewer = await world.member(project_id, admin, "Viewer")
    request = await run_request(await world.api(admin, project_id))

    denied = await (await world.api(viewer, project_id)).start_run(request, key=str(uuid4()))

    assert denied.status_code == 403, denied.text
    events = await audit_events(
        oidc_settings, "pipeline_run.create", request["pipeline_id"], project_id=project_id
    )
    assert events == [denied_to(viewer, project_id)]


async def test_each_denied_cancel_by_outsider_is_audited(
    world: World, oidc_settings: Settings
) -> None:
    admin, outsider = await world.user(), await world.user()
    project_id = await world.project(admin)
    admin_api = await world.api(admin, project_id)
    started = await admin_api.start_run(await run_request(admin_api), key=str(uuid4()))
    run_id = started.json()["pipeline_run_id"]
    outsider_api = await world.api(outsider, project_id)

    first = await outsider_api.cancel_run(run_id)
    second = await outsider_api.cancel_run(run_id)

    assert (first.status_code, second.status_code) == (403, 403)
    events = await audit_events(oidc_settings, "pipeline_run.cancel", run_id, project_id=project_id)
    assert events == [denied_to(outsider, project_id)] * 2


async def test_viewer_uploading_is_denied_and_audited(
    world: World, oidc_settings: Settings
) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    viewer = await world.member(project_id, admin, "Viewer")
    dataset_id = await (await world.api(admin, project_id)).new_dataset(f"source-{uuid4()}")

    denied = await (await world.client(viewer.token)).post(
        f"/api/v1/datasets/{dataset_id}/uploads", json={"checksum_sha256": sha256_b64(RUN_CSV)}
    )

    assert denied.status_code == 403, denied.text
    events = await audit_events(oidc_settings, "dataset.upload", dataset_id, project_id=project_id)
    assert events == [denied_to(viewer, project_id)]


async def test_denial_is_still_403_when_audit_write_fails(
    world: World, oidc_settings: Settings
) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    viewer = await world.member(project_id, admin, "Viewer")
    request = await run_request(await world.api(admin, project_id))
    world.app.state.audit = AuditLog(unreachable_database())

    denied = await (await world.api(viewer, project_id)).start_run(request, key=str(uuid4()))

    assert denied.status_code == 403, denied.text


async def test_listing_dataset_versions_leaves_no_audit_event(api: Api, settings: Settings) -> None:
    dataset_id = await api.new_dataset(f"source-{uuid4()}")
    await uploaded_version(api, dataset_id)
    before = await audit_event_count(settings)

    await api.versions(dataset_id)

    assert await audit_event_count(settings) == before


def denied_to(user: User, project_id: UUID) -> dict[str, Any]:
    return {
        "outcome": "denied",
        "user_id": user.user_id,
        "project_id": project_id,
        "trace_id": None,
    }


def unreachable_database() -> async_sessionmaker[AsyncSession]:
    """PostgreSQL, до которого не достучаться: запись отказа падает с OSError."""
    engine = create_async_engine("postgresql+asyncpg://e3:e3@127.0.0.1:1/e3")
    return async_sessionmaker(engine, expire_on_commit=False)


async def audit_event_count(settings: Settings) -> int:
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as connection:
            count = await connection.scalar(
                text("SELECT count(*) FROM audit.events WHERE project_id = :project_id"),
                {"project_id": settings.dev_project_id},
            )
            return int(count or 0)
    finally:
        await engine.dispose()


async def audit_events(
    settings: Settings,
    action: str,
    object_id: str | UUID | None = None,
    *,
    project_id: UUID | None = None,
) -> list[Any]:
    """Audit Event действия в Project (по умолчанию dev-Project), в порядке записи."""
    query = (
        "SELECT outcome, user_id, project_id, trace_id FROM audit.events"
        " WHERE action = :action AND project_id = :project_id"
    )
    params: dict[str, Any] = {"action": action, "project_id": project_id or settings.dev_project_id}
    if object_id is not None:
        query += " AND object_id = :object_id"
        params["object_id"] = UUID(str(object_id))
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(text(query + " ORDER BY occurred_at"), params)
            return [dict(row) for row in rows.mappings()]
    finally:
        await engine.dispose()
