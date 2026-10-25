"""Архивирование Dataset и Pipeline: из обращения уходят, уже созданные запуски работают."""

from uuid import UUID, uuid4

import pytest

from tests.integration.conftest import RUN_CSV, Api, Plant, run_request

pytestmark = pytest.mark.integration


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
