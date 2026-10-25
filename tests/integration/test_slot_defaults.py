"""Late Binding со значениями по умолчанию: слоты без привязки берутся из PipelineVersion."""

from uuid import UUID, uuid4

import pytest

from e3.api.app import create_app
from e3.catalog import Catalog
from e3.settings import Settings
from e3.shared.contracts import DatasetVersionRef
from tests.integration.conftest import (
    Api,
    Plant,
    World,
    pipeline_with_defaults,
    publish_defaults,
    uploaded_version,
)

pytestmark = pytest.mark.integration


async def test_run_takes_exact_version_and_target_from_defaults(api: Api) -> None:
    source, target = await api.new_dataset(f"s-{uuid4()}"), await api.new_dataset(f"r-{uuid4()}")
    version_id = await uploaded_version(api, source)
    body = await pipeline_with_defaults(
        api,
        {"dataset_id": str(source), "version_id": version_id},
        {"dataset_id": str(target)},
    )

    response = await api.start_run(body, key=str(uuid4()))

    assert response.status_code == 202, response.text
    assert response.json()["inputs"] == {
        "source": {"dataset_id": str(source), "version_id": version_id}
    }
    assert response.json()["outputs"] == {"result": {"dataset_id": str(target)}}


async def test_latest_version_is_fixed_when_run_is_created(plant: Plant) -> None:
    api = plant.api
    source, target = await api.new_dataset(f"s-{uuid4()}"), await api.new_dataset(f"r-{uuid4()}")
    await uploaded_version(api, source)
    latest = await uploaded_version(api, source)
    body = await pipeline_with_defaults(
        api, {"dataset_id": str(source), "version": "latest"}, {"dataset_id": str(target)}
    )
    started = (await api.start_run(body, key=str(uuid4()))).json()

    await uploaded_version(api, source)
    command = await plant.submitted(started["pipeline_run_id"])

    pinned = DatasetVersionRef(dataset_id=source, version_id=UUID(latest))
    assert started["inputs"]["source"]["version_id"] == latest
    assert command.payload.context.inputs["source"].source == pinned


async def test_latest_version_of_empty_dataset_is_rejected(api: Api) -> None:
    source, target = await api.new_dataset(f"s-{uuid4()}"), await api.new_dataset(f"r-{uuid4()}")
    body = await pipeline_with_defaults(
        api, {"dataset_id": str(source), "version": "latest"}, {"dataset_id": str(target)}
    )

    response = await api.start_run(body, key=str(uuid4()))

    assert response.status_code == 422, response.text
    assert "source" in response.json()["detail"]
    assert "нет версий" in response.json()["detail"]


async def test_request_binding_overrides_default(api: Api) -> None:
    default, chosen = await api.new_dataset(f"d-{uuid4()}"), await api.new_dataset(f"c-{uuid4()}")
    await uploaded_version(api, default)
    chosen_version = await uploaded_version(api, chosen)
    target = await api.new_dataset(f"r-{uuid4()}")
    body = await pipeline_with_defaults(
        api, {"dataset_id": str(default), "version": "latest"}, {"dataset_id": str(target)}
    )
    override = {"source": {"dataset_id": str(chosen), "version_id": chosen_version}}

    response = await api.start_run(body | {"inputs": override}, key=str(uuid4()))

    assert response.status_code == 202, response.text
    assert response.json()["inputs"] == override


async def test_explicit_input_from_readable_foreign_project_is_accepted(world: World) -> None:
    user, foreign_admin = await world.user(), await world.user()
    home_id, foreign_id = await world.project(user), await world.project(foreign_admin)
    foreign_api = await world.api(foreign_admin, foreign_id)
    foreign = await foreign_api.new_dataset(f"f-{uuid4()}")
    foreign_version = await uploaded_version(foreign_api, foreign)
    await world.grant(foreign_id, foreign_admin, user, "Viewer")
    api = await world.api(user, home_id)
    target = await api.new_dataset(f"r-{uuid4()}")
    body = await pipeline_with_defaults(api, None, {"dataset_id": str(target)})
    binding = {"source": {"dataset_id": str(foreign), "version_id": foreign_version}}

    response = await api.start_run(body | {"inputs": binding}, key=str(uuid4()))

    assert response.status_code == 202, response.text
    assert response.json()["inputs"] == binding


async def test_slot_without_binding_or_default_is_rejected(api: Api) -> None:
    target = await api.new_dataset(f"r-{uuid4()}")
    body = await pipeline_with_defaults(api, None, {"dataset_id": str(target)})

    response = await api.start_run(body, key=str(uuid4()))

    assert response.status_code == 422, response.text
    assert "source" in response.json()["detail"]


async def test_archived_default_dataset_is_rejected(api: Api) -> None:
    source, target = await api.new_dataset(f"s-{uuid4()}"), await api.new_dataset(f"r-{uuid4()}")
    await uploaded_version(api, source)
    body = await pipeline_with_defaults(
        api, {"dataset_id": str(source), "version": "latest"}, {"dataset_id": str(target)}
    )
    await api.archive_dataset(source)

    response = await api.start_run(body, key=str(uuid4()))

    assert response.status_code == 422, response.text
    assert "в архиве" in response.json()["detail"]


async def test_same_dataset_is_input_and_output_of_one_run(api: Api) -> None:
    dataset_id = await api.new_dataset(f"loop-{uuid4()}")
    await uploaded_version(api, dataset_id)
    body = await pipeline_with_defaults(
        api, {"dataset_id": str(dataset_id), "version": "latest"}, {"dataset_id": str(dataset_id)}
    )

    response = await api.start_run(body, key=str(uuid4()))

    assert response.status_code == 202, response.text


async def test_default_on_unknown_dataset_is_not_published(api: Api) -> None:
    _, published = await publish_defaults(api, None, {"dataset_id": str(uuid4())})

    assert published.status_code == 422, published.text
    assert "result" in published.json()["detail"]


async def test_default_version_from_another_dataset_is_not_published(api: Api) -> None:
    source, other = await api.new_dataset(f"s-{uuid4()}"), await api.new_dataset(f"o-{uuid4()}")
    foreign_version = await uploaded_version(api, other)

    _, published = await publish_defaults(
        api, {"dataset_id": str(source), "version_id": foreign_version}, None
    )

    assert published.status_code == 422, published.text
    assert "source" in published.json()["detail"]


async def test_default_on_dataset_of_another_project_is_not_published(
    api: Api, settings: Settings
) -> None:
    catalog: Catalog = create_app(settings).state.catalog
    foreign = await catalog.create_dataset(project_id=uuid4(), name=f"f-{uuid4()}")

    _, published = await publish_defaults(
        api, {"dataset_id": str(foreign.dataset_id), "version": "latest"}, None
    )

    assert published.status_code == 422, published.text
    assert "Project" in published.json()["detail"]
