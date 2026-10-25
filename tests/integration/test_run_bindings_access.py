"""Права при создании PipelineRun: Viewer на Project каждого входа, Editor на Project выхода."""

from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from tests.integration.conftest import RUN_CSV, Api, User, World, run_request

pytestmark = pytest.mark.integration


@dataclass
class TwoProjects:
    """`editor` — Editor в `home`, где живёт пайплайн; `foreign` — Project другого Admin."""

    world: World
    editor: User
    request: dict[str, Any]
    foreign_admin: User
    foreign_id: UUID
    foreign: Api


@pytest.fixture
async def two(world: World) -> TwoProjects:
    home_admin, foreign_admin = await world.user(), await world.user()
    home_id = await world.project(home_admin)
    foreign_id = await world.project(foreign_admin)
    editor = await world.member(home_id, home_admin, "Editor")
    request = await run_request(await world.api(home_admin, home_id))
    foreign = await world.api(foreign_admin, foreign_id)
    return TwoProjects(world, editor, request, foreign_admin, foreign_id, foreign)


async def foreign_input(two: TwoProjects) -> dict[str, Any]:
    dataset_id = await two.foreign.new_dataset(f"foreign-{uuid4()}")
    uploaded = await two.foreign.upload_csv(dataset_id, RUN_CSV)
    assert uploaded.status_code == 200, uploaded.text
    version_id = uploaded.json()["version"]["version_id"]
    return {
        **two.request,
        "inputs": {"source": {"dataset_id": str(dataset_id), "version_id": version_id}},
    }


async def start(two: TwoProjects, body: dict[str, Any]) -> httpx.Response:
    client = await two.world.client(two.editor.token)
    return await client.post(
        "/api/v1/pipeline-runs", json=body, headers={"Idempotency-Key": str(uuid4())}
    )


async def test_input_from_project_without_membership_is_forbidden(two: TwoProjects) -> None:
    response = await start(two, await foreign_input(two))

    assert response.status_code == 403


async def test_input_from_project_with_viewer_role_is_allowed(two: TwoProjects) -> None:
    body = await foreign_input(two)
    await two.world.grant(two.foreign_id, two.foreign_admin, two.editor, "Viewer")

    response = await start(two, body)

    assert response.status_code == 202, response.text


async def test_output_without_editor_role_in_its_project_is_forbidden(two: TwoProjects) -> None:
    target = await two.foreign.new_dataset(f"target-{uuid4()}")
    await two.world.grant(two.foreign_id, two.foreign_admin, two.editor, "Viewer")

    response = await start(two, {**two.request, "outputs": {"result": {"dataset_id": str(target)}}})

    assert response.status_code == 403


async def test_output_in_another_project_is_rejected_even_for_its_editor(
    two: TwoProjects,
) -> None:
    target = await two.foreign.new_dataset(f"target-{uuid4()}")
    await two.world.grant(two.foreign_id, two.foreign_admin, two.editor, "Editor")

    response = await start(two, {**two.request, "outputs": {"result": {"dataset_id": str(target)}}})

    assert response.status_code == 422
    assert "другого Project" in response.text
