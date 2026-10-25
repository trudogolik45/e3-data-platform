"""Перед Publication Run Orchestrator перепроверяет текущий Membership Initiator (ADR-0004)."""

from uuid import UUID, uuid4

import pytest

from tests.integration.conftest import OidcPlant, User, run_request

pytestmark = pytest.mark.integration


async def start_as(stand: OidcPlant, user: User) -> str:
    request = await run_request(stand.plant.api)
    api = await stand.world.api(user, stand.project_id)
    response = await api.start_run(request, key=str(uuid4()))
    assert response.status_code == 202, response.text
    return response.json()["pipeline_run_id"]


async def run_until_publication(stand: OidcPlant, user: User) -> str:
    """Запуск от имени `user`, у которого Read уже отработал: дальше только Publication."""
    plant = stand.plant
    run_id = await start_as(stand, user)
    await plant.fake.succeed(await plant.submitted(run_id))
    await plant.consume_until(run_id, "read", "FINALIZING")
    return run_id


async def test_run_remembers_initiator_and_publishes_while_access_holds(
    oidc_plant: OidcPlant,
) -> None:
    stand = oidc_plant
    editor = await stand.world.member(stand.project_id, stand.admin, "Editor")
    run_id = await run_until_publication(stand, editor)

    await stand.plant.finish()

    run = await stand.plant.api.run(run_id)
    assert run["initiated_by_user_id"] == str(editor.user_id)
    assert run["state"] == "SUCCEEDED"


@pytest.mark.parametrize("revoke", ["demote", "remove"])
async def test_publication_fails_when_initiator_lost_write_access(
    oidc_plant: OidcPlant, revoke: str
) -> None:
    stand, plant = oidc_plant, oidc_plant.plant
    editor = await stand.world.member(stand.project_id, stand.admin, "Editor")
    run_id = await run_until_publication(stand, editor)
    if revoke == "demote":
        await stand.world.set_role(stand.project_id, stand.admin, editor, "Viewer")
    else:
        admin_client = await stand.world.client(stand.admin.token)
        removed = await admin_client.delete(
            f"/api/v1/projects/{stand.project_id}/members/{editor.user_id}"
        )
        assert removed.status_code == 204, removed.text

    await plant.finish()

    publish = await plant.node(run_id, "publish")
    assert publish["state"] == "FAILED"
    assert publish["error"].startswith("access_revoked")
    assert publish["published_version_id"] is None
    run = await plant.api.run(run_id)
    assert run["state"] == "FAILED"
    assert run["failed_node_run_id"] == publish["node_run_id"]
    assert await plant.api.versions(UUID(run["outputs"]["result"]["dataset_id"])) == []


async def test_revoked_publication_is_not_retried(oidc_plant: OidcPlant) -> None:
    stand, plant = oidc_plant, oidc_plant.plant
    editor = await stand.world.member(stand.project_id, stand.admin, "Editor")
    run_id = await run_until_publication(stand, editor)
    await stand.world.set_role(stand.project_id, stand.admin, editor, "Viewer")
    await plant.finish()
    await stand.world.set_role(stand.project_id, stand.admin, editor, "Editor")

    await plant.finish()

    assert await plant.state(run_id, "publish") == "FAILED"
    assert (await plant.api.run(run_id))["state"] == "FAILED"
