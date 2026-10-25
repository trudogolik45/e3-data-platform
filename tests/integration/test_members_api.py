"""REST API Membership и вход через OIDC."""

from collections.abc import Mapping
from uuid import UUID, uuid4

import httpx
import pytest

from tests.integration.conftest import User, World

pytestmark = pytest.mark.integration


async def me(world: World, token: str) -> dict[str, object]:
    response = await (await world.client(token)).get("/api/v1/me")
    assert response.status_code == 200, response.text
    return response.json()


async def test_next_sign_in_with_another_email_is_the_same_user(world: World) -> None:
    subject = uuid4().hex
    first = await me(world, world.oidc.token(subject, email=f"old-{subject}@e3.test"))

    again = await me(world, world.oidc.token(subject, email=f"new-{subject}@e3.test"))

    assert again["user_id"] == first["user_id"]


async def test_me_lists_project_roles(world: World) -> None:
    admin = await world.user()
    project_id = await world.project(admin)

    assert (await me(world, admin.token))["projects"] == [
        {"project_id": str(project_id), "role": "Admin"}
    ]


async def add(
    world: World, project_id: UUID, admin: User, body: Mapping[str, object]
) -> httpx.Response:
    client = await world.client(admin.token)
    return await client.post(f"/api/v1/projects/{project_id}/members", json=body)


async def test_member_is_added_by_verified_email(world: World) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    email = f"{uuid4().hex}@e3.test"
    joined = await me(world, world.oidc.token(uuid4().hex, email=email))

    response = await add(world, project_id, admin, {"email": email, "role": "Editor"})

    assert response.status_code == 201, response.text
    assert response.json() == {"user_id": joined["user_id"], "email": email, "role": "Editor"}


async def test_unverified_email_finds_nobody(world: World) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    email = f"{uuid4().hex}@e3.test"
    await me(world, world.oidc.token(uuid4().hex, email=email, email_verified=False))

    response = await add(world, project_id, admin, {"email": email, "role": "Viewer"})

    assert response.status_code == 404


async def test_user_who_never_signed_in_cannot_be_added(world: World) -> None:
    admin = await world.user()
    project_id = await world.project(admin)

    response = await add(world, project_id, admin, {"user_id": str(uuid4()), "role": "Viewer"})

    assert response.status_code == 404


async def test_member_is_added_by_exactly_one_of_id_and_email(world: World) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    body = {"user_id": str(admin.user_id), "email": "x@e3.test", "role": "Viewer"}

    assert (await add(world, project_id, admin, body)).status_code == 422
    assert (await add(world, project_id, admin, {"role": "Viewer"})).status_code == 422


async def test_existing_member_is_a_conflict(world: World) -> None:
    admin = await world.user()
    project_id = await world.project(admin)

    response = await add(
        world, project_id, admin, {"user_id": str(admin.user_id), "role": "Viewer"}
    )

    assert response.status_code == 409


async def test_last_admin_cannot_leave_or_step_down(world: World) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    client = await world.client(admin.token)
    url = f"/api/v1/projects/{project_id}/members/{admin.user_id}"

    assert (await client.put(url, json={"role": "Editor"})).status_code == 409
    assert (await client.delete(url)).status_code == 409
    assert (await me(world, admin.token))["projects"] == [
        {"project_id": str(project_id), "role": "Admin"}
    ]


async def test_project_admin_must_have_signed_in(world: World) -> None:
    platform = await world.client("platform-team-secret")
    organization = await platform.post("/api/v1/organizations", json={"name": "org"})
    body = {
        "organization_id": organization.json()["organization_id"],
        "name": "p",
        "admin": {"email": f"{uuid4().hex}@e3.test"},
    }

    assert (await platform.post("/api/v1/projects", json=body)).status_code == 404
