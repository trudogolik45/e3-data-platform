"""Каждый эндпоинт: без токена или с битым — `401`, Role ниже нужной — `403`, иначе пропускает.

«Пропускает» значит: ответ не `401` и не `403` — дальше могут быть `409` или `422`,
это уже не про доступ.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from tests.integration.conftest import (
    PLATFORM_TOKEN,
    RUN_CSV,
    Api,
    User,
    World,
    run_request,
)

pytestmark = pytest.mark.integration

LADDER = ["Viewer", "Editor", "Admin"]
DENIED = {401, 403}


@dataclass
class Estate:
    """Project с участниками всех Role, посторонним и ресурсами, созданными Admin."""

    world: World
    project_id: UUID
    users: dict[str, User]  # Role → участник; "outsider" — без Membership
    api: Api
    dataset_id: UUID
    pipeline_id: str
    pipeline_version: int
    run_request: dict[str, Any]
    run_id: str


@pytest.fixture
async def estate(world: World) -> Estate:
    admin = await world.user()
    project_id = await world.project(admin)
    users = {"Admin": admin, "outsider": await world.user()}
    for role in ("Viewer", "Editor"):
        users[role] = await world.member(project_id, admin, role)
    api = await world.api(admin, project_id)
    request = await run_request(api)
    started = await api.start_run(request, key=str(uuid4()))
    assert started.status_code == 202, started.text
    return Estate(
        world,
        project_id,
        users,
        api,
        UUID(request["inputs"]["source"]["dataset_id"]),
        request["pipeline_id"],
        request["pipeline_version"],
        request,
        started.json()["pipeline_run_id"],
    )


Call = Callable[[httpx.AsyncClient, Estate], Awaitable[httpx.Response]]


async def throwaway_member(estate: Estate) -> User:
    return await estate.world.member(estate.project_id, estate.users["Admin"], "Viewer")


async def create_dataset(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    body = {"project_id": str(e.project_id), "name": f"d-{uuid4()}"}
    return await c.post("/api/v1/datasets", json=body)


async def archive_dataset(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.post(f"/api/v1/datasets/{await e.api.new_dataset(f'a-{uuid4()}')}/archive")


async def start_upload(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    body = {"checksum_sha256": "A" * 43 + "="}
    return await c.post(f"/api/v1/datasets/{e.dataset_id}/uploads", json=body)


async def complete_upload(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    upload = await e.api.start_upload(e.dataset_id, RUN_CSV)
    return await c.post(f"/api/v1/datasets/{e.dataset_id}/uploads/{upload.upload_id}/complete")


async def list_versions(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.get(f"/api/v1/datasets/{e.dataset_id}/versions")


async def create_pipeline(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    body = {"project_id": str(e.project_id), "name": f"p-{uuid4()}"}
    return await c.post("/api/v1/pipelines", json=body)


async def archive_pipeline(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.post(f"/api/v1/pipelines/{await e.api.new_pipeline(f'a-{uuid4()}')}/archive")


async def save_draft(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.put(f"/api/v1/pipelines/{e.pipeline_id}/draft", json={"nodes": []})


async def publish_draft(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.post(f"/api/v1/pipelines/{e.pipeline_id}/versions")


async def get_pipeline_version(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.get(f"/api/v1/pipelines/{e.pipeline_id}/versions/{e.pipeline_version}")


async def start_run(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    headers = {"Idempotency-Key": str(uuid4())}
    return await c.post("/api/v1/pipeline-runs", json=e.run_request, headers=headers)


async def get_run(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.get(f"/api/v1/pipeline-runs/{e.run_id}")


async def list_nodes(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.get(f"/api/v1/pipeline-runs/{e.run_id}/nodes")


async def cancel_run(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.post(f"/api/v1/pipeline-runs/{e.run_id}/cancel")


async def list_members(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    return await c.get(f"/api/v1/projects/{e.project_id}/members")


async def add_member(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    user = await e.world.user()
    body = {"user_id": str(user.user_id), "role": "Viewer"}
    return await c.post(f"/api/v1/projects/{e.project_id}/members", json=body)


async def change_role(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    user = await throwaway_member(e)
    url = f"/api/v1/projects/{e.project_id}/members/{user.user_id}"
    return await c.put(url, json={"role": "Editor"})


async def remove_member(c: httpx.AsyncClient, e: Estate) -> httpx.Response:
    user = await throwaway_member(e)
    return await c.delete(f"/api/v1/projects/{e.project_id}/members/{user.user_id}")


ENDPOINTS: dict[str, tuple[Call, str]] = {
    "POST /datasets": (create_dataset, "Editor"),
    "POST /datasets/{id}/archive": (archive_dataset, "Editor"),
    "POST /datasets/{id}/uploads": (start_upload, "Editor"),
    "POST /datasets/{id}/uploads/{id}/complete": (complete_upload, "Editor"),
    "GET /datasets/{id}/versions": (list_versions, "Viewer"),
    "POST /pipelines": (create_pipeline, "Editor"),
    "POST /pipelines/{id}/archive": (archive_pipeline, "Editor"),
    "PUT /pipelines/{id}/draft": (save_draft, "Editor"),
    "POST /pipelines/{id}/versions": (publish_draft, "Editor"),
    "GET /pipelines/{id}/versions/{n}": (get_pipeline_version, "Viewer"),
    "POST /pipeline-runs": (start_run, "Editor"),
    "GET /pipeline-runs/{id}": (get_run, "Viewer"),
    "GET /pipeline-runs/{id}/nodes": (list_nodes, "Viewer"),
    "POST /pipeline-runs/{id}/cancel": (cancel_run, "Editor"),
    "GET /projects/{id}/members": (list_members, "Viewer"),
    "POST /projects/{id}/members": (add_member, "Admin"),
    "PUT /projects/{id}/members/{user}": (change_role, "Admin"),
    "DELETE /projects/{id}/members/{user}": (remove_member, "Admin"),
}


@pytest.mark.parametrize("endpoint", ENDPOINTS)
async def test_endpoint_requires_its_role(estate: Estate, endpoint: str) -> None:
    call, minimum = ENDPOINTS[endpoint]
    expected = {"outsider": False} | {
        role: LADDER.index(role) >= LADDER.index(minimum) for role in LADDER
    }

    statuses: dict[str, int] = {}
    for who, user in estate.users.items():
        statuses[who] = (await call(await estate.world.client(user.token), estate)).status_code

    assert {who: status not in DENIED for who, status in statuses.items()} == expected
    assert all(status == 403 for status in statuses.values() if status in DENIED)


@pytest.mark.parametrize("endpoint", [*ENDPOINTS, "GET /blocks", "GET /me"])
@pytest.mark.parametrize("token", [None, "garbage"], ids=["no-token", "bad-token"])
async def test_endpoint_rejects_missing_or_bad_token(
    estate: Estate, endpoint: str, token: str | None
) -> None:
    call = ENDPOINTS[endpoint][0] if endpoint in ENDPOINTS else None
    client = await estate.world.client(token)

    if call is not None:
        response = await call(client, estate)
    else:
        response = await client.get(f"/api/v1{endpoint.split(' ')[1]}")

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.parametrize("path", ["/api/v1/blocks", "/api/v1/me"])
async def test_any_signed_in_user_may_read_blocks_and_self(estate: Estate, path: str) -> None:
    client = await estate.world.client(estate.users["outsider"].token)

    assert (await client.get(path)).status_code == 200


async def test_platform_admin_api_needs_platform_token(estate: Estate) -> None:
    admin = await estate.world.client(estate.users["Admin"].token)
    anonymous = await estate.world.client(None)
    platform = await estate.world.client(PLATFORM_TOKEN)
    body = {"name": "org"}

    assert (await anonymous.post("/api/v1/organizations", json=body)).status_code == 401
    assert (await admin.post("/api/v1/organizations", json=body)).status_code == 403
    assert (await platform.post("/api/v1/organizations", json=body)).status_code == 201


async def test_platform_token_is_not_a_user_token(estate: Estate) -> None:
    platform = await estate.world.client(PLATFORM_TOKEN)

    assert (await platform.get(f"/api/v1/datasets/{estate.dataset_id}/versions")).status_code == 401
