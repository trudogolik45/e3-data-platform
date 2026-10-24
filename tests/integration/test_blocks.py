from typing import Any
from uuid import uuid4

import pytest

from e3.settings import Settings
from tests.integration.conftest import Api, client_for

pytestmark = pytest.mark.integration

DIGEST = "registry.local/e3/read@sha256:" + "b" * 64


def block_spec(**overrides: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "name": f"read-{uuid4().hex}",
        "version": "1.0.0",
        "runtime": "spark",
        "inputs": [],
        "outputs": [{"name": "data", "data_type": "dataset", "required": True}],
        "parameters_schema": {"type": "object"},
        "executable_ref": DIGEST,
    }
    return spec | overrides


async def test_platform_team_registers_block(api: Api) -> None:
    spec = block_spec()

    response = await api.register_block(spec)

    assert response.status_code == 201, response.text
    assert response.json() == spec


@pytest.mark.parametrize(
    ("token", "status_code"),
    [(None, 401), ("wrong-token", 403), ("user-token-from-dev-login", 403)],
)
async def test_block_registration_without_platform_token_is_refused(
    api: Api, token: str | None, status_code: int
) -> None:
    spec = block_spec()

    response = await api.register_block(spec, token=token)

    assert response.status_code == status_code
    assert spec["name"] not in await registered_names(api)


async def test_block_registration_is_impossible_without_configured_token(
    settings: Settings,
) -> None:
    unconfigured = settings.model_copy(update={"platform_token": None})
    async with client_for(unconfigured) as client:
        api = Api(client, settings.dev_project_id)

        response = await api.register_block(block_spec())
        empty_bearer = await client.post(
            "/api/v1/blocks", json=block_spec(), headers={"Authorization": "Bearer "}
        )

    assert response.status_code in (401, 403)
    assert empty_bearer.status_code in (401, 403)


@pytest.mark.parametrize("ref", ["registry.local/e3/read:latest", "registry.local/e3/read:3.5"])
async def test_block_with_tag_instead_of_digest_is_invalid(api: Api, ref: str) -> None:
    response = await api.register_block(block_spec(executable_ref=ref))

    assert response.status_code == 422


async def test_repeating_same_block_spec_is_idempotent(api: Api) -> None:
    spec = block_spec()
    await api.register_block(spec)

    response = await api.register_block(spec)

    assert response.status_code == 200, response.text
    assert (await registered_names(api)).count(spec["name"]) == 1


async def test_same_name_and_version_with_other_body_conflicts(api: Api) -> None:
    spec = block_spec()
    await api.register_block(spec)

    response = await api.register_block(spec | {"executable_ref": "other@sha256:" + "c" * 64})

    assert response.status_code == 409


async def test_new_version_of_block_is_registered_alongside(api: Api) -> None:
    spec = block_spec()
    await api.register_block(spec)

    response = await api.register_block(spec | {"version": "2.0.0"})

    assert response.status_code == 201
    assert (await registered_names(api)).count(spec["name"]) == 2


async def test_project_users_see_registered_blocks(api: Api) -> None:
    spec = block_spec()
    await api.register_block(spec)

    response = await api.client.get("/api/v1/blocks")

    assert response.status_code == 200
    assert spec in response.json()


async def registered_names(api: Api) -> list[str]:
    response = await api.client.get("/api/v1/blocks")
    assert response.status_code == 200, response.text
    return [block["name"] for block in response.json()]
