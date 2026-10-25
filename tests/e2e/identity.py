"""Вход на e2e-стенде: токен от `mock-oauth2-server` и свой Project через admin API."""

import os
from uuid import UUID, uuid4

import httpx

from tests.integration.conftest import Api

OIDC_URL = os.environ.get("E3_E2E_OIDC_URL", "http://localhost:8080/e3")
PLATFORM_TOKEN = os.environ.get("E3_E2E_PLATFORM_TOKEN", "dev-platform-token")


async def project_api(client: httpx.AsyncClient) -> Api:
    """Новый User с токеном в `client` и Project, где он Admin."""
    client.headers["Authorization"] = f"Bearer {await _token(f'e2e-{uuid4().hex}')}"
    me = await client.get("/api/v1/me")
    assert me.status_code == 200, me.text
    platform = {"Authorization": f"Bearer {PLATFORM_TOKEN}"}
    organization = await client.post(
        "/api/v1/organizations", json={"name": "e2e"}, headers=platform
    )
    assert organization.status_code == 201, organization.text
    project = await client.post(
        "/api/v1/projects",
        json={
            "organization_id": organization.json()["organization_id"],
            "name": f"e2e-{uuid4()}",
            "admin": {"user_id": me.json()["user_id"]},
        },
        headers=platform,
    )
    assert project.status_code == 201, project.text
    return Api(client, UUID(project.json()["project_id"]))


async def _token(login: str) -> str:
    async with httpx.AsyncClient(timeout=30) as oidc:
        response = await oidc.post(
            f"{OIDC_URL}/token",
            data={"grant_type": "client_credentials", "client_id": login, "client_secret": "e2e"},
        )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]
