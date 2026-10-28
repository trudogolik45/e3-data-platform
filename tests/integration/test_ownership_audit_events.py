"""Audit Event изменений владения и доступа: Organization, Project и Membership.

Журнал читается только через SQL, поэтому и тест смотрит в `audit.events` напрямую.
`user_id = NULL` — действие Platform Team.
"""

from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from e3.identity import IdentityService
from e3.settings import Settings
from tests.integration.conftest import PLATFORM_TOKEN, User, World
from tests.integration.test_audit_events import unreachable_database

pytestmark = pytest.mark.integration


async def test_created_organization_leaves_audit_event_by_platform_team(
    world: World, oidc_settings: Settings
) -> None:
    platform = await world.client(PLATFORM_TOKEN)

    created = await platform.post("/api/v1/organizations", json={"name": "org"})

    assert created.status_code == 201, created.text
    organization_id = created.json()["organization_id"]
    assert await audit_events(oidc_settings, "organization.create", organization_id) == [
        {
            "outcome": "allowed",
            "user_id": None,
            "project_id": None,
            "role": None,
            "previous_role": None,
            "dedup_key": organization_id,
            "trace_id": None,
        }
    ]


async def test_created_project_leaves_audit_events_of_project_and_its_first_admin(
    world: World, oidc_settings: Settings
) -> None:
    admin = await world.user()

    project_id = await world.project(admin)

    assert await audit_events(oidc_settings, "project.create", project_id) == [
        by_platform_team(project_id, dedup_key=str(project_id))
    ]
    assert await audit_events(oidc_settings, "membership.grant", admin.user_id) == [
        by_platform_team(project_id, role="Admin")
    ]


async def test_membership_grant_role_changes_and_revoke_are_audited_one_by_one(
    world: World, oidc_settings: Settings
) -> None:
    admin, user = await world.user(), await world.user()
    project_id = await world.project(admin)
    member_url = f"/api/v1/projects/{project_id}/members/{user.user_id}"
    client = await world.client(admin.token)

    await world.grant(project_id, admin, user, "Viewer")
    await world.set_role(project_id, admin, user, "Editor")
    await world.set_role(project_id, admin, user, "Viewer")
    revoked = await client.delete(member_url)

    assert revoked.status_code == 204, revoked.text
    assert await audit_events(oidc_settings, "membership.grant", user.user_id) == [
        by_user(admin, project_id, role="Viewer", previous_role=None)
    ]
    assert await audit_events(oidc_settings, "membership.change_role", user.user_id) == [
        by_user(admin, project_id, role="Editor", previous_role="Viewer"),
        by_user(admin, project_id, role="Viewer", previous_role="Editor"),
    ]
    assert await audit_events(oidc_settings, "membership.revoke", user.user_id) == [
        by_user(admin, project_id, role=None, previous_role="Viewer")
    ]


async def test_rejected_membership_changes_leave_no_audit_event(
    world: World, oidc_settings: Settings
) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    client = await world.client(admin.token)
    admin_url = f"/api/v1/projects/{project_id}/members/{admin.user_id}"

    regranted = await client.post(
        f"/api/v1/projects/{project_id}/members",
        json={"user_id": str(admin.user_id), "role": "Viewer"},
    )
    stepped_down = await client.put(admin_url, json={"role": "Editor"})
    left = await client.delete(admin_url)

    assert (regranted.status_code, stepped_down.status_code, left.status_code) == (409, 409, 409)
    assert await audit_events(oidc_settings, "membership.grant", admin.user_id) == [
        by_platform_team(project_id, role="Admin")
    ]
    assert await audit_events(oidc_settings, "membership.change_role", admin.user_id) == []
    assert await audit_events(oidc_settings, "membership.revoke", admin.user_id) == []


async def test_non_admin_granting_membership_is_denied_with_requested_role(
    world: World, oidc_settings: Settings
) -> None:
    admin, outsider = await world.user(), await world.user()
    project_id = await world.project(admin)
    editor = await world.member(project_id, admin, "Editor")

    denied = await (await world.client(editor.token)).post(
        f"/api/v1/projects/{project_id}/members",
        json={"user_id": str(outsider.user_id), "role": "Admin"},
    )

    assert denied.status_code == 403, denied.text
    assert await audit_events(oidc_settings, "membership.grant", outsider.user_id) == [
        denied_to(editor, project_id, role="Admin", previous_role=None)
    ]


async def test_non_admin_granting_membership_by_email_is_denied_without_target(
    world: World, oidc_settings: Settings
) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    viewer = await world.member(project_id, admin, "Viewer")

    denied = await (await world.client(viewer.token)).post(
        f"/api/v1/projects/{project_id}/members",
        json={"email": f"{viewer.subject}@e3.local", "role": "Editor"},
    )

    assert denied.status_code == 403, denied.text
    events = await audit_events(oidc_settings, "membership.grant", None, project_id=project_id)
    assert events == [denied_to(viewer, project_id, role="Editor", previous_role=None)]


async def test_non_admin_changing_role_is_denied_with_requested_and_current_role(
    world: World, oidc_settings: Settings
) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    editor = await world.member(project_id, admin, "Editor")

    denied = await (await world.client(editor.token)).put(
        f"/api/v1/projects/{project_id}/members/{editor.user_id}", json={"role": "Admin"}
    )

    assert denied.status_code == 403, denied.text
    assert await audit_events(oidc_settings, "membership.change_role", editor.user_id) == [
        denied_to(editor, project_id, role="Admin", previous_role="Editor")
    ]


async def test_non_admin_revoking_membership_is_denied_with_current_role(
    world: World, oidc_settings: Settings
) -> None:
    admin, outsider = await world.user(), await world.user()
    project_id = await world.project(admin)

    denied = await (await world.client(outsider.token)).delete(
        f"/api/v1/projects/{project_id}/members/{admin.user_id}"
    )

    assert denied.status_code == 403, denied.text
    assert await audit_events(oidc_settings, "membership.revoke", admin.user_id) == [
        denied_to(outsider, project_id, role=None, previous_role="Admin")
    ]


async def test_membership_denial_is_still_403_when_current_role_is_unknown(
    world: World,
) -> None:
    admin = await world.user()
    project_id = await world.project(admin)
    viewer = await world.member(project_id, admin, "Viewer")
    world.app.state.identity = IdentityService(unreachable_database())

    denied = await (await world.client(viewer.token)).delete(
        f"/api/v1/projects/{project_id}/members/{admin.user_id}"
    )

    assert denied.status_code == 403, denied.text


@pytest.mark.parametrize("token", [None, "not-the-platform-token"])
async def test_rejected_platform_request_leaves_no_audit_event(
    world: World, oidc_settings: Settings, token: str | None
) -> None:
    admin = await world.user()
    organization_id = (
        await (await world.client(PLATFORM_TOKEN)).post("/api/v1/organizations", json={"name": "o"})
    ).json()["organization_id"]
    client = await world.client(token)
    before = await ownership_event_count(oidc_settings)

    organization = await client.post("/api/v1/organizations", json={"name": "org"})
    project = await client.post(
        "/api/v1/projects",
        json={
            "organization_id": organization_id,
            "name": "p",
            "admin": {"user_id": str(admin.user_id)},
        },
    )

    assert organization.status_code == project.status_code == (401 if token is None else 403)
    assert await ownership_event_count(oidc_settings) == before


async def test_dev_bootstrap_leaves_no_audit_event(world: World, oidc_settings: Settings) -> None:
    identity: IdentityService = world.app.state.identity
    user_id = oidc_settings.dev_user_id
    organization_id = oidc_settings.dev_organization_id
    project_id = oidc_settings.dev_project_id

    await identity.ensure_dev(
        user_id=user_id, organization_id=organization_id, project_id=project_id
    )

    assert await audit_events(oidc_settings, "organization.create", organization_id) == []
    assert await audit_events(oidc_settings, "project.create", project_id) == []
    assert await audit_events(oidc_settings, "membership.grant", user_id) == []


def by_user(
    actor: User, project_id: UUID, *, role: str | None, previous_role: str | None
) -> dict[str, Any]:
    return {
        "outcome": "allowed",
        "user_id": actor.user_id,
        "project_id": project_id,
        "role": role,
        "previous_role": previous_role,
        "dedup_key": None,
        "trace_id": None,
    }


def denied_to(
    actor: User, project_id: UUID, *, role: str | None, previous_role: str | None
) -> dict[str, Any]:
    return {
        **by_user(actor, project_id, role=role, previous_role=previous_role),
        "outcome": "denied",
    }


def by_platform_team(
    project_id: UUID, *, role: str | None = None, dedup_key: str | None = None
) -> dict[str, Any]:
    return {
        "outcome": "allowed",
        "user_id": None,
        "project_id": project_id,
        "role": role,
        "previous_role": None,
        "dedup_key": dedup_key,
        "trace_id": None,
    }


async def ownership_event_count(settings: Settings) -> int:
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as connection:
            count = await connection.scalar(
                text(
                    "SELECT count(*) FROM audit.events"
                    " WHERE action IN ('organization.create', 'project.create')"
                )
            )
            return int(count or 0)
    finally:
        await engine.dispose()


async def audit_events(
    settings: Settings,
    action: str,
    object_id: str | UUID | None,
    *,
    project_id: UUID | None = None,
) -> list[Any]:
    """Audit Event действия над объектом (`None` — без объекта), в порядке записи."""
    query = (
        "SELECT outcome, user_id, project_id, role, previous_role, dedup_key, trace_id"
        " FROM audit.events WHERE action = :action"
        " AND object_id IS NOT DISTINCT FROM CAST(:object_id AS uuid)"
    )
    params: dict[str, Any] = {
        "action": action,
        "object_id": None if object_id is None else UUID(str(object_id)),
    }
    if project_id is not None:
        query += " AND project_id = :project_id"
        params["project_id"] = project_id
    engine = create_async_engine(settings.database_url)
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(text(query + " ORDER BY occurred_at"), params)
            return [dict(row) for row in rows.mappings()]
    finally:
        await engine.dispose()
