"""Заглушка доступа для dev: один Organization, один Project, один Admin.

Настоящие OIDC и роли появятся в срезе Identity & Access (#8).
"""

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from fastapi import Request

Role = Literal["Viewer", "Editor", "Admin"]


class ProjectAccessDenied(Exception):
    """У пользователя нет доступа к Project."""


@dataclass(frozen=True)
class Principal:
    user_id: UUID
    organization_id: UUID
    project_roles: dict[UUID, Role]


def dev_principal(*, user_id: UUID, organization_id: UUID, project_id: UUID) -> Principal:
    return Principal(user_id, organization_id, {project_id: "Admin"})


def authorize_project(principal: Principal, project_id: UUID) -> None:
    if project_id not in principal.project_roles:
        raise ProjectAccessDenied(str(project_id))


def current_principal(request: Request) -> Principal:
    principal: Principal = request.app.state.principal
    return principal
