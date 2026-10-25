"""Role, Principal и проверка доступа к Project по Membership (ADR-0004)."""

from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID


class Role(StrEnum):
    VIEWER = "Viewer"
    EDITOR = "Editor"
    ADMIN = "Admin"

    def includes(self, minimum: "Role") -> bool:
        """Каждая следующая Role включает предыдущую."""
        return LADDER.index(self) >= LADDER.index(minimum)


LADDER = (Role.VIEWER, Role.EDITOR, Role.ADMIN)


class ProjectAccessDenied(Exception):
    """У пользователя нет Membership в Project с нужной Role."""


@dataclass(frozen=True)
class Principal:
    user_id: UUID
    project_roles: dict[UUID, Role]


def authorize(principal: Principal, project_id: UUID, minimum: Role) -> None:
    role = principal.project_roles.get(project_id)
    if role is None or not role.includes(minimum):
        raise ProjectAccessDenied(str(project_id))
