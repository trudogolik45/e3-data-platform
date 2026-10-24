"""Identity & Access: кто пользователь и к каким Project у него есть доступ."""

from e3.identity.access import (
    Principal,
    ProjectAccessDenied,
    authorize_project,
    current_principal,
    dev_principal,
)

__all__ = [
    "Principal",
    "ProjectAccessDenied",
    "authorize_project",
    "current_principal",
    "dev_principal",
]
