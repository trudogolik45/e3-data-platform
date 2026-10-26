"""Identity & Access: кто пользователь и к каким Project у него есть доступ (ADR-0004).

OIDC отвечает за идентичность (`iss` + `sub`), Membership в PostgreSQL — за права.
"""

from n3.identity.access import Principal, ProjectAccessDenied, Role, authorize
from n3.identity.api import router
from n3.identity.authentication import (
    Authenticator,
    DevAuthenticator,
    OidcAuthenticator,
    current_principal,
    require_platform_team,
)
from n3.identity.service import (
    LEGACY_MIGRATION_USER_ID,
    IdentityService,
    LastAdmin,
    Member,
    MemberExists,
    MemberNotFound,
    OrganizationNotFound,
    ProjectNotFound,
    UserAmbiguous,
    UserNotFound,
)
from n3.identity.tokens import Identity, InvalidToken, JwksUnavailable, TokenVerifier

__all__ = [
    "LEGACY_MIGRATION_USER_ID",
    "Authenticator",
    "DevAuthenticator",
    "Identity",
    "IdentityService",
    "InvalidToken",
    "JwksUnavailable",
    "LastAdmin",
    "Member",
    "MemberExists",
    "MemberNotFound",
    "OidcAuthenticator",
    "OrganizationNotFound",
    "Principal",
    "ProjectAccessDenied",
    "ProjectNotFound",
    "Role",
    "TokenVerifier",
    "UserAmbiguous",
    "UserNotFound",
    "authorize",
    "current_principal",
    "require_platform_team",
    "router",
]
