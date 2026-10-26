"""Кто делает запрос: FastAPI-зависимости `current_principal` и `require_platform_team`.

`oidc` — Bearer JWT провайдера; `dev` — один пользователь, Admin dev-Project, без токена.
"""

import hmac
from typing import Annotated, Protocol
from uuid import UUID

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import SecretStr

from n3.identity.access import Principal
from n3.identity.service import IdentityService
from n3.identity.tokens import InvalidToken, JwksUnavailable, TokenVerifier

bearer = HTTPBearer(auto_error=False)
Credentials = Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]


class Authenticator(Protocol):
    async def principal(self, credentials: HTTPAuthorizationCredentials | None) -> Principal: ...


class OidcAuthenticator:
    def __init__(self, verifier: TokenVerifier, identity: IdentityService) -> None:
        self._verifier = verifier
        self._identity = identity

    async def principal(self, credentials: HTTPAuthorizationCredentials | None) -> Principal:
        if credentials is None:
            raise _unauthorized("нужен Bearer-токен")
        try:
            identity = await self._verifier.verify(credentials.credentials)
        except InvalidToken:
            raise _unauthorized("токен не прошёл проверку") from None
        except JwksUnavailable:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, "ключи OIDC-провайдера недоступны"
            ) from None
        return await self._identity.principal(await self._identity.sign_in(identity))


class DevAuthenticator:
    def __init__(
        self, identity: IdentityService, *, user_id: UUID, organization_id: UUID, project_id: UUID
    ) -> None:
        self._identity = identity
        self._user_id = user_id
        self._organization_id = organization_id
        self._project_id = project_id
        self._seeded = False

    async def principal(self, credentials: HTTPAuthorizationCredentials | None) -> Principal:
        if not self._seeded:
            await self._identity.ensure_dev(
                user_id=self._user_id,
                organization_id=self._organization_id,
                project_id=self._project_id,
            )
            self._seeded = True
        return await self._identity.principal(self._user_id)


async def current_principal(request: Request, credentials: Credentials) -> Principal:
    authenticator: Authenticator = request.app.state.authenticator
    return await authenticator.principal(credentials)


def require_platform_team(request: Request, credentials: Credentials) -> None:
    """Admin API команды платформы: только с `N3_PLATFORM_TOKEN`."""
    if credentials is None:
        raise _unauthorized("нужен токен команды платформы")
    expected: SecretStr | None = request.app.state.platform_token
    if not _token_matches(credentials.credentials, expected):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "только для команды платформы")


def _token_matches(presented: str, expected: SecretStr | None) -> bool:
    # Пустой или незаданный токен не открывает доступ никому.
    if expected is None or not expected.get_secret_value():
        return False
    return hmac.compare_digest(presented.encode(), expected.get_secret_value().encode())


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status.HTTP_401_UNAUTHORIZED, detail, headers={"WWW-Authenticate": "Bearer"}
    )
