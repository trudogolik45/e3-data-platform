"""REST API v1 Block Registry.

`POST` — административный: только команда платформы с `E3_PLATFORM_TOKEN`.
"""

import hmac
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import SecretStr

from e3.blocks.registry import BlockRegistry, BlockSpecConflict
from e3.identity import Principal, current_principal
from e3.shared.contracts import BlockSpec

router = APIRouter(prefix="/api/v1/blocks", tags=["blocks"])

bearer = HTTPBearer(auto_error=False)


def get_registry(request: Request) -> BlockRegistry:
    registry: BlockRegistry = request.app.state.blocks
    return registry


def require_platform_team(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
) -> None:
    if credentials is None:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "нужен токен команды платформы",
            headers={"WWW-Authenticate": "Bearer"},
        )
    expected: SecretStr | None = request.app.state.platform_token
    if not _token_matches(credentials.credentials, expected):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "только для команды платформы")


def _token_matches(presented: str, expected: SecretStr | None) -> bool:
    # Пустой или незаданный токен не открывает доступ никому.
    if expected is None or not expected.get_secret_value():
        return False
    return hmac.compare_digest(presented.encode(), expected.get_secret_value().encode())


RegistryDep = Annotated[BlockRegistry, Depends(get_registry)]


@router.post("", status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_platform_team)])
async def register_block(spec: BlockSpec, registry: RegistryDep, response: Response) -> BlockSpec:
    try:
        registration = await registry.register(spec)
    except BlockSpecConflict:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "эта версия блока уже зарегистрирована с другим телом"
        ) from None
    if not registration.created:
        response.status_code = status.HTTP_200_OK
    return registration.spec


@router.get("")
async def list_blocks(
    registry: RegistryDep, _principal: Annotated[Principal, Depends(current_principal)]
) -> list[BlockSpec]:
    return await registry.list_specs()
