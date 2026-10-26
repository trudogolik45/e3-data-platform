"""REST API v1 Block Registry.

`POST` — административный: только команда платформы с `N3_PLATFORM_TOKEN`.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from n3.blocks.registry import BlockRegistry, BlockSpecConflict
from n3.identity import Principal, current_principal, require_platform_team
from n3.shared.contracts import BlockSpec

router = APIRouter(prefix="/api/v1/blocks", tags=["blocks"])


def get_registry(request: Request) -> BlockRegistry:
    registry: BlockRegistry = request.app.state.blocks
    return registry


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
    """Блоки общие для платформы: любому вошедшему, без Role."""
    return await registry.list_specs()
