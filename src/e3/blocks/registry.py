"""Регистрация и чтение неизменяемых BlockSpec."""

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from e3.blocks.models import BlockSpecRow
from e3.shared.contracts import BlockSpec


class BlockSpecConflict(Exception):
    """BlockSpec с такими `name` и `version` уже есть, и тело у него другое."""


class BlockNotFound(Exception):
    """BlockSpec с такими `name` и `version` нет."""


@dataclass(frozen=True)
class Registration:
    spec: BlockSpec
    created: bool


class BlockRegistry:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def register(self, spec: BlockSpec) -> Registration:
        """Сохраняет BlockSpec; повтор того же тела — успех без новой записи."""
        body = spec.model_dump(mode="json")
        async with self._sessions.begin() as session:
            inserted = await session.scalar(
                insert(BlockSpecRow)
                .values(id=uuid4(), name=spec.name, version=spec.version, spec=body)
                .on_conflict_do_nothing(index_elements=["name", "version"])
                .returning(BlockSpecRow.id)
            )
            if inserted is not None:
                return Registration(spec, created=True)
            existing = await _find_spec(session, spec.name, spec.version)
        if existing != body:
            raise BlockSpecConflict(f"{spec.name}:{spec.version}")
        return Registration(spec, created=False)

    async def list_specs(self) -> list[BlockSpec]:
        async with self._sessions() as session:
            bodies = await session.scalars(
                select(BlockSpecRow.spec).order_by(BlockSpecRow.name, BlockSpecRow.registered_at)
            )
            return [BlockSpec.model_validate(body) for body in bodies]

    async def get_spec(self, name: str, version: str) -> BlockSpec:
        async with self._sessions() as session:
            body = await _find_spec(session, name, version)
        if body is None:
            raise BlockNotFound(f"{name}:{version}")
        return BlockSpec.model_validate(body)


async def _find_spec(session: AsyncSession, name: str, version: str) -> dict[str, Any] | None:
    return await session.scalar(
        select(BlockSpecRow.spec).where(BlockSpecRow.name == name, BlockSpecRow.version == version)
    )
