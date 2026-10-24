"""Block Registry: неизменяемые версии BlockSpec.

Публичный интерфейс — `BlockRegistry` и `router`. Таблицы — внутренности модуля.
"""

from e3.blocks.api import router
from e3.blocks.registry import BlockNotFound, BlockRegistry

__all__ = ["BlockNotFound", "BlockRegistry", "router"]
