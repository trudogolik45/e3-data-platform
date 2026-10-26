"""Block Registry: неизменяемые версии BlockSpec.

Публичный интерфейс — `BlockRegistry` и `router`. Таблицы — внутренности модуля.
"""

from n3.blocks.api import router
from n3.blocks.registry import BlockNotFound, BlockRegistry

__all__ = ["BlockNotFound", "BlockRegistry", "router"]
