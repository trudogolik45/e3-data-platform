"""Dataset Catalog: Dataset, Upload, DatasetVersion и их схемы.

Публичный интерфейс — `Catalog` и `router`. Таблицы, Upload, регистрация
версий и проверки CSV — внутренности модуля.
"""

from e3.catalog.api import router
from e3.catalog.catalog import Catalog
from e3.catalog.versions import DatasetVersion, DatasetVersionNotFound

__all__ = ["Catalog", "DatasetVersion", "DatasetVersionNotFound", "router"]
