"""ExecutionResult или ExecutionError, которые блок записал в S3 рядом со своими выходами."""

from pydantic import ValidationError

from e3.shared.contracts import (
    ERROR_FILE,
    ERROR_FILE_LIMIT,
    RESULT_FILE,
    ExecutionContext,
    ExecutionError,
    ExecutionResult,
)
from e3.shared.storage import ObjectNotFound, ObjectStorage


class StoredResults:
    def __init__(self, storage: ObjectStorage) -> None:
        self._storage = storage

    async def fetch(self, context: ExecutionContext) -> ExecutionResult | None:
        key = self._storage.key(f"{context.output_prefix}/{RESULT_FILE}")
        try:
            return ExecutionResult.model_validate_json(await self._storage.read(key))
        except (ObjectNotFound, ValidationError):
            return None

    async def fetch_error(self, context: ExecutionContext) -> ExecutionError | None:
        """Файл больше `ERROR_FILE_LIMIT` не читается: это уже не ошибка блока, а мусор.

        Попытка мертва, поэтому между проверкой размера и чтением файл не меняется.
        """
        key = self._storage.key(f"{context.output_prefix}/{ERROR_FILE}")
        info = await self._storage.head(key)
        if info is None or info.size_bytes > ERROR_FILE_LIMIT:
            return None
        try:
            return ExecutionError.model_validate_json(await self._storage.read(key))
        except (ObjectNotFound, ValidationError):
            return None
