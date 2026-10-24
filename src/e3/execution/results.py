"""ExecutionResult, который блок записал в S3 рядом со своими выходами."""

from pydantic import ValidationError

from e3.shared.contracts import RESULT_FILE, ExecutionContext, ExecutionResult
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
