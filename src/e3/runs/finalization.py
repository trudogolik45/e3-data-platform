"""FINALIZING: проверка manifest из ExecutionResult перед приёмом NodeOutput.

Принимается только то, что Current Attempt положил в свой
`staging/executions/{attempt_id}/`, с полным набором выходов BlockSpec и
существующими файлами нужного размера. Контрольные суммы сверяет
публикация при копировании.
"""

from uuid import UUID

from pydantic import ValidationError

from e3.shared.contracts import ExecutionResult
from e3.shared.manifest import Manifest
from e3.shared.storage import ObjectNotFound, ObjectStorage


class OutputRejected(Exception):
    """Результат попытки нельзя принять: текст — причина."""


def execution_prefix(attempt_id: UUID) -> str:
    return f"staging/executions/{attempt_id}"


async def verify_outputs(
    storage: ObjectStorage, result: ExecutionResult, expected_ports: list[str]
) -> dict[str, Manifest]:
    if set(result.outputs) != set(expected_ports):
        raise OutputRejected(
            f"ожидались выходы {sorted(expected_ports)}, пришли {sorted(result.outputs)}"
        )
    prefix = storage.uri(execution_prefix(result.attempt_id)) + "/"
    manifests: dict[str, Manifest] = {}
    for port, uri in result.outputs.items():
        if not uri.startswith(prefix):
            raise OutputRejected(f"{port}: manifest вне каталога попытки")
        manifests[port] = await _verified_manifest(storage, storage.key(uri))
    return manifests


async def _verified_manifest(storage: ObjectStorage, key: str) -> Manifest:
    try:
        manifest = Manifest.model_validate_json(await storage.read(key))
    except (ObjectNotFound, ValidationError) as error:
        raise OutputRejected(f"{key}: manifest не читается ({type(error).__name__})") from None
    directory = key.rpartition("/")[0]
    for file in manifest.files:
        info = await storage.head(f"{directory}/{file.path}")
        if info is None or info.size_bytes != file.size_bytes:
            raise OutputRejected(f"{file.path}: файла нет или размер не совпадает с manifest")
    return manifest
