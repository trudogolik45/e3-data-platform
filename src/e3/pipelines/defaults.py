"""Проверка значений слотов по умолчанию перед публикацией PipelineVersion.

Значение по умолчанию ссылается только на существующий Dataset того же
Project. Архив здесь не проверяется: его проверяет запуск.
"""

from uuid import UUID

from e3 import catalog
from e3.pipelines.graph import ExactVersion, PipelineGraph
from e3.pipelines.validation import GraphInvalid


async def check_defaults(datasets: catalog.Catalog, graph: PipelineGraph, project_id: UUID) -> None:
    for slot in graph.input_slots:
        if slot.default is None:
            continue
        where = f"Input Slot {slot.name}"
        await _check_dataset(datasets, where, slot.default.dataset_id, project_id)
        if isinstance(slot.default, ExactVersion):
            await _check_version(datasets, where, slot.default)
    for slot in graph.output_slots:
        if slot.default is None:
            continue
        where = f"Output Slot {slot.name}"
        await _check_dataset(datasets, where, slot.default.dataset_id, project_id)


async def _check_dataset(
    datasets: catalog.Catalog, where: str, dataset_id: UUID, project_id: UUID
) -> None:
    try:
        dataset = await datasets.get_dataset(dataset_id)
    except catalog.DatasetNotFound:
        raise GraphInvalid(f"{where}: по умолчанию указан несуществующий Dataset") from None
    if dataset.project_id != project_id:
        raise GraphInvalid(f"{where}: по умолчанию указан Dataset из другого Project")


async def _check_version(datasets: catalog.Catalog, where: str, default: ExactVersion) -> None:
    try:
        version = await datasets.resolve(default.version_id)
    except catalog.DatasetVersionNotFound:
        raise GraphInvalid(f"{where}: по умолчанию указана несуществующая DatasetVersion") from None
    if version.dataset_id != default.dataset_id:
        raise GraphInvalid(f"{where}: DatasetVersion по умолчанию из другого Dataset")
