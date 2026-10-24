"""Запрос на запуск и проверка привязок слотов (PRD §5).

Late Binding: при создании PipelineRun каждый слот получает точное значение.
Input Slot — существующая DatasetVersion, Output Slot — существующий Dataset
того же Project. Значения по умолчанию появятся с #7.
"""

import hashlib
import json
from collections.abc import Iterable, Mapping
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from e3 import catalog
from e3.identity import Principal, authorize_project
from e3.pipelines import PipelineGraph


class InputBinding(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    dataset_id: UUID
    version_id: UUID


class OutputBinding(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    dataset_id: UUID


class RunRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    pipeline_id: UUID
    pipeline_version: int
    inputs: dict[str, InputBinding] = Field(default_factory=dict[str, InputBinding])
    outputs: dict[str, OutputBinding] = Field(default_factory=dict[str, OutputBinding])

    def fingerprint(self) -> str:
        """Хеш тела без учёта порядка ключей: повтор с тем же ключом сверяется по нему."""
        canonical = json.dumps(self.model_dump(mode="json"), sort_keys=True)
        return hashlib.sha256(canonical.encode()).hexdigest()


class RunRequestInvalid(Exception):
    """Запуск нельзя создать: текст исключения — причина для пользователя."""


async def check_bindings(
    datasets: catalog.Catalog,
    principal: Principal,
    *,
    project_id: UUID,
    graph: PipelineGraph,
    request: RunRequest,
) -> None:
    """Проверяет слоты и права: чтение входов и запись в выходы (`ProjectAccessDenied`)."""
    _check_slots("Input Slot", (slot.name for slot in graph.input_slots), request.inputs)
    _check_slots("Output Slot", (slot.name for slot in graph.output_slots), request.outputs)
    for slot, binding in request.inputs.items():
        await _check_input(datasets, principal, slot, binding)
    for slot, binding in request.outputs.items():
        await _check_output(datasets, principal, slot, binding, project_id)


def _check_slots(kind: str, declared: Iterable[str], bound: Mapping[str, object]) -> None:
    names = set(declared)
    missing = sorted(names - bound.keys())
    if missing:
        raise RunRequestInvalid(f"не привязан {kind}: {', '.join(missing)}")
    unknown = sorted(bound.keys() - names)
    if unknown:
        raise RunRequestInvalid(f"у PipelineVersion нет {kind}: {', '.join(unknown)}")


async def _check_input(
    datasets: catalog.Catalog, principal: Principal, slot: str, binding: InputBinding
) -> None:
    try:
        version = await datasets.resolve(binding.version_id)
    except catalog.DatasetVersionNotFound:
        raise RunRequestInvalid(f"Input Slot {slot}: нет DatasetVersion") from None
    if version.dataset_id != binding.dataset_id:
        raise RunRequestInvalid(f"Input Slot {slot}: DatasetVersion из другого Dataset")
    dataset = await datasets.get_dataset(binding.dataset_id)
    authorize_project(principal, dataset.project_id)


async def _check_output(
    datasets: catalog.Catalog,
    principal: Principal,
    slot: str,
    binding: OutputBinding,
    project_id: UUID,
) -> None:
    try:
        dataset = await datasets.get_dataset(binding.dataset_id)
    except catalog.DatasetNotFound:
        raise RunRequestInvalid(f"Output Slot {slot}: нет Dataset") from None
    authorize_project(principal, dataset.project_id)
    if dataset.project_id != project_id:
        raise RunRequestInvalid(f"Output Slot {slot}: Dataset из другого Project")
