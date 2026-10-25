"""Запрос на запуск и разрешение привязок слотов (PRD §5).

Late Binding: при создании PipelineRun каждый слот получает точное значение —
из запроса или из значения по умолчанию PipelineVersion. Input Slot —
существующая DatasetVersion, Output Slot — существующий Dataset того же
Project. «Последняя версия» превращается в точную здесь же, архивные Dataset
отклоняются.
"""

import hashlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from e3 import catalog
from e3.identity import Principal, Role, authorize
from e3.pipelines import ExactVersion, LatestVersion, PipelineGraph, TargetDataset


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


@dataclass(frozen=True)
class Bindings:
    """Точные привязки всех слотов, которые фиксирует PipelineRun."""

    inputs: dict[str, InputBinding]
    outputs: dict[str, OutputBinding]


async def resolve_bindings(
    datasets: catalog.Catalog,
    principal: Principal,
    *,
    project_id: UUID,
    graph: PipelineGraph,
    request: RunRequest,
) -> Bindings:
    """Запрос поверх значений по умолчанию; права — чтение входов и запись в выходы."""
    _check_known("Input Slot", (slot.name for slot in graph.input_slots), request.inputs)
    _check_known("Output Slot", (slot.name for slot in graph.output_slots), request.outputs)
    inputs: dict[str, InputBinding] = {}
    for slot in graph.input_slots:
        binding = request.inputs.get(slot.name) or await _input_default(
            datasets, slot.name, slot.default
        )
        await _check_input(datasets, principal, slot.name, binding)
        inputs[slot.name] = binding
    outputs: dict[str, OutputBinding] = {}
    for slot in graph.output_slots:
        output = request.outputs.get(slot.name) or _output_default(slot.name, slot.default)
        await _check_output(datasets, principal, slot.name, output, project_id)
        outputs[slot.name] = output
    return Bindings(inputs, outputs)


def _check_known(kind: str, declared: Iterable[str], bound: Mapping[str, object]) -> None:
    unknown = sorted(bound.keys() - set(declared))
    if unknown:
        raise RunRequestInvalid(f"у PipelineVersion нет {kind}: {', '.join(unknown)}")


async def _input_default(
    datasets: catalog.Catalog, slot: str, default: ExactVersion | LatestVersion | None
) -> InputBinding:
    if default is None:
        raise RunRequestInvalid(f"не привязан Input Slot: {slot}")
    if isinstance(default, ExactVersion):
        return InputBinding(dataset_id=default.dataset_id, version_id=default.version_id)
    latest = await datasets.latest_version(default.dataset_id)
    if latest is None:
        raise RunRequestInvalid(f"Input Slot {slot}: у Dataset нет версий")
    return InputBinding(dataset_id=default.dataset_id, version_id=latest.version_id)


def _output_default(slot: str, default: TargetDataset | None) -> OutputBinding:
    if default is None:
        raise RunRequestInvalid(f"не привязан Output Slot: {slot}")
    return OutputBinding(dataset_id=default.dataset_id)


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
    authorize(principal, dataset.project_id, Role.VIEWER)
    if dataset.archived:
        raise RunRequestInvalid(f"Input Slot {slot}: Dataset в архиве")


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
    # Запись разрешает Role в Project целевого Dataset, а не место запуска.
    authorize(principal, dataset.project_id, Role.EDITOR)
    if dataset.project_id != project_id:
        raise RunRequestInvalid(f"Output Slot {slot}: Dataset из другого Project")
    if dataset.archived:
        raise RunRequestInvalid(f"Output Slot {slot}: Dataset в архиве")
