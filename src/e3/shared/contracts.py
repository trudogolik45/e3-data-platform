"""Контракты Backend ↔ DS (PRD §4), общие для Control Plane и Execution Plane.

Публикуются как JSON Schema командой `e3-contracts-schema` (см. `schemas`).
"""

from typing import Annotated, Any, Literal
from uuid import UUID

from jsonschema import Draft202012Validator, SchemaError
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from e3.shared.manifest import DataFormat, DataSchema

# Блок кладёт свой ExecutionResult в `{output_prefix}/result.json`: оттуда его
# забирает Execution Controller.
RESULT_FILE = "result.json"

# Неизменяемая ссылка на образ: `<repo>@sha256:<64 hex>`. Теги вроде `:latest` не годятся.
OCI_DIGEST_PATTERN = r"^[^\s@]+@sha256:[0-9a-f]{64}$"


class DatasetVersionRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["dataset_version"] = "dataset_version"
    dataset_id: UUID
    version_id: UUID


class NodeOutputRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    kind: Literal["node_output"] = "node_output"
    node_run_id: UUID
    port: str


DataRef = Annotated[DatasetVersionRef | NodeOutputRef, Field(discriminator="kind")]


class ResolvedInput(BaseModel):
    model_config = ConfigDict(frozen=True, serialize_by_alias=True)

    manifest_uri: str
    format: DataFormat
    data_schema: DataSchema = Field(
        validation_alias=AliasChoices("schema", "data_schema"), serialization_alias="schema"
    )
    source: DataRef = Field(description="Только для логов и lineage")


DataType = Literal["dataset", "scalar", "artifact"]


class PortSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1)
    data_type: DataType
    required: bool = True


class BlockSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    runtime: Literal["spark"]
    inputs: list[PortSpec]
    outputs: list[PortSpec]
    parameters_schema: dict[str, Any]
    executable_ref: str = Field(pattern=OCI_DIGEST_PATTERN, description="OCI image digest")

    @field_validator("parameters_schema")
    @classmethod
    def _is_json_schema(cls, schema: dict[str, Any]) -> dict[str, Any]:
        """`parameters` узлов проверяются по ней при публикации PipelineVersion."""
        try:
            Draft202012Validator.check_schema(schema)
        except SchemaError as error:
            raise ValueError(f"не JSON Schema Draft 2020-12: {error.message}") from None
        return schema


class ExecutionContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    node_run_id: UUID
    attempt_id: UUID
    inputs: dict[str, ResolvedInput]
    parameters: dict[str, Any]
    output_prefix: str


class ExecutionResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    node_run_id: UUID
    attempt_id: UUID
    outputs: dict[str, str] = Field(description="port → manifest_uri подготовленного manifest")
    metrics: dict[str, float] = Field(default_factory=dict[str, float])
