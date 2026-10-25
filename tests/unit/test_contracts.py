from typing import Any
from uuid import uuid4

import pytest
from pydantic import TypeAdapter, ValidationError

from e3.shared.contracts import (
    BlockSpec,
    DataRef,
    DatasetVersionRef,
    NodeOutputRef,
)
from e3.shared.manifest import Column
from e3.shared.schemas import json_schemas

DIGEST = "registry.local/e3/read@sha256:" + "a" * 64


def block_spec(**overrides: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "name": "read",
        "version": "1.0.0",
        "runtime": "spark",
        "inputs": [],
        "outputs": [{"name": "data", "data_type": "dataset"}],
        "parameters_schema": {},
        "executable_ref": DIGEST,
    }
    return spec | overrides


def test_data_ref_is_chosen_by_kind() -> None:
    adapter = TypeAdapter[DataRef](DataRef)
    node_run_id = uuid4()

    ref = adapter.validate_python({"kind": "node_output", "node_run_id": node_run_id, "port": "x"})

    assert ref == NodeOutputRef(node_run_id=node_run_id, port="x")


def test_data_ref_without_known_kind_is_invalid() -> None:
    adapter = TypeAdapter[DataRef](DataRef)

    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "s3_path", "dataset_id": uuid4(), "version_id": uuid4()})


def test_block_spec_accepts_oci_digest() -> None:
    spec = BlockSpec.model_validate(block_spec())

    assert spec.executable_ref == DIGEST


@pytest.mark.parametrize(
    "ref",
    [
        "registry.local/e3/read:latest",
        "registry.local/e3/read:3.5",
        "registry.local/e3/read@sha256:abc",
        "@sha256:" + "a" * 64,
    ],
)
def test_block_spec_rejects_anything_but_digest(ref: str) -> None:
    with pytest.raises(ValidationError, match="executable_ref"):
        BlockSpec.model_validate(block_spec(executable_ref=ref))


def test_block_spec_supports_only_spark_runtime() -> None:
    with pytest.raises(ValidationError, match="runtime"):
        BlockSpec.model_validate(block_spec(runtime="python"))


def test_block_spec_rejects_invalid_parameters_schema() -> None:
    with pytest.raises(ValidationError, match="parameters_schema"):
        BlockSpec.model_validate(block_spec(parameters_schema={"type": "no-such-type"}))


@pytest.mark.parametrize("column_type", ["string", "long", "double"])
def test_manifest_column_carries_real_type(column_type: str) -> None:
    column = Column.model_validate({"name": "amount", "type": column_type})

    assert column.type == column_type


def test_manifest_column_rejects_unknown_type() -> None:
    with pytest.raises(ValidationError, match="type"):
        Column.model_validate({"name": "amount", "type": "decimal"})


def test_json_schema_covers_all_contracts() -> None:
    schemas = json_schemas()

    assert set(schemas) == {
        "DatasetVersionRef",
        "NodeOutputRef",
        "DataRef",
        "ResolvedInput",
        "PortSpec",
        "BlockSpec",
        "ExecutionContext",
        "ExecutionResult",
        "Manifest",
        "SubmitExecution",
        "ExecutionEvent",
    }


def test_data_ref_schema_is_discriminated_by_kind() -> None:
    schema = json_schemas()["DataRef"]

    assert schema["discriminator"]["propertyName"] == "kind"
    assert set(schema["discriminator"]["mapping"]) == {"dataset_version", "node_output"}


def test_execution_context_and_result_carry_attempt_identity() -> None:
    schemas = json_schemas()

    for name in ("ExecutionContext", "ExecutionResult"):
        assert {"node_run_id", "attempt_id"} <= set(schemas[name]["required"])


def test_dataset_version_ref_has_fixed_kind() -> None:
    ref = DatasetVersionRef(dataset_id=uuid4(), version_id=uuid4())

    assert ref.model_dump()["kind"] == "dataset_version"
