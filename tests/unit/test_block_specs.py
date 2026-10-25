"""BlockSpec из `blocks/*/block.yaml`: регистрируются и проверяют параметры узлов."""

from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

from e3.shared.contracts import BlockSpec

BLOCKS = Path(__file__).resolve().parents[2] / "blocks"
DIGEST = "registry.local/e3/block@sha256:" + "c" * 64


def spec(name: str) -> BlockSpec:
    body = yaml.safe_load((BLOCKS / name / "block.yaml").read_text())
    return BlockSpec.model_validate(body | {"executable_ref": DIGEST})


def accepts(name: str, parameters: dict[str, Any]) -> bool:
    return Draft202012Validator(spec(name).parameters_schema).is_valid(parameters)


CONDITION = {"column": "qty", "op": "gt", "value": 1}
AGGREGATION = {"column": "qty", "function": "sum", "as": "total"}


@pytest.mark.parametrize("name", ["read", "filter", "aggregate"])
def test_block_spec_is_valid(name: str) -> None:
    assert spec(name).name == name


@pytest.mark.parametrize(
    ("name", "parameters"),
    [
        ("read", {}),
        ("read", {"types": {"qty": "long", "price": "double"}}),
        ("filter", {"conditions": [CONDITION, {"column": "city", "op": "eq", "value": "msk"}]}),
        ("aggregate", {"aggregations": [AGGREGATION]}),
        ("aggregate", {"group_by": ["city"], "aggregations": [AGGREGATION]}),
    ],
)
def test_parameters_schema_accepts_agreed_shape(name: str, parameters: dict[str, Any]) -> None:
    assert accepts(name, parameters)


@pytest.mark.parametrize(
    ("name", "parameters"),
    [
        ("read", {"types": {"qty": "int"}}),
        ("filter", {}),
        ("filter", {"conditions": []}),
        ("filter", {"conditions": [CONDITION | {"op": "like"}]}),
        ("filter", {"conditions": [CONDITION | {"value": [1]}]}),
        ("filter", {"expression": "qty > 1"}),
        ("aggregate", {"aggregations": []}),
        ("aggregate", {"aggregations": [AGGREGATION | {"function": "median"}]}),
        ("aggregate", {"group_by": ["city", "city"], "aggregations": [AGGREGATION]}),
    ],
)
def test_parameters_schema_rejects_anything_else(name: str, parameters: dict[str, Any]) -> None:
    assert not accepts(name, parameters)
