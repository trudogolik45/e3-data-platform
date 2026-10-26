"""JSON Schema контрактов Backend ↔ DS и сообщений Kafka: `n3-contracts-schema`."""

import json
from typing import Any

from pydantic import TypeAdapter

from n3.shared.contracts import (
    BlockSpec,
    DataRef,
    DatasetVersionRef,
    ExecutionContext,
    ExecutionResult,
    NodeOutputRef,
    PortSpec,
    ResolvedInput,
)
from n3.shared.manifest import Manifest
from n3.shared.messages import (
    CancelExecution,
    ExecutionCommand,
    ExecutionEvent,
    ReconcileExecution,
    SubmitExecution,
)

CONTRACTS: dict[str, Any] = {
    "DatasetVersionRef": DatasetVersionRef,
    "NodeOutputRef": NodeOutputRef,
    "DataRef": DataRef,
    "ResolvedInput": ResolvedInput,
    "PortSpec": PortSpec,
    "BlockSpec": BlockSpec,
    "ExecutionContext": ExecutionContext,
    "ExecutionResult": ExecutionResult,
    "Manifest": Manifest,
    "SubmitExecution": SubmitExecution,
    "ReconcileExecution": ReconcileExecution,
    "CancelExecution": CancelExecution,
    "ExecutionCommand": ExecutionCommand,
    "ExecutionEvent": ExecutionEvent,
}


def json_schemas() -> dict[str, dict[str, Any]]:
    return {name: TypeAdapter[Any](contract).json_schema() for name, contract in CONTRACTS.items()}


def print_json_schema() -> None:
    print(json.dumps(json_schemas(), ensure_ascii=False, indent=2))
