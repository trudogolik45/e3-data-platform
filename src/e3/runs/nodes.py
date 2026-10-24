"""NodeRun из графа PipelineVersion и привязок слотов.

Входы каждого узла фиксируются при создании PipelineRun: Input Slot — точная
DatasetVersionRef, ребро от upstream — NodeOutputRef на его NodeRun.
"""

from collections.abc import Mapping
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import TypeAdapter

from e3.pipelines import BlockNode, BlockRef, InputSlotRef, NodePort, PipelineGraph
from e3.runs.bindings import RunRequest
from e3.runs.models import NodeRunRow
from e3.runs.progress import NodeProgress, NodeState
from e3.shared.contracts import BlockSpec, DataRef, DatasetVersionRef, NodeOutputRef

data_refs = TypeAdapter[dict[str, DataRef]](dict[str, DataRef])


class NodeKind(StrEnum):
    BLOCK = "BLOCK"
    SYSTEM = "SYSTEM"


def build_node_runs(
    pipeline_run_id: UUID,
    graph: PipelineGraph,
    request: RunRequest,
    specs: Mapping[BlockRef, BlockSpec],
) -> list[NodeRunRow]:
    ids = {node.id: uuid4() for node in graph.nodes}
    inputs = _wire_inputs(graph, request, ids)
    rows: list[NodeRunRow] = []
    for node in graph.nodes:
        row = NodeRunRow(
            id=ids[node.id],
            pipeline_run_id=pipeline_run_id,
            node_id=node.id,
            state=NodeState.PENDING,
            inputs=data_refs.dump_python(inputs[node.id], mode="json"),
            output_ports=[],
            parameters={},
        )
        if isinstance(node, BlockNode):
            spec = specs[node.block]
            row.kind = NodeKind.BLOCK
            row.parameters = node.parameters
            row.executable_ref = spec.executable_ref
            row.output_ports = [port.name for port in spec.outputs]
        else:
            row.kind = NodeKind.SYSTEM
            row.target_dataset_id = request.outputs[node.output_slot].dataset_id
        rows.append(row)
    return rows


def node_inputs(row: NodeRunRow) -> dict[str, DataRef]:
    return data_refs.validate_python(row.inputs)


def progress(row: NodeRunRow) -> NodeProgress:
    upstream = frozenset(
        ref.node_run_id for ref in node_inputs(row).values() if isinstance(ref, NodeOutputRef)
    )
    return NodeProgress(row.id, NodeState(row.state), upstream)


def _wire_inputs(
    graph: PipelineGraph, request: RunRequest, ids: Mapping[str, UUID]
) -> dict[str, dict[str, DataRef]]:
    inputs: dict[str, dict[str, DataRef]] = {node.id: {} for node in graph.nodes}
    for edge in graph.edges:
        inputs[edge.target.node][edge.target.port] = _source_ref(edge.source, request, ids)
    return inputs


def _source_ref(
    source: NodePort | InputSlotRef, request: RunRequest, ids: Mapping[str, UUID]
) -> DataRef:
    if isinstance(source, InputSlotRef):
        binding = request.inputs[source.slot]
        return DatasetVersionRef(dataset_id=binding.dataset_id, version_id=binding.version_id)
    return NodeOutputRef(node_run_id=ids[source.node], port=source.port)
