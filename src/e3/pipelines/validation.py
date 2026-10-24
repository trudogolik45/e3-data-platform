"""Базовая валидация графа перед публикацией PipelineVersion.

Первое нарушенное правило становится причиной отказа: `GraphInvalid`.
"""

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter

from e3.pipelines.graph import (
    PUBLISH_INPUT_PORT,
    BlockNode,
    BlockRef,
    InputSlotRef,
    Node,
    NodePort,
    PipelineGraph,
)
from e3.shared.contracts import BlockSpec


class GraphInvalid(Exception):
    """Draft нельзя опубликовать: текст исключения — причина для пользователя."""


@dataclass(frozen=True)
class _Ports:
    inputs: dict[str, bool]  # имя → обязательный
    outputs: frozenset[str]


def validate_graph(graph: PipelineGraph, specs: Mapping[BlockRef, BlockSpec]) -> None:
    _check_unique_node_ids(graph)
    _check_unique_slot_names(graph)
    _check_blocks_exist(graph, specs)
    _check_publish_slots(graph)
    _check_one_publish_per_slot(graph)
    ports = {node.id: _ports(node, specs) for node in graph.nodes}
    _check_edges(graph, ports)
    _check_required_inputs(graph, ports)
    _check_acyclic(graph)


def _check_unique_node_ids(graph: PipelineGraph) -> None:
    repeated = _repeated(node.id for node in graph.nodes)
    if repeated:
        raise GraphInvalid(f"повторяются id узлов: {', '.join(repeated)}")


def _check_unique_slot_names(graph: PipelineGraph) -> None:
    repeated = _repeated(slot.name for slot in [*graph.input_slots, *graph.output_slots])
    if repeated:
        raise GraphInvalid(f"повторяются имена слотов: {', '.join(repeated)}")


def _check_blocks_exist(graph: PipelineGraph, specs: Mapping[BlockRef, BlockSpec]) -> None:
    missing = sorted(f"{ref.name}:{ref.version}" for ref in graph.block_refs() - specs.keys())
    if missing:
        raise GraphInvalid(f"нет таких версий блоков: {', '.join(missing)}")


def _check_publish_slots(graph: PipelineGraph) -> None:
    output_slots = {slot.name for slot in graph.output_slots}
    for node in graph.nodes:
        if isinstance(node, BlockNode) or node.output_slot in output_slots:
            continue
        raise GraphInvalid(f"Publish Dataset {node.id}: нет Output Slot {node.output_slot}")


def _check_one_publish_per_slot(graph: PipelineGraph) -> None:
    repeated = _repeated(
        node.output_slot for node in graph.nodes if not isinstance(node, BlockNode)
    )
    if repeated:
        raise GraphInvalid(
            f"в Output Slot публикуют несколько Publish Dataset: {', '.join(repeated)}"
        )


def _ports(node: Node, specs: Mapping[BlockRef, BlockSpec]) -> _Ports:
    if not isinstance(node, BlockNode):
        return _Ports({PUBLISH_INPUT_PORT: True}, frozenset())
    spec = specs[node.block]
    return _Ports(
        {port.name: port.required for port in spec.inputs},
        frozenset(port.name for port in spec.outputs),
    )


def _check_edges(graph: PipelineGraph, ports: Mapping[str, _Ports]) -> None:
    input_slots = {slot.name for slot in graph.input_slots}
    for edge in graph.edges:
        if isinstance(edge.source, InputSlotRef):
            if edge.source.slot not in input_slots:
                raise GraphInvalid(f"ребро из несуществующего Input Slot {edge.source.slot}")
        else:
            _check_port(edge.source, ports, inputs=False)
        _check_port(edge.target, ports, inputs=True)


def _check_port(endpoint: NodePort, ports: Mapping[str, _Ports], *, inputs: bool) -> None:
    node_ports = ports.get(endpoint.node)
    if node_ports is None:
        raise GraphInvalid(f"ребро ссылается на несуществующий узел {endpoint.node}")
    names = node_ports.inputs.keys() if inputs else node_ports.outputs
    if endpoint.port not in names:
        kind = "входа" if inputs else "выхода"
        raise GraphInvalid(f"у узла {endpoint.node} нет {kind} {endpoint.port}")


def _check_required_inputs(graph: PipelineGraph, ports: Mapping[str, _Ports]) -> None:
    connected = {(edge.target.node, edge.target.port) for edge in graph.edges}
    for node_id, node_ports in ports.items():
        for name, required in node_ports.inputs.items():
            if required and (node_id, name) not in connected:
                raise GraphInvalid(f"не подключён обязательный вход {node_id}.{name}")


def _check_acyclic(graph: PipelineGraph) -> None:
    sorter = TopologicalSorter[str]({node.id: set[str]() for node in graph.nodes})
    for edge in graph.edges:
        if isinstance(edge.source, NodePort):
            sorter.add(edge.target.node, edge.source.node)
    try:
        sorter.prepare()
    except CycleError as error:
        cycle: list[str] = error.args[1]
        raise GraphInvalid(f"граф содержит цикл: {' → '.join(cycle)}") from None


def _repeated(names: Iterable[str]) -> list[str]:
    return sorted(name for name, count in Counter(names).items() if count > 1)
