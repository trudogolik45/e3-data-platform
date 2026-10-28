"""Строители графов и BlockSpec для integration-тестов."""

from typing import Any

Graph = dict[str, Any]


def port(name: str, *, required: bool = True, data_type: str = "dataset") -> dict[str, Any]:
    return {"name": name, "data_type": data_type, "required": required}


def block_spec(
    name: str,
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    parameters_schema: dict[str, Any] | None = None,
) -> Graph:
    return {
        "name": name,
        "version": "1.0.0",
        "runtime": "spark",
        "inputs": inputs,
        "outputs": outputs,
        "parameters_schema": parameters_schema or {},
        "executable_ref": "registry.local/e3/" + name + "@sha256:" + "e" * 64,
    }


def block_node(
    node_id: str, block: str, version: str = "1.0.0", parameters: dict[str, Any] | None = None
) -> Graph:
    return {
        "kind": "BLOCK",
        "id": node_id,
        "block": {"name": block, "version": version},
        "parameters": parameters or {},
    }


def publish_node(node_id: str, output_slot: str) -> Graph:
    return {
        "kind": "SYSTEM",
        "id": node_id,
        "system": "publish_dataset",
        "output_slot": output_slot,
    }


def edge(source: Graph, target: Graph) -> Graph:
    return {"source": source, "target": target}


def at(node: str, port_name: str) -> Graph:
    return {"node": node, "port": port_name}


def slot(name: str) -> Graph:
    return {"slot": name}


def read_graph(block: str) -> Graph:
    """`source → Read → Publish Dataset(result)`."""
    return {
        "input_slots": [{"name": "source"}],
        "output_slots": [{"name": "result"}],
        "nodes": [block_node("read", block), publish_node("publish", "result")],
        "edges": [
            edge(slot("source"), at("read", "source")),
            edge(at("read", "data"), at("publish", "data")),
        ],
    }


def join_graph(block: str) -> Graph:
    """`left`, `right` → Join → Publish Dataset(result): у одного узла два входа-слота."""
    return {
        "input_slots": [{"name": "left"}, {"name": "right"}],
        "output_slots": [{"name": "result"}],
        "nodes": [block_node("join", block), publish_node("publish", "result")],
        "edges": [
            edge(slot("left"), at("join", "left")),
            edge(slot("right"), at("join", "right")),
            edge(at("join", "data"), at("publish", "data")),
        ],
    }


def read_beside_side_graph(read: str, side: str) -> Graph:
    """`source → Read → Publish Dataset(result)` и рядом независимый `source → Side`."""
    graph = read_graph(read)
    graph["nodes"].append(block_node("side", side))
    graph["edges"].append(edge(slot("source"), at("side", "source")))
    return graph


def read_then_left_and_right_graph(read: str, left: str, right: str) -> Graph:
    """`source → Read → Publish Dataset(result)`, и выход Read читают ещё Left и Right."""
    graph = read_graph(read)
    graph["nodes"] += [block_node("left", left), block_node("right", right)]
    graph["edges"] += [
        edge(at("read", "data"), at("left", "source")),
        edge(at("read", "data"), at("right", "source")),
    ]
    return graph


def read_filter_aggregate_graph(read: str, filter_block: str, aggregate: str) -> Graph:
    """`source → Read → Filter → Aggregate → Publish Dataset(result)`."""
    return {
        "input_slots": [{"name": "source"}],
        "output_slots": [{"name": "result"}],
        "nodes": [
            block_node("read", read),
            block_node("filter", filter_block),
            block_node("aggregate", aggregate),
            publish_node("publish", "result"),
        ],
        "edges": [
            edge(slot("source"), at("read", "source")),
            edge(at("read", "data"), at("filter", "source")),
            edge(at("filter", "result"), at("aggregate", "source")),
            edge(at("aggregate", "result"), at("publish", "data")),
        ],
    }
