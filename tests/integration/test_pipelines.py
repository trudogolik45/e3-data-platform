from collections.abc import Callable
from typing import Any
from uuid import UUID, uuid4

import pytest

from tests.integration.conftest import Api

pytestmark = pytest.mark.integration

Graph = dict[str, Any]


def port(name: str, *, required: bool = True) -> dict[str, Any]:
    return {"name": name, "data_type": "dataset", "required": required}


def block_spec(name: str, inputs: list[dict[str, Any]], outputs: list[dict[str, Any]]) -> Graph:
    return {
        "name": name,
        "version": "1.0.0",
        "runtime": "spark",
        "inputs": inputs,
        "outputs": outputs,
        "parameters_schema": {},
        "executable_ref": "registry.local/e3/" + name + "@sha256:" + "e" * 64,
    }


@pytest.fixture
async def filter_block(api: Api) -> str:
    """Блок `source → result` и необязательный вход `hint`."""
    name = f"filter-{uuid4().hex}"
    spec = block_spec(name, [port("source"), port("hint", required=False)], [port("result")])
    response = await api.register_block(spec)
    assert response.status_code == 201, response.text
    return name


def block_node(node_id: str, block: str, version: str = "1.0.0") -> Graph:
    return {"kind": "BLOCK", "id": node_id, "block": {"name": block, "version": version}}


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


def linear_graph(block: str) -> Graph:
    """`source → filter → Publish Dataset(result)`."""
    return {
        "input_slots": [{"name": "source"}],
        "output_slots": [{"name": "result"}],
        "nodes": [block_node("filter", block), publish_node("publish", "result")],
        "edges": [
            edge(slot("source"), at("filter", "source")),
            edge(at("filter", "result"), at("publish", "data")),
        ],
    }


async def test_publishing_draft_numbers_versions_within_pipeline(
    api: Api, filter_block: str
) -> None:
    pipeline_id = await api.new_pipeline("sales")
    await api.save_draft(pipeline_id, linear_graph(filter_block))

    numbers = [(await api.publish(pipeline_id)).json()["version_number"] for _ in range(3)]

    assert numbers == [1, 2, 3]


async def test_editing_draft_does_not_change_published_version(api: Api, filter_block: str) -> None:
    pipeline_id = await api.new_pipeline("sales")
    published = linear_graph(filter_block)
    await api.save_draft(pipeline_id, published)
    await api.publish(pipeline_id)

    edited = linear_graph(filter_block) | {"input_slots": [{"name": "source"}, {"name": "extra"}]}
    saved = await api.save_draft(pipeline_id, edited)
    version = await api.pipeline_version(pipeline_id, 1)

    assert saved.status_code == 200, saved.text
    assert version.status_code == 200, version.text
    assert [s["name"] for s in version.json()["graph"]["input_slots"]] == ["source"]


async def test_pipeline_in_foreign_project_is_forbidden(api: Api) -> None:
    response = await api.create_pipeline("sales", project_id=uuid4())

    assert response.status_code == 403


async def test_draft_of_unknown_pipeline_is_not_found(api: Api) -> None:
    response = await api.save_draft(uuid4(), {})

    assert response.status_code == 404


def with_cycle(block: str) -> Graph:
    graph = linear_graph(block)
    graph["nodes"].append(block_node("again", block))
    graph["edges"] += [
        edge(at("filter", "result"), at("again", "source")),
        edge(at("again", "result"), at("filter", "hint")),
    ]
    return graph


def with_unknown_block_version(block: str) -> Graph:
    graph = linear_graph(block)
    graph["nodes"][0] = block_node("filter", block, version="9.9.9")
    return graph


def with_edge_to_missing_port(block: str) -> Graph:
    graph = linear_graph(block)
    graph["edges"].append(edge(slot("source"), at("filter", "no_such_port")))
    return graph


def with_edge_from_missing_node(block: str) -> Graph:
    graph = linear_graph(block)
    graph["edges"][1] = edge(at("ghost", "result"), at("publish", "data"))
    return graph


def with_edge_from_missing_slot(block: str) -> Graph:
    graph = linear_graph(block)
    graph["edges"][0] = edge(slot("ghost"), at("filter", "source"))
    return graph


def with_required_input_unconnected(block: str) -> Graph:
    graph = linear_graph(block)
    del graph["edges"][0]
    return graph


def with_duplicate_slot_names(block: str) -> Graph:
    graph = linear_graph(block)
    graph["output_slots"].append({"name": "source"})
    return graph


def with_publish_to_missing_output_slot(block: str) -> Graph:
    graph = linear_graph(block)
    graph["nodes"][1] = publish_node("publish", "ghost")
    return graph


def with_unconnected_publish(block: str) -> Graph:
    graph = linear_graph(block)
    del graph["edges"][1]
    return graph


def with_duplicate_node_ids(block: str) -> Graph:
    graph = linear_graph(block)
    graph["nodes"].append(block_node("filter", block))
    return graph


@pytest.mark.parametrize(
    ("broken", "reason"),
    [
        (with_cycle, "цикл"),
        (with_unknown_block_version, "9.9.9"),
        (with_edge_to_missing_port, "no_such_port"),
        (with_edge_from_missing_node, "ghost"),
        (with_edge_from_missing_slot, "ghost"),
        (with_required_input_unconnected, "source"),
        (with_duplicate_slot_names, "source"),
        (with_publish_to_missing_output_slot, "ghost"),
        (with_unconnected_publish, "data"),
        (with_duplicate_node_ids, "filter"),
    ],
)
async def test_invalid_draft_is_not_published(
    api: Api, filter_block: str, broken: Callable[[str], Graph], reason: str
) -> None:
    pipeline_id = await api.new_pipeline("sales")
    saved = await api.save_draft(pipeline_id, broken(filter_block))

    response = await api.publish(pipeline_id)

    assert saved.status_code == 200, saved.text
    assert response.status_code == 422
    assert reason in response.json()["detail"]
    await assert_no_versions(api, pipeline_id)


async def assert_no_versions(api: Api, pipeline_id: UUID) -> None:
    response = await api.pipeline_version(pipeline_id, 1)
    assert response.status_code == 404
