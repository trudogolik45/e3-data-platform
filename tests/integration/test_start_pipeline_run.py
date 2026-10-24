from typing import Any
from uuid import uuid4

import pytest

from tests.integration.conftest import Api
from tests.integration.graphs import block_spec, port, read_graph

pytestmark = pytest.mark.integration

CSV = b"id,name\n1,alpha\n2,beta\n"


async def new_read_pipeline(api: Api) -> tuple[str, int]:
    block = f"read-{uuid4().hex}"
    registered = await api.register_block(block_spec(block, [port("source")], [port("data")]))
    assert registered.status_code == 201, registered.text
    pipeline_id = await api.new_pipeline("read")
    await api.save_draft(pipeline_id, read_graph(block))
    published = await api.publish(pipeline_id)
    assert published.status_code == 201, published.text
    return str(pipeline_id), published.json()["version_number"]


async def run_request(api: Api) -> dict[str, Any]:
    """Тело `POST /pipeline-runs` для `source → Read → Publish Dataset(result)`."""
    pipeline_id, version_number = await new_read_pipeline(api)
    source = await api.new_dataset(f"source-{uuid4()}")
    uploaded = await api.upload_csv(source, CSV)
    assert uploaded.status_code == 200, uploaded.text
    target = await api.new_dataset(f"result-{uuid4()}")
    return {
        "pipeline_id": pipeline_id,
        "pipeline_version": version_number,
        "inputs": {
            "source": {
                "dataset_id": str(source),
                "version_id": uploaded.json()["version"]["version_id"],
            }
        },
        "outputs": {"result": {"dataset_id": str(target)}},
    }


async def test_run_is_accepted_as_pending_with_exact_bindings(api: Api) -> None:
    body = await run_request(api)

    response = await api.start_run(body, key=str(uuid4()))

    assert response.status_code == 202, response.text
    run = response.json()
    assert run["state"] == "PENDING"
    assert run["status_url"] == f"/api/v1/pipeline-runs/{run['pipeline_run_id']}"
    assert run["inputs"] == body["inputs"]
    assert run["outputs"] == body["outputs"]


async def test_run_status_and_nodes_start_pending(api: Api) -> None:
    started = (await api.start_run(await run_request(api), key=str(uuid4()))).json()

    run = await api.run(started["pipeline_run_id"])
    nodes = await api.run_nodes(started["pipeline_run_id"])

    assert run["state"] == "PENDING"
    assert {node_id: node["state"] for node_id, node in nodes.items()} == {
        "read": "PENDING",
        "publish": "PENDING",
    }


async def test_repeated_request_with_same_key_returns_same_run(api: Api) -> None:
    body, key = await run_request(api), str(uuid4())

    first = await api.start_run(body, key=key)
    second = await api.start_run(body, key=key)

    assert second.status_code == 202, second.text
    assert second.json()["pipeline_run_id"] == first.json()["pipeline_run_id"]


async def test_same_key_with_different_body_is_conflict(api: Api) -> None:
    body, key = await run_request(api), str(uuid4())
    await api.start_run(body, key=key)

    response = await api.start_run(
        body | {"outputs": {"result": {"dataset_id": str(uuid4())}}}, key
    )

    assert response.status_code == 409


async def test_run_without_idempotency_key_is_rejected(api: Api) -> None:
    response = await api.start_run(await run_request(api), key=None)

    assert response.status_code == 422


def without_input(body: dict[str, Any]) -> dict[str, Any]:
    return body | {"inputs": {}}


def with_unknown_slot(body: dict[str, Any]) -> dict[str, Any]:
    return body | {"outputs": body["outputs"] | {"ghost": body["outputs"]["result"]}}


def with_missing_version(body: dict[str, Any]) -> dict[str, Any]:
    source = body["inputs"]["source"] | {"version_id": str(uuid4())}
    return body | {"inputs": {"source": source}}


def with_missing_output_dataset(body: dict[str, Any]) -> dict[str, Any]:
    return body | {"outputs": {"result": {"dataset_id": str(uuid4())}}}


def with_missing_pipeline_version(body: dict[str, Any]) -> dict[str, Any]:
    return body | {"pipeline_version": 99}


@pytest.mark.parametrize(
    ("broken", "reason"),
    [
        (without_input, "source"),
        (with_unknown_slot, "ghost"),
        (with_missing_version, "source"),
        (with_missing_output_dataset, "result"),
        (with_missing_pipeline_version, "99"),
    ],
)
async def test_run_with_unresolvable_bindings_is_rejected(
    api: Api, broken: Any, reason: str
) -> None:
    response = await api.start_run(broken(await run_request(api)), key=str(uuid4()))

    assert response.status_code == 422, response.text
    assert reason in response.json()["detail"]


async def test_run_of_unknown_pipeline_is_not_found(api: Api) -> None:
    body = await run_request(api) | {"pipeline_id": str(uuid4())}

    response = await api.start_run(body, key=str(uuid4()))

    assert response.status_code == 404
