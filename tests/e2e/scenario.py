"""Сценарий PRD §11 на стенде: `Read → Filter → Aggregate → Publish Dataset`.

Запуск, наблюдение за сменами состояний через REST и строки опубликованной
DatasetVersion прямо из MinIO стенда.
"""

import asyncio
import io
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, cast
from uuid import UUID, uuid4

import boto3
import pyarrow.parquet as pq

from e3.settings import Settings
from e3.shared.manifest import Manifest
from tests.integration.conftest import Api
from tests.integration.graphs import read_filter_aggregate_graph

CSV = b"city,qty,price\nmsk,5,10.0\nmsk,1,99.5\nspb,7,3.0\nmsk,3,2.0\nekb,2,1.0\n"
PARAMETERS = {
    "read": {"types": {"qty": "long", "price": "double"}},
    "filter": {"conditions": [{"column": "qty", "op": "gt", "value": 1}]},
    "aggregate": {
        "group_by": ["city"],
        "aggregations": [
            {"column": "qty", "function": "sum", "as": "total_qty"},
            {"column": "price", "function": "avg", "as": "avg_price"},
        ],
    },
}
BLOCKS = list(PARAMETERS)
# Агрегаты CSV после фильтра `qty > 1`, посчитанные вручную.
AGGREGATES = [("ekb", 2, 1.0), ("msk", 8, 6.0), ("spb", 7, 3.0)]
RUN_TIMEOUT_SECONDS = 600
POLL_SECONDS = 1
FINISHED = frozenset({"SUCCEEDED", "FAILED"})

NODE_CHAIN = ["PENDING", "QUEUED", "RUNNING", "FINALIZING", "SUCCEEDED"]
PUBLISH_CHAIN = ["PENDING", "RUNNING", "SUCCEEDED"]
RUN_CHAIN = ["PENDING", "RUNNING", "SUCCEEDED"]
# Состояния, которые длятся дольше опроса раз в POLL_SECONDS: QUEUED — пока
# Spark Operator поднимает driver, RUNNING — пока идёт Spark. Остальные
# (PENDING, FINALIZING, RUNNING у Publish Dataset) проходят за один цикл
# Run Orchestrator, и опрос через REST их может не застать.
BLOCK_LASTING = frozenset({"QUEUED", "RUNNING"})
RUN_LASTING = frozenset({"RUNNING"})

Nodes = dict[str, dict[str, Any]]


@dataclass
class Trace:
    """Наблюдённые смены состояний: каждое состояние — один раз, по порядку."""

    run: list[str] = field(default_factory=list[str])
    nodes: dict[str, list[str]] = field(default_factory=dict[str, list[str]])

    def note(self, run: dict[str, Any], nodes: Nodes) -> None:
        _append(self.run, run["state"])
        for node_id, node in nodes.items():
            _append(self.nodes.setdefault(node_id, []), node["state"])


def _append(states: list[str], state: str) -> None:
    if not states or states[-1] != state:
        states.append(state)


def follows(observed: list[str], chain: list[str], lasting: frozenset[str] = frozenset()) -> bool:
    """`observed` идёт по `chain` вперёд и пропускает только мимолётные состояния.

    Каждое состояние из `lasting` должно быть замечено.
    """
    remaining = iter(chain)
    in_order = all(state in remaining for state in observed)
    return in_order and lasting <= set(observed)


@dataclass
class Started:
    """Принятый `POST /pipeline-runs`: тело и ключ, чтобы его повторить."""

    api: Api
    body: dict[str, Any]
    key: str
    run_id: str


async def start(api: Api, csv: bytes = CSV) -> Started:
    body = await _request(api, csv)
    key = str(uuid4())
    started = await api.start_run(body, key=key)
    assert started.status_code == 202, started.text
    return Started(api, body, key, started.json()["pipeline_run_id"])


async def _request(api: Api, csv: bytes) -> dict[str, Any]:
    pipeline_id = await api.new_pipeline(f"read-filter-aggregate-on-spark-{uuid4()}")
    assert (await api.save_draft(pipeline_id, _graph())).status_code == 200
    published = await api.publish(pipeline_id)
    assert published.status_code == 201, published.text
    source = await api.new_dataset(f"source-{uuid4()}")
    uploaded = await api.upload_csv(source, csv)
    assert uploaded.status_code == 200, uploaded.text
    target = await api.new_dataset(f"result-{uuid4()}")
    return {
        "pipeline_id": str(pipeline_id),
        "pipeline_version": published.json()["version_number"],
        "inputs": {
            "source": {
                "dataset_id": str(source),
                "version_id": uploaded.json()["version"]["version_id"],
            }
        },
        "outputs": {"result": {"dataset_id": str(target)}},
    }


def _graph() -> dict[str, Any]:
    graph = read_filter_aggregate_graph(*BLOCKS)
    for node in graph["nodes"]:
        if node["kind"] == "BLOCK":
            node["parameters"] = PARAMETERS[node["id"]]
    return graph


async def follow(
    api: Api,
    run_id: str,
    trace: Trace,
    done: Callable[[dict[str, Any], Nodes], bool] = lambda run, _: run["state"] in FINISHED,
) -> None:
    """Опрашивает запуск и пишет смены состояний в `trace`, пока не `done`."""
    async with asyncio.timeout(RUN_TIMEOUT_SECONDS):
        while True:
            run, nodes = await api.run(run_id), await api.run_nodes(run_id)
            trace.note(run, nodes)
            if done(run, nodes):
                return
            await asyncio.sleep(POLL_SECONDS)


def rows(dataset_id: str, version_id: str) -> list[tuple[Any, ...]]:
    """Строки опубликованной версии прямо из MinIO стенда."""
    settings = Settings(environment="dev")  # pyright: ignore[reportCallIssue]
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key.get_secret_value(),
        aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
        region_name=settings.s3_region,
    )
    prefix = f"datasets/{dataset_id}/{version_id}"

    def read(key: str) -> bytes:
        return s3.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()

    manifest = Manifest.model_validate_json(read(f"{prefix}/manifest.json"))
    found: list[tuple[Any, ...]] = []
    for file in manifest.files:
        found += _parquet_rows(read(f"{prefix}/{file.path}"))
    return sorted(found)


def _parquet_rows(data: bytes) -> list[tuple[Any, ...]]:
    # У pyarrow нет типов для pyright.
    table: Any = pq.read_table(io.BytesIO(data))  # pyright: ignore[reportUnknownVariableType]
    return [tuple(row.values()) for row in cast("list[dict[str, Any]]", table.to_pylist())]


async def published_version(api: Api, run: dict[str, Any]) -> dict[str, Any]:
    """Единственная DatasetVersion Output Slot `result`: вторая — дубль публикации."""
    [version] = await api.versions(UUID(run["outputs"]["result"]["dataset_id"]))
    return cast("dict[str, Any]", version)
