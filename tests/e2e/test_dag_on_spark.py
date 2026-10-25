"""E2E на kind: `Read → Filter → Aggregate → Publish Dataset` на настоящем Spark.

Запускает `scripts/e2e.sh`: он поднимает kind, Spark Operator, Control Plane и
Execution Controller, регистрирует блоки `read`, `filter` и `aggregate` и
передаёт адрес API в `E3_E2E_API_URL`. Без этой переменной тесты пропускаются.
"""

import asyncio
import io
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, cast
from uuid import UUID, uuid4

import boto3
import httpx
import pyarrow.parquet as pq
import pytest
from kubernetes_asyncio import config as kube_config
from kubernetes_asyncio.client import ApiClient, CustomObjectsApi

from e3.execution.controller import ExecutionController
from e3.execution.kubernetes import KubernetesSparkApplications
from e3.execution.settings import ExecutionSettings
from e3.execution.workload import ATTEMPT_LABEL, COMMAND_ANNOTATION, GROUP, PLURAL, VERSION
from e3.settings import Settings
from e3.shared.contracts import ExecutionContext, ExecutionResult
from e3.shared.manifest import Manifest
from e3.shared.messages import ExecutionEvent, parse_command
from tests.integration.conftest import Api
from tests.integration.graphs import dag_graph

API_URL = os.environ.get("E3_E2E_API_URL")

pytestmark = pytest.mark.skipif(API_URL is None, reason="E2E запускает scripts/e2e.sh")

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
RUN_TIMEOUT_SECONDS = 600
POLL_SECONDS = 1

NODE_CHAIN = ["PENDING", "QUEUED", "RUNNING", "FINALIZING", "SUCCEEDED"]
PUBLISH_CHAIN = ["PENDING", "RUNNING", "SUCCEEDED"]
RUN_CHAIN = ["PENDING", "RUNNING", "SUCCEEDED"]
# Состояния, которые длятся дольше опроса раз в POLL_SECONDS: QUEUED — пока
# Spark Operator поднимает driver, RUNNING — пока идёт Spark. Остальные
# (PENDING, FINALIZING, RUNNING у Publish Dataset) проходят за один цикл
# Run Orchestrator, и опрос через REST их может не застать.
BLOCK_LASTING = frozenset({"QUEUED", "RUNNING"})
RUN_LASTING = frozenset({"RUNNING"})


@dataclass
class Trace:
    """Наблюдённые смены состояний: каждое состояние — один раз, по порядку."""

    run: list[str] = field(default_factory=list[str])
    nodes: dict[str, list[str]] = field(default_factory=dict[str, list[str]])

    def note(self, run: dict[str, Any], nodes: dict[str, dict[str, Any]]) -> None:
        _append(self.run, run["state"])
        for node_id, node in nodes.items():
            _append(self.nodes.setdefault(node_id, []), node["state"])


@dataclass
class FinishedRun:
    api: Api
    run: dict[str, Any]
    nodes: dict[str, dict[str, Any]]
    trace: Trace


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


@pytest.fixture(scope="module")
async def finished() -> AsyncIterator[FinishedRun]:
    assert API_URL is not None
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        api = Api(client, Settings.model_fields["dev_project_id"].default)
        run_id = await _start(api)
        trace = await _follow(api, run_id)
        yield FinishedRun(api, await api.run(run_id), await api.run_nodes(run_id), trace)


def _graph() -> dict[str, Any]:
    graph = dag_graph(*BLOCKS)
    for node in graph["nodes"]:
        if node["kind"] == "BLOCK":
            node["parameters"] = PARAMETERS[node["id"]]
    return graph


async def _start(api: Api) -> str:
    pipeline_id = await api.new_pipeline(f"dag-on-spark-{uuid4()}")
    assert (await api.save_draft(pipeline_id, _graph())).status_code == 200
    published = await api.publish(pipeline_id)
    assert published.status_code == 201, published.text
    source = await api.new_dataset(f"source-{uuid4()}")
    uploaded = await api.upload_csv(source, CSV)
    assert uploaded.status_code == 200, uploaded.text
    target = await api.new_dataset(f"result-{uuid4()}")
    body = {
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
    started = await api.start_run(body, key=str(uuid4()))
    assert started.status_code == 202, started.text
    return started.json()["pipeline_run_id"]


async def _follow(api: Api, run_id: str) -> Trace:
    trace = Trace()
    async with asyncio.timeout(RUN_TIMEOUT_SECONDS):
        while True:
            run = await api.run(run_id)
            trace.note(run, await api.run_nodes(run_id))
            if run["state"] in {"SUCCEEDED", "FAILED"}:
                return trace
            await asyncio.sleep(POLL_SECONDS)


async def test_dag_on_spark_publishes_typed_aggregates(finished: FinishedRun) -> None:
    target = finished.run["outputs"]["result"]["dataset_id"]

    [version] = await finished.api.versions(UUID(target))

    assert finished.run["state"] == "SUCCEEDED", finished.nodes
    assert version["format"] == "PARQUET"
    assert version["schema"] == {
        "columns": [
            {"name": "city", "type": "string"},
            {"name": "total_qty", "type": "long"},
            {"name": "avg_price", "type": "double"},
        ]
    }
    assert version["version_id"] == finished.nodes["publish"]["published_version_id"]
    assert _rows(target, str(version["version_id"])) == [
        ("ekb", 2, 1.0),
        ("msk", 8, 6.0),
        ("spb", 7, 3.0),
    ]


def _rows(dataset_id: str, version_id: str) -> list[tuple[Any, ...]]:
    """Строки опубликованной версии прямо из MinIO стенда."""
    settings = Settings(environment="dev")
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    )
    prefix = f"datasets/{dataset_id}/{version_id}"

    def read(key: str) -> bytes:
        return s3.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()

    manifest = Manifest.model_validate_json(read(f"{prefix}/manifest.json"))
    rows: list[tuple[Any, ...]] = []
    for file in manifest.files:
        rows += _parquet_rows(read(f"{prefix}/{file.path}"))
    return sorted(rows)


def _parquet_rows(data: bytes) -> list[tuple[Any, ...]]:
    # У pyarrow нет типов для pyright.
    table: Any = pq.read_table(io.BytesIO(data))  # pyright: ignore[reportUnknownVariableType]
    return [tuple(row.values()) for row in cast("list[dict[str, Any]]", table.to_pylist())]


async def test_states_follow_same_chains_as_with_fake_controller(finished: FinishedRun) -> None:
    trace = finished.trace

    assert follows(trace.run, RUN_CHAIN, RUN_LASTING) and trace.run[-1] == "SUCCEEDED", trace
    for node_id in BLOCKS:
        assert follows(trace.nodes[node_id], NODE_CHAIN, BLOCK_LASTING), trace
        assert trace.nodes[node_id][-1] == "SUCCEEDED", trace
    assert follows(trace.nodes["publish"], PUBLISH_CHAIN), trace
    assert trace.nodes["publish"][-1] == "SUCCEEDED", trace


class _Unused:
    async def send(self, event: ExecutionEvent) -> None:
        raise AssertionError("повторная команда не шлёт событий")

    async def fetch(self, context: ExecutionContext) -> ExecutionResult | None:
        raise AssertionError("повторная команда не читает результат")


async def test_redelivered_submit_execution_leaves_single_spark_application(
    finished: FinishedRun,
) -> None:
    attempt_id = finished.nodes["read"]["attempt_id"]
    settings = ExecutionSettings()
    await kube_config.load_kube_config()
    api = ApiClient()
    try:
        objects = CustomObjectsApi(api)
        [application] = await _applications(objects, settings.spark_namespace, attempt_id)
        command = parse_command(application["metadata"]["annotations"][COMMAND_ANNOTATION].encode())
        controller = ExecutionController(
            KubernetesSparkApplications(objects, settings.spark_namespace),
            _Unused(),
            _Unused(),
            settings.workload(),
        )

        await controller.submit(command)

        [again] = await _applications(objects, settings.spark_namespace, attempt_id)
    finally:
        await api.close()
    assert again["metadata"]["uid"] == application["metadata"]["uid"]


async def _applications(
    objects: CustomObjectsApi, namespace: str, attempt_id: str
) -> list[dict[str, Any]]:
    listed: dict[str, Any] = await objects.list_namespaced_custom_object(
        GROUP, VERSION, namespace, PLURAL, label_selector=f"{ATTEMPT_LABEL}={attempt_id}"
    )
    return listed["items"]
