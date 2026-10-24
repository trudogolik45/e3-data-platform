"""E2E на kind: `Read → Publish Dataset` на настоящем Spark через REST API.

Запускает `scripts/e2e.sh`: он поднимает kind, Spark Operator, Control Plane и
Execution Controller, регистрирует блок `Read` и передаёт адрес API в
`E3_E2E_API_URL`. Без этой переменной тесты пропускаются.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from kubernetes_asyncio import config as kube_config
from kubernetes_asyncio.client import ApiClient, CustomObjectsApi

from e3.execution.controller import ExecutionController
from e3.execution.kubernetes import KubernetesSparkApplications
from e3.execution.settings import ExecutionSettings
from e3.execution.workload import ATTEMPT_LABEL, COMMAND_ANNOTATION, GROUP, PLURAL, VERSION
from e3.settings import Settings
from e3.shared.contracts import ExecutionContext, ExecutionResult
from e3.shared.messages import ExecutionEvent, parse_command
from tests.integration.conftest import Api
from tests.integration.graphs import read_graph

API_URL = os.environ.get("E3_E2E_API_URL")
BLOCK = os.environ.get("E3_E2E_BLOCK", "read")

pytestmark = pytest.mark.skipif(API_URL is None, reason="E2E запускает scripts/e2e.sh")

CSV = b"id,name\n1,alpha\n2,beta\n3,gamma\n"
RUN_TIMEOUT_SECONDS = 600
POLL_SECONDS = 1

NODE_CHAIN = ["PENDING", "QUEUED", "RUNNING", "FINALIZING", "SUCCEEDED"]
PUBLISH_CHAIN = ["PENDING", "RUNNING", "SUCCEEDED"]
RUN_CHAIN = ["PENDING", "RUNNING", "SUCCEEDED"]
# Состояния, которые длятся дольше опроса раз в POLL_SECONDS: QUEUED — пока
# Spark Operator поднимает driver, RUNNING — пока идёт Spark. Остальные
# (PENDING, FINALIZING, RUNNING у Publish Dataset) проходят за один цикл
# Run Orchestrator, и опрос через REST их может не застать.
READ_LASTING = frozenset({"QUEUED", "RUNNING"})
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


async def _start(api: Api) -> str:
    pipeline_id = await api.new_pipeline(f"read-on-spark-{uuid4()}")
    assert (await api.save_draft(pipeline_id, read_graph(BLOCK))).status_code == 200
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


async def test_read_on_spark_publishes_parquet_dataset_version(finished: FinishedRun) -> None:
    target = UUID(finished.run["outputs"]["result"]["dataset_id"])

    [version] = await finished.api.versions(target)

    assert finished.run["state"] == "SUCCEEDED", finished.nodes
    assert version["format"] == "PARQUET"
    assert version["schema"] == {
        "columns": [{"name": "id", "type": "string"}, {"name": "name", "type": "string"}]
    }
    assert version["version_id"] == finished.nodes["publish"]["published_version_id"]


async def test_states_follow_same_chains_as_with_fake_controller(finished: FinishedRun) -> None:
    trace = finished.trace

    assert follows(trace.run, RUN_CHAIN, RUN_LASTING) and trace.run[-1] == "SUCCEEDED", trace
    assert follows(trace.nodes["read"], NODE_CHAIN, READ_LASTING), trace
    assert trace.nodes["read"][-1] == "SUCCEEDED", trace
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
