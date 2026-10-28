"""E2E на kind: `Read → Filter → Aggregate → Publish Dataset` на настоящем Spark.

Запускает `scripts/e2e.sh`: он поднимает kind, Spark Operator, Control Plane и
Execution Controller, регистрирует блоки `read`, `filter` и `aggregate` и
передаёт адрес API в `E3_E2E_API_URL`. Без этой переменной тесты пропускаются.
"""

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
from kubernetes_asyncio import config as kube_config
from kubernetes_asyncio.client import ApiClient, CoreV1Api, CustomObjectsApi

from e3.execution.controller import ExecutionController
from e3.execution.kubernetes import KubernetesSparkApplications
from e3.execution.main import attempt_credentials
from e3.execution.settings import ExecutionSettings
from e3.execution.workload import ATTEMPT_LABEL, COMMAND_ANNOTATION, GROUP, PLURAL, VERSION
from e3.shared.contracts import ExecutionContext, ExecutionError, ExecutionResult
from e3.shared.messages import ExecutionEvent, parse_command
from tests.e2e.cluster import attempt_logs, attempt_secret, secret
from tests.e2e.identity import project_api
from tests.e2e.scenario import (
    AGGREGATES,
    BLOCK_LASTING,
    BLOCKS,
    NODE_CHAIN,
    PUBLISH_CHAIN,
    RUN_CHAIN,
    RUN_LASTING,
    Nodes,
    Trace,
    follow,
    follows,
    published_version,
    rows,
    start,
)
from tests.integration.conftest import Api

API_URL = os.environ.get("E3_E2E_API_URL")

pytestmark = pytest.mark.skipif(API_URL is None, reason="E2E запускает scripts/e2e.sh")


@dataclass
class FinishedRun:
    api: Api
    run: dict[str, Any]
    nodes: Nodes
    trace: Trace


@pytest.fixture(scope="module")
async def finished() -> AsyncIterator[FinishedRun]:
    assert API_URL is not None
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        api = await project_api(client)
        run_id = (await start(api)).run_id
        trace = Trace()
        await follow(api, run_id, trace)
        yield FinishedRun(api, await api.run(run_id), await api.run_nodes(run_id), trace)


async def test_spark_publishes_typed_aggregates(finished: FinishedRun) -> None:
    target = finished.run["outputs"]["result"]["dataset_id"]

    version = await published_version(finished.api, finished.run)

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
    assert rows(target, str(version["version_id"])) == AGGREGATES


async def test_value_that_does_not_cast_fails_read_once_with_column_type_and_logs() -> None:
    assert API_URL is not None
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        api = await project_api(client)
        run_id = (await start(api, b"city,qty,price\nmsk,hello,10.0\n")).run_id
        await follow(api, run_id, Trace())
        read = (await api.run_nodes(run_id))["read"]
    [attempt] = read["attempts"]
    logs = await attempt_logs(
        read["node_run_id"], attempt["attempt_id"], "driver.log", "executor-1.log"
    )

    assert read["state"] == "FAILED"
    assert read["error"].startswith("колонка qty: значения не приводятся к long (логи driver: ")
    assert f"/e3-{attempt['attempt_id']}, attempt_id {attempt['attempt_id']}" in read["error"]
    assert "hello" not in read["error"]
    assert (attempt["state"], attempt["reason"]) == ("FAILED", "BLOCK_FAILED")
    assert logs["driver.log"] and logs["executor-1.log"]


async def test_states_follow_same_chains_as_with_fake_controller(finished: FinishedRun) -> None:
    trace = finished.trace

    assert follows(trace.run, RUN_CHAIN, RUN_LASTING) and trace.run[-1] == "SUCCEEDED", trace
    for node_id in BLOCKS:
        assert follows(trace.nodes[node_id], NODE_CHAIN, BLOCK_LASTING), trace
        assert trace.nodes[node_id][-1] == "SUCCEEDED", trace
    assert follows(trace.nodes["publish"], PUBLISH_CHAIN), trace
    assert trace.nodes["publish"][-1] == "SUCCEEDED", trace


async def test_each_succeeded_attempt_leaves_driver_and_executor_logs(
    finished: FinishedRun,
) -> None:
    logs = {
        block: await attempt_logs(
            finished.nodes[block]["node_run_id"],
            finished.nodes[block]["attempt_id"],
            "driver.log",
            "executor-1.log",
        )
        for block in BLOCKS
    }

    for block, files in logs.items():
        assert "SparkContext" in files["driver.log"].decode(), block
        assert files["executor-1.log"], block


class _Unused:
    async def send(self, event: ExecutionEvent) -> None:
        raise AssertionError("повторная команда не шлёт событий")

    async def fetch(self, context: ExecutionContext) -> ExecutionResult | None:
        raise AssertionError("повторная команда не читает результат")

    async def fetch_error(self, context: ExecutionContext) -> ExecutionError | None:
        raise AssertionError("повторная команда не читает ошибку блока")

    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        raise AssertionError("повторная команда не пишет Diagnostic Artifact")


async def test_each_attempt_ran_on_own_keys_owned_by_its_spark_application(
    finished: FinishedRun,
) -> None:
    settings = ExecutionSettings()  # pyright: ignore[reportCallIssue]
    await kube_config.load_kube_config()
    owners: dict[str, list[str]] = {}
    api = ApiClient()
    try:
        for block in BLOCKS:
            attempt_id = finished.nodes[block]["attempt_id"]
            [application] = await _applications(
                CustomObjectsApi(api), settings.spark_namespace, attempt_id
            )
            found = await attempt_secret(attempt_id)
            assert found is not None, block
            owners[application["metadata"]["uid"]] = [
                owner.uid for owner in found.metadata.owner_references
            ]
    finally:
        await api.close()

    assert all(uids == [uid] for uid, uids in owners.items())
    assert await secret("e3-s3") is None


async def test_redelivered_submit_execution_leaves_single_spark_application(
    finished: FinishedRun,
) -> None:
    attempt_id = finished.nodes["read"]["attempt_id"]
    settings = ExecutionSettings()  # pyright: ignore[reportCallIssue]
    await kube_config.load_kube_config()
    api = ApiClient()
    try:
        objects = CustomObjectsApi(api)
        [application] = await _applications(objects, settings.spark_namespace, attempt_id)
        command = parse_command(application["metadata"]["annotations"][COMMAND_ANNOTATION].encode())
        controller = ExecutionController(
            KubernetesSparkApplications(objects, CoreV1Api(api), settings.spark_namespace),
            _Unused(),
            _Unused(),
            attempt_credentials(settings),
            _Unused(),
            settings.workload(),
        )

        await controller.handle(command)

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
