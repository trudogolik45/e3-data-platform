"""E2E на kind: Attempt Fence, ретрай пропавшей попытки (ADR-0003) и отмена запуска.

Запускает `scripts/e2e.sh` с коротким `E3_RECONCILE_AFTER_SECONDS`, чтобы
reconciliation успела за время теста. Без `E3_E2E_API_URL` тесты пропускаются.
"""

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from kubernetes_asyncio import config as kube_config
from kubernetes_asyncio.client import ApiClient, CoreV1Api, CustomObjectsApi

from e3.execution.controller import ExecutionController
from e3.execution.kubernetes import KubernetesSparkApplications
from e3.execution.settings import ExecutionSettings
from e3.execution.workload import (
    COMMAND_ANNOTATION,
    GROUP,
    PLURAL,
    VERSION,
    attempt_fence,
    is_cancelled,
    is_fence,
    workload_name,
)
from e3.shared.contracts import ExecutionContext, ExecutionResult
from e3.shared.messages import (
    ExecutionEvent,
    Submission,
    SubmitExecution,
    parse_command,
    reconcile,
)
from tests.e2e.identity import project_api
from tests.integration.conftest import Api
from tests.integration.graphs import read_graph

API_URL = os.environ.get("E3_E2E_API_URL")

pytestmark = pytest.mark.skipif(API_URL is None, reason="E2E запускает scripts/e2e.sh")

CSV = b"city,qty\nmsk,5\nspb,7\n"
TIMEOUT_SECONDS = 600
POLL_SECONDS = 1
# Сколько ждать, что оператор не поднимет pod'ы огороженной попытки.
SUSPEND_WATCH_SECONDS = 20
IMAGE = "localhost:5001/e3-read@sha256:" + "0" * 64


class _Unused:
    async def send(self, event: ExecutionEvent) -> None:
        raise AssertionError("SubmitExecution не шлёт событий")

    async def fetch(self, context: ExecutionContext) -> ExecutionResult | None:
        raise AssertionError("SubmitExecution не читает результат")


@pytest.fixture
async def cluster() -> AsyncIterator[tuple[KubernetesSparkApplications, ExecutionController]]:
    settings = ExecutionSettings()
    await kube_config.load_kube_config()
    api = ApiClient()
    try:
        applications = KubernetesSparkApplications(
            CustomObjectsApi(api), CoreV1Api(api), settings.spark_namespace
        )
        controller = ExecutionController(applications, _Unused(), _Unused(), settings.workload())
        yield applications, controller
    finally:
        await api.close()


async def eventually[T](
    probe: Callable[[], Awaitable[T]], done: Callable[[T], bool], seconds: float = TIMEOUT_SECONDS
) -> T:
    async with asyncio.timeout(seconds):
        while True:
            value = await probe()
            if done(value):
                return value
            await asyncio.sleep(POLL_SECONDS)


def submission() -> SubmitExecution:
    node_run_id, attempt_id = uuid4(), uuid4()
    context = ExecutionContext(
        node_run_id=node_run_id,
        attempt_id=attempt_id,
        inputs={},
        parameters={},
        output_prefix=f"s3://e3-lake/staging/executions/{attempt_id}",
    )
    return SubmitExecution(
        node_run_id=node_run_id,
        attempt_id=attempt_id,
        trace_id="e2e-suspend",
        payload=Submission(executable_ref=IMAGE, context=context),
    )


async def test_suspended_attempt_fence_starts_no_pods(
    cluster: tuple[KubernetesSparkApplications, ExecutionController],
) -> None:
    applications, _ = cluster
    command = submission()
    settings = ExecutionSettings()

    assert await applications.create(attempt_fence(reconcile(command, 1), settings.workload()))
    await asyncio.sleep(SUSPEND_WATCH_SECONDS)

    fence = await applications.get(workload_name(command.attempt_id))
    assert fence is not None
    assert fence.get("status", {}).get("applicationState", {}).get("state") == "SUSPENDED"
    assert await applications.pods(command.attempt_id) == []


async def test_vanished_attempt_is_fenced_then_retried_to_success(
    cluster: tuple[KubernetesSparkApplications, ExecutionController],
) -> None:
    applications, controller = cluster
    assert API_URL is not None
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        api = await project_api(client)
        run_id = await _start(api)
        first = await eventually(lambda: _read(api, run_id), lambda n: n["state"] == "RUNNING")
        first_id = UUID(first["attempt_id"])

        await _delete(workload_name(first_id))
        node = await eventually(lambda: _read(api, run_id), lambda n: len(n["attempts"]) == 2)

        fence = await applications.get(workload_name(first_id))
        assert fence is not None and is_fence(fence)
        lost = node["attempts"][0]
        assert (lost["state"], lost["reason"]) == ("LOST", "LOST")
        assert _created(fence) <= datetime.fromisoformat(lost["finished_at"])
        assert await applications.pods(first_id) == []

        await controller.handle(_original(fence))
        await asyncio.sleep(SUSPEND_WATCH_SECONDS)
        assert await applications.pods(first_id) == []

        run = await eventually(
            lambda: api.run(run_id), lambda r: r["state"] in {"SUCCEEDED", "FAILED"}
        )
        node = await _read(api, run_id)
    assert run["state"] == "SUCCEEDED", node
    assert [(a["number"], a["state"]) for a in node["attempts"]] == [(1, "LOST"), (2, "SUCCEEDED")]


async def test_cancel_mid_spark_replaces_attempt_with_fence_and_ends_cancelled(
    cluster: tuple[KubernetesSparkApplications, ExecutionController],
) -> None:
    applications, _ = cluster
    assert API_URL is not None
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        api = await project_api(client)
        run_id = await _start(api)
        node = await eventually(lambda: _read(api, run_id), lambda n: n["state"] == "RUNNING")
        attempt_id = UUID(node["attempt_id"])

        cancelled = await api.cancel_run(run_id)
        assert (cancelled.status_code, cancelled.json()["state"]) == (202, "CANCELLING")
        run = await eventually(
            lambda: api.run(run_id), lambda r: r["state"] not in {"RUNNING", "CANCELLING"}
        )
        node = await _read(api, run_id)
    fence = await applications.get(workload_name(attempt_id))
    assert run["state"] == "CANCELLED", node
    assert fence is not None and is_fence(fence) and is_cancelled(fence)
    assert await applications.pods(attempt_id) == []
    assert [a["state"] for a in node["attempts"]] == ["CANCELLED"]


async def _start(api: Api) -> str:
    pipeline_id = await api.new_pipeline(f"fencing-on-spark-{uuid4()}")
    assert (await api.save_draft(pipeline_id, read_graph("read"))).status_code == 200
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


async def _read(api: Api, run_id: str) -> dict[str, Any]:
    return (await api.run_nodes(run_id))["read"]


async def _delete(name: str) -> None:
    """Удаляет SparkApplication мимо Controller, как `kubectl delete`."""
    settings = ExecutionSettings()
    api = ApiClient()
    try:
        await CustomObjectsApi(api).delete_namespaced_custom_object(
            GROUP, VERSION, settings.spark_namespace, PLURAL, name
        )
    finally:
        await api.close()


def _created(application: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(application["metadata"]["creationTimestamp"])


def _original(fence: dict[str, Any]) -> SubmitExecution:
    """Исходная SubmitExecution попытки: опоздавшая доставка из outbox."""
    command = parse_command(fence["metadata"]["annotations"][COMMAND_ANNOTATION].encode())
    assert isinstance(command, SubmitExecution)
    return command
