"""E2E на kind: повторный POST и рестарт каждого компонента посреди PipelineRun.

Посреди `Read → Filter → Aggregate → Publish Dataset` тест повторяет запрос
запуска или перезапускает компонент стенда, пока Spark считает `read`. Запуск
всё равно доходит до SUCCEEDED с одной DatasetVersion и без лишних попыток.
Без `E3_E2E_API_URL` (его задаёт `scripts/e2e.sh`) тесты пропускаются.
"""

import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest

from e3.runs.attempts import MAX_ATTEMPTS
from tests.e2e.identity import project_api
from tests.e2e.scenario import (
    AGGREGATES,
    BLOCK_LASTING,
    BLOCKS,
    FINISHED,
    NODE_CHAIN,
    PUBLISH_CHAIN,
    RUN_CHAIN,
    Nodes,
    Started,
    Trace,
    follow,
    follows,
    published_version,
    rows,
    start,
)
from tests.e2e.stand import pipeline_runs, restart_controller, restart_service

API_URL = os.environ.get("E3_E2E_API_URL")

pytestmark = pytest.mark.skipif(API_URL is None, reason="E2E запускает scripts/e2e.sh")

READY_TIMEOUT_SECONDS = 120
POLL_SECONDS = 1
# Дольше цикла Run Orchestrator и переподключения к Kafka (KAFKA_RETRY_SECONDS).
SETTLE_SECONDS = 15

# Ретрай: узел, ждавший исхода попытки, снова PENDING; опрос может застать уже QUEUED.
AWAITING_OUTCOME = frozenset({"QUEUED", "RUNNING"})
RETRY_TO = frozenset({"PENDING", "QUEUED"})

Disruption = Callable[[Started], Awaitable[None]]


def _restart(service: str) -> Disruption:
    async def disrupt(started: Started) -> None:
        await restart_service(service)
        await _ready(started)

    return disrupt


async def _restart_controller(started: Started) -> None:
    await restart_controller()


async def _repeat_post(started: Started) -> None:
    again = await started.api.start_run(started.body, key=started.key)
    assert again.status_code == 202, again.text
    assert again.json()["pipeline_run_id"] == started.run_id


async def _ready(started: Started) -> None:
    """Ждёт, пока API снова отвечает запуском: после рестарта api или postgres."""
    async with asyncio.timeout(READY_TIMEOUT_SECONDS):
        while True:
            if await _answers(started):
                return
            await asyncio.sleep(POLL_SECONDS)


async def _answers(started: Started) -> bool:
    try:
        response = await started.api.client.get(f"/api/v1/pipeline-runs/{started.run_id}")
    except httpx.TransportError:
        return False
    return response.status_code == 200


def _spark_runs_read_or_finished(run: dict[str, Any], nodes: Nodes) -> bool:
    return run["state"] in FINISHED or nodes["read"]["state"] == "RUNNING"


def _follows_with_retries(observed: list[str], chain: list[str]) -> bool:
    """Каждая попытка узла идёт по `chain`.

    Ретрай возвращает узел из QUEUED или RUNNING в PENDING: с него идёт новая
    попытка. Любой другой шаг назад — нарушение цепочки.
    """
    tries: list[list[str]] = [[]]
    for state in observed:
        if tries[-1] and _retried(tries[-1][-1], state):
            tries.append([])
        tries[-1].append(state)
    return all(follows(states, chain) for states in tries)


def _retried(previous: str, current: str) -> bool:
    return previous in AWAITING_OUTCOME and current in RETRY_TO and previous != current


@pytest.mark.parametrize(
    "disrupt",
    [
        pytest.param(_repeat_post, id="repeated-post"),
        pytest.param(_restart("api"), id="api"),
        pytest.param(_restart_controller, id="execution-controller"),
        pytest.param(_restart("kafka"), id="kafka"),
        pytest.param(_restart("postgres"), id="postgres"),
        pytest.param(_restart("minio"), id="minio"),
    ],
)
async def test_run_survives_disruption_with_single_dataset_version(disrupt: Disruption) -> None:
    assert API_URL is not None
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        api = await project_api(client)
        started = await start(api)
        trace = Trace()
        await follow(api, started.run_id, trace, _spark_runs_read_or_finished)
        assert trace.run[-1] == "RUNNING", trace

        await disrupt(started)

        await follow(api, started.run_id, trace)
        # Опоздавшая повторная доставка после рестарта дала бы дубль уже после SUCCEEDED.
        await asyncio.sleep(SETTLE_SECONDS)
        run, nodes = await api.run(started.run_id), await api.run_nodes(started.run_id)
        version = await published_version(api, run)
        runs = await pipeline_runs(api.dev_project_id)
    assert run["state"] == "SUCCEEDED", nodes
    assert version["version_id"] == nodes["publish"]["published_version_id"]
    assert rows(run["outputs"]["result"]["dataset_id"], str(version["version_id"])) == AGGREGATES
    assert runs == 1
    assert all(len(nodes[block]["attempts"]) <= MAX_ATTEMPTS for block in BLOCKS), nodes
    assert follows(trace.run, RUN_CHAIN), trace
    for block in BLOCKS:
        assert _follows_with_retries(trace.nodes[block], NODE_CHAIN), trace
        assert set(trace.nodes[block]) >= BLOCK_LASTING, trace
    assert follows(trace.nodes["publish"], PUBLISH_CHAIN), trace
