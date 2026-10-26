"""Лимит активных PipelineRun на Project: остальные ждут в PENDING по FIFO.

Шаги Run Orchestrator тест вызывает сам, как в `test_run_lifecycle`.
"""

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from e3.api.app import create_app
from e3.runs.orchestrator import ACTIVE_RUNS_LIMIT as LIMIT
from e3.runs.orchestrator import DISPATCH_BATCH
from tests.integration.conftest import (
    Api,
    Plant,
    client_for,
    drain,
    own_project,
    run_request,
    stop_proven,
)

pytestmark = pytest.mark.integration


async def start_runs(plant: Plant, count: int) -> list[str]:
    """`count` запусков одного пайплайна в dev-Project, по порядку создания."""
    request = await run_request(plant.api)
    return [await start(plant.api, request) for _ in range(count)]


async def start(api: Api, request: dict[str, Any]) -> str:
    response = await api.start_run(request, key=str(uuid4()))
    assert response.status_code == 202, response.text
    return response.json()["pipeline_run_id"]


async def states(plant: Plant, run_ids: list[str]) -> list[str]:
    return [(await plant.api.run(run_id))["state"] for run_id in run_ids]


async def test_sixth_run_waits_in_pending_without_started_nodes(plant: Plant) -> None:
    run_ids = await start_runs(plant, LIMIT + 1)

    await drain(plant.orchestrator.dispatch_pending)

    assert await states(plant, run_ids) == ["RUNNING"] * LIMIT + ["PENDING"]
    assert await plant.state(run_ids[-1], "read") == "PENDING"


async def test_finished_run_frees_slot_for_earliest_waiting_run(plant: Plant) -> None:
    run_ids = await start_runs(plant, LIMIT + 2)
    await drain(plant.orchestrator.dispatch_pending)

    await plant.execute(run_ids[0], "read")
    await plant.orchestrator.publish_pending()
    await drain(plant.orchestrator.dispatch_pending)

    assert await states(plant, run_ids) == ["SUCCEEDED"] + ["RUNNING"] * LIMIT + ["PENDING"]


async def test_cancelling_run_keeps_its_slot_until_cancelled(plant: Plant) -> None:
    run_ids = await start_runs(plant, LIMIT + 1)
    command = await plant.submitted(run_ids[0])

    await plant.api.cancel_run(run_ids[0])
    await drain(plant.orchestrator.dispatch_pending)
    held = await states(plant, run_ids)
    await stop_proven(plant, run_ids[0], command)
    await drain(plant.orchestrator.dispatch_pending)

    assert held == ["CANCELLING"] + ["RUNNING"] * (LIMIT - 1) + ["PENDING"]
    assert await states(plant, run_ids) == ["CANCELLED"] + ["RUNNING"] * LIMIT


async def test_cancelled_waiting_run_leaves_queue_without_taking_slot(plant: Plant) -> None:
    run_ids = await start_runs(plant, LIMIT + 2)
    await drain(plant.orchestrator.dispatch_pending)

    cancelled = await plant.api.cancel_run(run_ids[LIMIT])
    await plant.execute(run_ids[0], "read")
    await plant.orchestrator.publish_pending()
    await drain(plant.orchestrator.dispatch_pending)

    assert cancelled.json()["state"] == "CANCELLED"
    assert await plant.state(run_ids[LIMIT], "read") == "SKIPPED"
    assert await states(plant, run_ids) == (
        ["SUCCEEDED"] + ["RUNNING"] * (LIMIT - 1) + ["CANCELLED", "RUNNING"]
    )


async def test_full_project_queue_does_not_block_other_project(plant: Plant) -> None:
    await start_runs(plant, LIMIT + DISPATCH_BATCH + 1)
    await drain(plant.orchestrator.dispatch_pending)
    other = own_project(plant.settings)
    async with client_for(other) as client:
        other_api = Api(client, other.dev_project_id)
        other_run = await start(other_api, await run_request(other_api))

        await drain(plant.orchestrator.dispatch_pending)

    assert await states(plant, [other_run]) == ["RUNNING"]


async def test_parallel_orchestrators_never_exceed_limit(plant: Plant) -> None:
    run_ids = await start_runs(plant, 2 * LIMIT)
    orchestrators = [create_app(plant.settings).state.runs for _ in range(4)]

    await asyncio.gather(*(o.dispatch_pending() for o in orchestrators))
    await asyncio.gather(*(drain(o.dispatch_pending) for o in orchestrators))

    assert await states(plant, run_ids) == ["RUNNING"] * LIMIT + ["PENDING"] * LIMIT
