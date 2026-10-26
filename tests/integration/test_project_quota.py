"""Project Quota: узел без свободного места ждёт в PENDING без попытки (ADR-0005).

Квота стенда — две попытки размера по умолчанию. Шаги Run Orchestrator тест
вызывает сам, как в `test_run_limit`.
"""

import asyncio
import logging
from uuid import uuid4

import pytest

from e3.api.app import create_app
from e3.settings import Settings
from tests.integration.conftest import (
    Api,
    Plant,
    client_for,
    drain,
    new_read_then_left_and_right_pipeline,
    own_project,
    run_request,
    stop_proven,
)

pytestmark = pytest.mark.integration

# Попытка по умолчанию — driver и executor по ядру и 512 MiB.
TWO_ATTEMPTS = {"project_quota_cpu_millicores": 4000, "project_quota_memory_mib": 2048}


@pytest.fixture
def settings(base_settings: Settings) -> Settings:
    return own_project(base_settings).model_copy(update=TWO_ATTEMPTS)


def numbers(attempts: list[dict[str, object]]) -> list[object]:
    return [attempt["number"] for attempt in attempts]


async def test_third_ready_node_waits_until_attempt_outcome_frees_room(plant: Plant) -> None:
    run_ids = [await plant.start() for _ in range(3)]
    await drain(plant.orchestrator.dispatch_pending)
    waiting = [await plant.state(run_id, "read") for run_id in run_ids]

    await plant.execute(run_ids[0], "read")

    assert waiting == ["QUEUED", "QUEUED", "PENDING"]
    assert numbers(await plant.attempts(run_ids[2])) == [1]


async def test_parallel_orchestrators_never_exceed_quota(plant: Plant) -> None:
    run_ids = [await plant.start() for _ in range(4)]
    orchestrators = [create_app(plant.settings).state.runs for _ in range(4)]

    await asyncio.gather(*(o.dispatch_pending() for o in orchestrators))
    await asyncio.gather(*(drain(o.dispatch_pending) for o in orchestrators))

    states = [await plant.state(run_id, "read") for run_id in run_ids]
    assert sorted(states) == ["PENDING", "PENDING", "QUEUED", "QUEUED"]


async def test_newer_run_outcome_gives_room_to_older_run_node(plant: Plant) -> None:
    older = await plant.start(new_read_then_left_and_right_pipeline)
    newer, newest = await plant.start(), await plant.start()
    await drain(plant.orchestrator.dispatch_pending)
    await plant.execute(older, "read")

    await plant.execute(newer, "read")

    assert await plant.state(older, "right") == "QUEUED"
    assert await plant.attempts(newest) == []


async def test_failed_attempt_waiting_for_room_keeps_its_retry_number(plant: Plant) -> None:
    older = await plant.start(new_read_then_left_and_right_pipeline)
    newer = await plant.start()
    await drain(plant.orchestrator.dispatch_pending)
    await plant.execute(older, "read")

    await plant.fake.fail(await plant.submitted(newer), "executor lost")
    await plant.consume_until(newer, "read", "PENDING")
    waited = numbers(await plant.attempts(newer))
    await plant.execute(older, "left")

    assert waited == [1]
    assert numbers(await plant.attempts(newer)) == [1, 2]


async def test_cancelled_attempt_holds_room_until_proven_stopped(plant: Plant) -> None:
    run_ids = [await plant.start() for _ in range(3)]
    command = await plant.submitted(run_ids[0])

    await plant.api.cancel_run(run_ids[0])
    await drain(plant.orchestrator.dispatch_pending)
    held = await plant.attempts(run_ids[2])
    await stop_proven(plant, run_ids[0], command)

    assert held == []
    assert numbers(await plant.attempts(run_ids[2])) == [1]


async def test_full_quota_does_not_delay_other_project(plant: Plant) -> None:
    for _ in range(3):
        await plant.start()
    await drain(plant.orchestrator.dispatch_pending)
    other = own_project(plant.settings)
    async with client_for(other) as client:
        other_api = Api(client, other.dev_project_id)
        response = await other_api.start_run(await run_request(other_api), key=str(uuid4()))
        other_run = response.json()["pipeline_run_id"]

        await drain(plant.orchestrator.dispatch_pending)

        assert (await other_api.run_nodes(other_run))["read"]["state"] == "QUEUED"


async def test_node_without_room_is_logged_with_usage_and_ceilings(
    plant: Plant, caplog: pytest.LogCaptureFixture
) -> None:
    for _ in range(3):
        await plant.start()

    with caplog.at_level(logging.INFO, logger="e3.runs.orchestrator"):
        await drain(plant.orchestrator.dispatch_pending)

    [message] = [r.getMessage() for r in caplog.records if "project_quota_exceeded" in r.message]
    assert str(plant.settings.dev_project_id) in message
    assert "CPU 4000m из 4000m" in message
    assert "RAM 2048Mi из 2048Mi" in message
