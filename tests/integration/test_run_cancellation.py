"""Отмена PipelineRun и Fail-fast: остановка доказывается, а не предполагается.

Шаги Run Orchestrator и Execution Plane тест вызывает сам, как в `test_run_retries`.
"""

from uuid import UUID

import pytest

from e3.execution.controller import ExecutionController
from e3.execution.workload import Pod
from e3.shared.messages import CancelExecution, ReconcileExecution, SubmitExecution
from tests.integration.conftest import (
    Plant,
    age,
    drain,
    new_read_beside_side_pipeline,
    past_timeout,
)

MAX_ATTEMPTS = 4

pytestmark = pytest.mark.integration


async def cancels(plant: Plant, attempt_id: UUID) -> list[CancelExecution]:
    await drain(plant.relay.publish_pending)
    return [
        c
        for c in await plant.fake.quiet_commands()
        if isinstance(c, CancelExecution) and c.attempt_id == attempt_id
    ]


async def submissions(plant: Plant, run_id: str, *node_ids: str) -> list[SubmitExecution]:
    """SubmitExecution узлов, запущенных одним проходом: `submitted` пропустил бы соседей."""
    await drain(plant.orchestrator.dispatch_pending)
    await drain(plant.relay.publish_pending)
    nodes = await plant.api.run_nodes(run_id)
    by_node = {
        c.node_run_id: c
        for c in await plant.fake.quiet_commands()
        if isinstance(c, SubmitExecution)
    }
    return [by_node[UUID(nodes[node_id]["node_run_id"])] for node_id in node_ids]


async def running(plant: Plant, run_id: str) -> tuple[SubmitExecution, ExecutionController]:
    """`read` работает в Spark: driver жив, `Started` принят."""
    command = await plant.submitted(run_id)
    controller = plant.fake.controller()
    await controller.handle(command)
    plant.fake.run_spark(command)
    await controller.report()
    await plant.consume_until(run_id, "read", "RUNNING")
    return command, controller


async def test_cancel_during_spark_node_ends_cancelled_only_after_pods_are_gone(
    plant: Plant,
) -> None:
    run_id = await plant.start()
    command, controller = await running(plant, run_id)

    response = await plant.api.cancel_run(run_id)

    assert response.status_code == 202, response.text
    assert response.json()["state"] == "CANCELLING"
    assert await plant.state(run_id, "read") == "CANCELLING"
    assert await plant.state(run_id, "publish") == "SKIPPED"
    [stop] = await cancels(plant, command.attempt_id)
    await controller.handle(stop)
    await plant.events.poll(timeout_ms=1000)
    assert (await plant.api.run(run_id))["state"] == "CANCELLING"

    plant.fake.cluster.set_pods(command.attempt_id)
    await controller.report()
    await plant.consume_until(run_id, "read", "CANCELLED")

    assert (await plant.api.run(run_id))["state"] == "CANCELLED"
    [attempt] = await plant.attempts(run_id)
    assert (attempt["state"], attempt["reason"]) == ("CANCELLED", None)


async def test_cancel_of_queued_node_fences_it_and_late_submit_starts_no_spark(
    plant: Plant,
) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)

    await plant.api.cancel_run(run_id)
    [stop] = await cancels(plant, command.attempt_id)
    controller = plant.fake.controller()
    await controller.handle(stop)
    await controller.handle(command)
    await controller.report()
    await plant.consume_until(run_id, "read", "CANCELLED")

    assert plant.fake.cluster.items[f"e3-{command.attempt_id}"]["spec"]["suspend"] is True
    [attempt] = await plant.attempts(run_id)
    assert (attempt["state"], attempt["started_at"]) == ("CANCELLED", None)
    assert (await plant.api.run(run_id))["state"] == "CANCELLED"


async def test_cancel_during_finalizing_accepts_output_and_skips_downstream(
    plant: Plant,
) -> None:
    run_id = await plant.start()
    await plant.fake.succeed(await plant.submitted(run_id))
    await plant.consume_until(run_id, "read", "FINALIZING")

    response = await plant.api.cancel_run(run_id)
    await plant.finish()

    assert response.json()["state"] == "CANCELLING"
    assert await plant.state(run_id, "read") == "SUCCEEDED"
    assert len((await plant.node(run_id, "read"))["outputs"]) == 1
    assert await plant.state(run_id, "publish") == "SKIPPED"
    assert (await plant.api.run(run_id))["state"] == "CANCELLED"


async def test_cancel_during_publication_publishes_version_then_ends_cancelled(
    plant: Plant,
) -> None:
    run_id = await plant.start()
    await plant.execute(run_id, "read")
    assert await plant.state(run_id, "publish") == "RUNNING"

    response = await plant.api.cancel_run(run_id)
    assert response.json()["state"] == "CANCELLING"
    await plant.finish()

    publish = await plant.node(run_id, "publish")
    run = await plant.api.run(run_id)
    target = UUID(run["outputs"]["result"]["dataset_id"])
    assert publish["state"] == "SUCCEEDED"
    assert [v["version_id"] for v in await plant.api.versions(target)] == [
        publish["published_version_id"]
    ]
    assert run["state"] == "CANCELLED"


async def test_cancel_of_pending_run_skips_every_node_and_ends_cancelled_at_once(
    plant: Plant,
) -> None:
    run_id = await plant.start()

    response = await plant.api.cancel_run(run_id)
    await drain(plant.orchestrator.dispatch_pending)

    assert (response.status_code, response.json()["state"]) == (202, "CANCELLED")
    assert {n["state"] for n in (await plant.api.run_nodes(run_id)).values()} == {"SKIPPED"}
    assert (await plant.api.run(run_id))["state"] == "CANCELLED"


async def test_repeated_cancel_is_accepted_and_changes_nothing(plant: Plant) -> None:
    run_id = await plant.start()
    command, _ = await running(plant, run_id)
    await plant.api.cancel_run(run_id)

    again = await plant.api.cancel_run(run_id)

    assert (again.status_code, again.json()["state"]) == (202, "CANCELLING")
    assert len(await cancels(plant, command.attempt_id)) == 1


async def test_cancel_of_finished_run_is_conflict(plant: Plant) -> None:
    run_id = await plant.start()
    await plant.execute(run_id, "read")
    await plant.finish()
    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"

    response = await plant.api.cancel_run(run_id)

    assert response.status_code == 409, response.text


async def test_fail_fast_cancels_running_node_and_fails_run_after_proven_stop(
    plant: Plant,
) -> None:
    run_id = await plant.start(new_read_beside_side_pipeline)
    read, side = await submissions(plant, run_id, "read", "side")
    controller = plant.fake.controller()
    await controller.handle(side)
    plant.fake.run_spark(side)
    await controller.report()
    await plant.consume_until(run_id, "side", "RUNNING")

    for number in range(1, MAX_ATTEMPTS + 1):
        await plant.fake.fail(read, f"executor lost #{number}")
        await plant.consume_attempts(run_id, min(number + 1, MAX_ATTEMPTS))
        if number < MAX_ATTEMPTS:
            read = await plant.submitted(run_id)
    await plant.consume_until(run_id, "read", "FAILED")

    run = await plant.api.run(run_id)
    assert run["state"] == "RUNNING"
    assert run["failed_node_run_id"] == (await plant.node(run_id, "read"))["node_run_id"]
    assert await plant.state(run_id, "side") == "CANCELLING"
    assert await plant.state(run_id, "publish") == "SKIPPED"
    late = await plant.api.cancel_run(run_id)
    assert (late.status_code, late.json()["state"]) == (202, "RUNNING")

    [stop] = await cancels(plant, side.attempt_id)
    await controller.handle(stop)
    plant.fake.cluster.set_pods(side.attempt_id)
    await controller.report()
    await plant.consume_until(run_id, "side", "CANCELLED")

    assert (await plant.api.run(run_id))["state"] == "FAILED"


async def test_fail_fast_keeps_already_published_version(plant: Plant) -> None:
    run_id = await plant.start(new_read_beside_side_pipeline)
    read, side = await submissions(plant, run_id, "read", "side")
    await plant.fake.succeed(read)
    await plant.consume_until(run_id, "read", "FINALIZING")
    await plant.finish()
    assert await plant.state(run_id, "publish") == "SUCCEEDED"

    for number in range(1, MAX_ATTEMPTS + 1):
        await plant.fake.fail(side, f"executor lost #{number}")
        await plant.consume_attempts(run_id, min(number + 1, MAX_ATTEMPTS), "side")
        if number < MAX_ATTEMPTS:
            side = await plant.submitted(run_id, "side")
    await plant.consume_until(run_id, "side", "FAILED")

    run = await plant.api.run(run_id)
    target = UUID(run["outputs"]["result"]["dataset_id"])
    published = (await plant.node(run_id, "publish"))["published_version_id"]
    assert run["state"] == "FAILED"
    assert [v["version_id"] for v in await plant.api.versions(target)] == [published]


async def test_node_being_cancelled_is_not_retried_after_spark_failure(plant: Plant) -> None:
    run_id = await plant.start()
    command, _ = await running(plant, run_id)
    await plant.api.cancel_run(run_id)

    await plant.fake.fail(command, "executor lost")
    await plant.consume_until(run_id, "read", "FAILED")

    await drain(plant.relay.publish_pending)
    commands = await plant.fake.quiet_commands()
    assert not [c for c in commands if isinstance(c, SubmitExecution)]
    assert [a["state"] for a in await plant.attempts(run_id)] == ["FAILED"]
    assert (await plant.api.run(run_id))["state"] == "FAILED"


async def test_stuck_pod_gets_repeated_cancel_not_reconcile_until_it_is_gone(
    plant: Plant,
) -> None:
    run_id = await plant.start()
    command, controller = await running(plant, run_id)
    await plant.api.cancel_run(run_id)
    [first] = await cancels(plant, command.attempt_id)
    await controller.handle(first)
    plant.fake.cluster.set_pods(command.attempt_id, Pod(phase="Running", terminating=True))

    await age(plant, command.attempt_id, past_timeout(plant))
    await plant.orchestrator.reconcile_pending()
    await drain(plant.relay.publish_pending)
    commands = await plant.fake.quiet_commands()

    [again] = [c for c in commands if isinstance(c, CancelExecution)]
    assert again.event_id != first.event_id
    assert not [c for c in commands if isinstance(c, ReconcileExecution)]
    assert await plant.state(run_id, "read") == "CANCELLING"
    plant.fake.cluster.set_pods(command.attempt_id)
    await controller.handle(again)
    await plant.consume_until(run_id, "read", "CANCELLED")
    assert (await plant.api.run(run_id))["state"] == "CANCELLED"


async def test_restarts_mid_cancel_end_cancelled_without_new_attempts(plant: Plant) -> None:
    run_id = await plant.start()
    command, _ = await running(plant, run_id)
    await plant.api.cancel_run(run_id)
    [stop] = await cancels(plant, command.attempt_id)

    plant.restart()
    restarted = plant.fake.controller()
    await restarted.handle(stop)
    await restarted.handle(command)
    plant.fake.cluster.set_pods(command.attempt_id)
    await plant.fake.controller().report()
    await plant.consume_until(run_id, "read", "CANCELLED")
    await drain(plant.orchestrator.dispatch_pending)

    await drain(plant.relay.publish_pending)
    assert await plant.fake.quiet_commands() == []
    assert [a["state"] for a in await plant.attempts(run_id)] == ["CANCELLED"]
    assert (await plant.api.run(run_id))["state"] == "CANCELLED"
