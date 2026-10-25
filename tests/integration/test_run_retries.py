"""Ретраи NodeRun, одна активная попытка и reconciliation (ADR-0003).

Сценарий — `Read → Publish Dataset`. Шаги Run Orchestrator и Execution Plane
тест вызывает сам: так видно, кто что решил и в каком порядке.
"""

from collections.abc import Callable
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from e3.execution.workload import Pod
from e3.shared.contracts import ExecutionResult
from e3.shared.messages import (
    ExecutionEvent,
    ExecutionSucceeded,
    FailureReason,
    ReconcileExecution,
    SubmitExecution,
    failed,
    lost,
    reconcile,
    succeeded,
)
from tests.integration.conftest import Plant, drain

pytestmark = pytest.mark.integration

MAX_ATTEMPTS = 4


def summary(attempts: list[dict[str, object]]) -> list[tuple[object, ...]]:
    return [(a["number"], a["state"], a["reason"]) for a in attempts]


async def test_node_fails_after_four_spark_failures(plant: Plant) -> None:
    run_id = await plant.start()

    for number in range(1, MAX_ATTEMPTS + 1):
        await plant.fake.fail(await plant.submitted(run_id), f"executor lost #{number}")
        await plant.consume_attempts(run_id, min(number + 1, MAX_ATTEMPTS))
    await plant.consume_until(run_id, "read", "FAILED")

    attempts = await plant.attempts(run_id)
    assert summary(attempts) == [(n, "FAILED", "SPARK_FAILED") for n in range(1, 5)]
    assert len({a["attempt_id"] for a in attempts}) == MAX_ATTEMPTS
    assert (await plant.node(run_id, "read"))["error"] == "executor lost #4"
    assert (await plant.api.run(run_id))["state"] == "FAILED"
    assert await plant.state(run_id, "publish") == "SKIPPED"


Outcome = Callable[[SubmitExecution], ExecutionEvent]


def spark_failed(command: SubmitExecution) -> ExecutionEvent:
    return failed(command, "executor lost", "SPARK_FAILED")


def submission_failed(command: SubmitExecution) -> ExecutionEvent:
    return failed(command, "no such image", "SUBMISSION_FAILED")


@pytest.mark.parametrize(
    ("outcome", "ended"),
    [
        (spark_failed, ("FAILED", "SPARK_FAILED")),
        (submission_failed, ("FAILED", "SUBMISSION_FAILED")),
        (lost, ("LOST", "LOST")),
    ],
)
async def test_retryable_failure_is_followed_by_attempt_that_succeeds(
    plant: Plant, outcome: Outcome, ended: tuple[str, str]
) -> None:
    run_id = await plant.start()
    first = await plant.submitted(run_id)
    await plant.fake.send(outcome(first))
    await plant.consume_attempts(run_id, 2)

    second = await plant.execute(run_id, "read")
    await plant.finish()

    attempts = await plant.attempts(run_id)
    assert second.attempt_id != first.attempt_id
    assert summary(attempts) == [(1, *ended), (2, "SUCCEEDED", None)]
    assert (await plant.node(run_id, "read"))["error"] is None
    assert [a["attempt_id"] for a in attempts] == [str(first.attempt_id), str(second.attempt_id)]
    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"


@pytest.mark.parametrize("reason", ["NO_RESULT", "UNKNOWN"])
async def test_non_retryable_failure_fails_node_after_one_attempt(
    plant: Plant, reason: FailureReason
) -> None:
    run_id = await plant.start()

    await plant.fake.fail(await plant.submitted(run_id), "нет результата", reason)
    await plant.consume_until(run_id, "read", "FAILED")

    assert summary(await plant.attempts(run_id)) == [(1, "FAILED", reason)]
    assert (await plant.api.run(run_id))["state"] == "FAILED"


async def test_rejected_manifest_fails_attempt_without_retry(plant: Plant) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    result = plant.fake.execute(command).model_copy(update={"outputs": {}})
    await plant.fake.send(succeeded(command, result))
    await plant.consume_until(run_id, "read", "FINALIZING")

    await plant.finish()

    assert summary(await plant.attempts(run_id)) == [(1, "FAILED", "OUTPUT_REJECTED")]
    assert await plant.state(run_id, "read") == "FAILED"
    assert (await plant.api.run(run_id))["state"] == "FAILED"


async def test_late_success_of_previous_attempt_is_ignored(plant: Plant) -> None:
    run_id = await plant.start()
    first = await plant.submitted(run_id)
    await plant.fake.fail(first, "executor lost")
    await plant.consume_attempts(run_id, 2)
    second = await plant.submitted(run_id)

    await plant.fake.succeed(first)
    await plant.events.poll(timeout_ms=1000)
    await plant.finish()

    assert await plant.state(run_id, "read") == "QUEUED"
    assert (await plant.node(run_id, "read"))["outputs"] == []
    await plant.fake.succeed(second)
    await plant.consume_until(run_id, "read", "FINALIZING")
    await plant.finish()
    [output] = (await plant.node(run_id, "read"))["outputs"]
    assert f"executions/{second.attempt_id}/" in output["manifest_uri"]


async def test_lost_after_success_does_not_change_attempt_outcome(plant: Plant) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    await plant.fake.succeed(command)
    await plant.consume_until(run_id, "read", "FINALIZING")

    await plant.fake.send(lost(command))
    await plant.events.poll(timeout_ms=1000)
    await plant.finish()

    assert summary(await plant.attempts(run_id)) == [(1, "SUCCEEDED", None)]
    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"


async def test_second_failure_of_same_attempt_keeps_first_reason(plant: Plant) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    await plant.fake.send(failed(command, "нет результата", "NO_RESULT"))
    await plant.consume_until(run_id, "read", "FAILED")

    await plant.fake.send(lost(command))
    await plant.events.poll(timeout_ms=1000)

    assert summary(await plant.attempts(run_id)) == [(1, "FAILED", "NO_RESULT")]


async def test_repeated_success_during_finalizing_keeps_first_result(plant: Plant) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    first = await plant.fake.succeed(command)
    await plant.consume_until(run_id, "read", "FINALIZING")

    other = first.payload.model_copy(update={"outputs": {"elsewhere": "s3://e3-lake/x"}})
    await plant.fake.send(bypassing_inbox(first, other))
    await plant.events.poll(timeout_ms=1000)
    await plant.finish()

    [output] = (await plant.node(run_id, "read"))["outputs"]
    assert output["manifest_uri"] == first.payload.outputs["data"]
    assert summary(await plant.attempts(run_id)) == [(1, "SUCCEEDED", None)]


def bypassing_inbox(event: ExecutionSucceeded, payload: ExecutionResult) -> ExecutionSucceeded:
    """То же событие с другим `payload` и новым `event_id`: inbox его не узнает."""
    return event.model_copy(update={"event_id": uuid4(), "payload": payload})


async def age(plant: Plant, attempt_id: UUID, by: timedelta) -> None:
    """Сдвигает часы попытки назад, будто с её последней новости прошло `by`."""
    engine = create_async_engine(plant.settings.database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE runs.execution_attempts"
                    " SET created_at = created_at - CAST(:by AS interval),"
                    " started_at = started_at - CAST(:by AS interval),"
                    " reconcile_requested_at = reconcile_requested_at - CAST(:by AS interval)"
                    " WHERE attempt_id = :attempt_id"
                ),
                {"by": by, "attempt_id": attempt_id},
            )
    finally:
        await engine.dispose()


def past_timeout(plant: Plant) -> timedelta:
    """Чуть больше таймаута reconciliation стенда."""
    return timedelta(seconds=plant.settings.reconcile_after_seconds + 1)


async def reconciles(plant: Plant, attempt_id: UUID) -> list[ReconcileExecution]:
    await drain(plant.relay.publish_pending)
    return [
        c
        for c in await plant.fake.quiet_commands()
        if isinstance(c, ReconcileExecution) and c.attempt_id == attempt_id
    ]


async def test_quiet_live_attempt_is_reconciled_not_lost(plant: Plant) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    assert await reconciles(plant, command.attempt_id) == []

    await age(plant, command.attempt_id, past_timeout(plant))
    await plant.orchestrator.reconcile_pending()
    await age(plant, command.attempt_id, past_timeout(plant))
    await plant.orchestrator.reconcile_pending()

    [check] = await reconciles(plant, command.attempt_id)
    assert (check.node_run_id, check.payload) == (command.node_run_id, command.payload)
    assert summary(await plant.attempts(run_id)) == [(1, "SUBMITTED", None)]
    await age(plant, command.attempt_id, past_timeout(plant))
    await plant.orchestrator.reconcile_pending()
    [next_check] = await reconciles(plant, command.attempt_id)
    assert next_check.event_id != check.event_id


async def test_unprocessed_check_is_replaced_and_vanished_attempt_is_retried(
    plant: Plant,
) -> None:
    run_id = await plant.start()
    first = await plant.submitted(run_id)
    controller = plant.fake.controller()
    await controller.handle(first)
    plant.fake.cluster.delete(f"e3-{first.attempt_id}")
    await age(plant, first.attempt_id, past_timeout(plant))
    await plant.orchestrator.reconcile_pending()
    [unprocessed] = await reconciles(plant, first.attempt_id)

    await age(plant, first.attempt_id, past_timeout(plant))
    await plant.orchestrator.reconcile_pending()
    [check] = await reconciles(plant, first.attempt_id)
    restarted = plant.fake.controller()
    await restarted.handle(check)
    await plant.consume_attempts(run_id, 2)

    await restarted.handle(first)
    await restarted.report()
    second = await plant.submitted(run_id)
    await restarted.handle(second)
    plant.fake.complete_spark(second)
    await restarted.report()
    await plant.consume_until(run_id, "read", "FINALIZING")
    await plant.finish()

    assert check.event_id != unprocessed.event_id
    assert plant.fake.cluster.items[f"e3-{first.attempt_id}"]["spec"]["suspend"] is True
    assert summary(await plant.attempts(run_id)) == [(1, "LOST", "LOST"), (2, "SUCCEEDED", None)]
    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"


async def test_restarts_mid_run_give_one_output_and_no_extra_attempts(plant: Plant) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    await plant.fake.controller().handle(command)
    plant.fake.run_spark(command)
    await plant.fake.controller().report()
    await plant.consume_until(run_id, "read", "RUNNING")

    plant.restart()
    restarted = plant.fake.controller()
    await restarted.handle(command)
    await drain(plant.orchestrator.dispatch_pending)
    plant.fake.complete_spark(command)
    await restarted.report()
    await plant.fake.controller().handle(reconcile(command, 1))
    await plant.consume_until(run_id, "read", "FINALIZING")
    await plant.finish()

    await drain(plant.relay.publish_pending)
    assert await plant.fake.quiet_commands() == []
    assert list(plant.fake.cluster.items) == [f"e3-{command.attempt_id}"]
    assert len((await plant.node(run_id, "read"))["outputs"]) == 1
    assert summary(await plant.attempts(run_id)) == [(1, "SUCCEEDED", None)]
    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"


async def test_next_attempt_waits_until_pods_of_failed_one_are_gone(plant: Plant) -> None:
    run_id = await plant.start()
    first = await plant.submitted(run_id)
    controller = plant.fake.controller()
    await controller.handle(first)
    plant.fake.cluster.set_state(f"e3-{first.attempt_id}", "FAILED", error="executor lost")
    plant.fake.cluster.set_pods(first.attempt_id, Pod(phase="Running", terminating=True))

    await controller.report()
    await plant.events.poll(timeout_ms=1000)
    await drain(plant.orchestrator.dispatch_pending)
    await drain(plant.relay.publish_pending)

    assert await plant.fake.quiet_commands() == []
    assert summary(await plant.attempts(run_id)) == [(1, "SUBMITTED", None)]
    plant.fake.cluster.set_pods(first.attempt_id, Pod(phase="Failed"))
    await controller.report()
    await plant.consume_attempts(run_id, 2)
    assert (await plant.submitted(run_id)).attempt_id != first.attempt_id
