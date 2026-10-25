from uuid import uuid4

import pytest

from e3.execution.controller import ExecutionController
from e3.execution.settings import ExecutionSettings
from e3.execution.workload import WorkloadConfig
from e3.shared.contracts import ExecutionContext, ExecutionResult
from e3.shared.messages import (
    ExecutionEvent,
    ExecutionFailed,
    ExecutionSucceeded,
    Submission,
    SubmitExecution,
)
from tests.fakes.spark_applications import InMemorySparkApplications

IMAGE = "localhost:5001/e3-read@sha256:" + "a" * 64
CONFIG = WorkloadConfig(
    namespace="default",
    service_account="spark-operator-spark",
    spark_version="3.5.9",
    s3_endpoint_url="http://minio:9000",
    s3_secret="e3-s3",
)


class RecordedEvents:
    def __init__(self) -> None:
        self.sent: list[ExecutionEvent] = []

    async def send(self, event: ExecutionEvent) -> None:
        self.sent.append(event)


class StoredResults:
    def __init__(self) -> None:
        self.results: dict[str, ExecutionResult] = {}

    async def fetch(self, context: ExecutionContext) -> ExecutionResult | None:
        return self.results.get(context.output_prefix)


def command() -> SubmitExecution:
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
        trace_id="run-1",
        payload=Submission(executable_ref=IMAGE, context=context),
    )


def controller() -> tuple[
    ExecutionController, InMemorySparkApplications, RecordedEvents, StoredResults
]:
    applications, events, results = InMemorySparkApplications(), RecordedEvents(), StoredResults()
    return ExecutionController(applications, events, results, CONFIG), applications, events, results


async def test_redelivered_submit_execution_creates_single_spark_application() -> None:
    execution, applications, _, _ = controller()
    submit = command()

    await execution.submit(submit)
    await execution.submit(submit)

    assert list(applications.items) == [f"e3-{submit.attempt_id}"]


async def test_spark_application_runs_block_image_once_with_execution_context() -> None:
    execution, applications, _, _ = controller()
    submit = command()

    await execution.submit(submit)

    [spec] = [app["spec"] for app in applications.items.values()]
    assert spec["image"] == IMAGE
    assert spec["restartPolicy"] == {"type": "Never"}
    assert ExecutionContext.model_validate_json(spec["arguments"][0]) == submit.payload.context


async def test_running_application_is_reported_started_once() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(f"e3-{submit.attempt_id}", "RUNNING")

    await execution.report()
    await execution.report()

    assert [event.type for event in events.sent] == ["ExecutionStarted"]
    assert (events.sent[0].attempt_id, events.sent[0].trace_id) == (submit.attempt_id, "run-1")


async def test_completed_application_is_reported_started_then_succeeded_with_block_result() -> None:
    execution, applications, events, results = controller()
    submit = command()
    context = submit.payload.context
    result = ExecutionResult(
        node_run_id=context.node_run_id,
        attempt_id=context.attempt_id,
        outputs={"data": f"{context.output_prefix}/data/manifest.json"},
    )
    results.results[context.output_prefix] = result
    await execution.submit(submit)
    applications.set_state(f"e3-{submit.attempt_id}", "COMPLETED")

    await execution.report()
    await execution.report()

    assert [event.type for event in events.sent] == ["ExecutionStarted", "ExecutionSucceeded"]
    assert isinstance(events.sent[1], ExecutionSucceeded)
    assert events.sent[1].payload == result


async def test_failed_application_is_reported_failed_with_operator_error() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(f"e3-{submit.attempt_id}", "FAILED", error="driver exited with 1")

    await execution.report()
    await execution.report()

    assert len(events.sent) == 1
    assert isinstance(events.sent[0], ExecutionFailed)
    assert "driver exited with 1" in events.sent[0].payload.error


async def test_completed_application_without_block_result_is_reported_failed() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(f"e3-{submit.attempt_id}", "COMPLETED")

    await execution.report()

    assert [event.type for event in events.sent] == ["ExecutionStarted", "ExecutionFailed"]


async def test_application_not_yet_running_is_not_reported() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(f"e3-{submit.attempt_id}", "SUBMITTED")

    pending = await execution.report()

    assert (pending, events.sent) == (1, [])


async def test_spark_pods_request_configured_cpu_share_of_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("E3_SPARK_CORE_REQUEST", "250m")
    config = ExecutionSettings(_env_file=None).workload()  # pyright: ignore[reportCallIssue]
    applications = InMemorySparkApplications()
    execution = ExecutionController(applications, RecordedEvents(), StoredResults(), config)

    await execution.submit(command())

    [spec] = [app["spec"] for app in applications.items.values()]
    for pod in (spec["driver"], spec["executor"]):
        assert (pod["cores"], pod["coreRequest"]) == (1, "250m")


async def test_spark_pods_request_whole_core_by_default() -> None:
    execution, applications, _, _ = controller()

    await execution.submit(command())

    [spec] = [app["spec"] for app in applications.items.values()]
    for pod in (spec["driver"], spec["executor"]):
        assert pod["cores"] == 1
        assert "coreRequest" not in pod
