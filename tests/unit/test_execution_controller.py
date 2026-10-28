from typing import cast
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaProducer
from pydantic import ValidationError

from e3.execution.controller import ExecutionController
from e3.execution.main import KafkaEvents
from e3.execution.settings import ExecutionSettings
from e3.execution.workload import (
    CANCELLED_LABEL,
    COMMAND_ANNOTATION,
    FENCED_LABEL,
    GROUP,
    TRACE_LABEL,
    VERSION,
    Pod,
    PodLog,
    WorkloadConfig,
    spark_application,
)
from e3.shared.contracts import ExecutionContext, ExecutionError, ExecutionResult
from e3.shared.messages import (
    DriverLogs,
    ExecutionEvent,
    ExecutionFailed,
    ExecutionSucceeded,
    PodSize,
    Submission,
    SubmitExecution,
    cancel,
    parse_command,
    reconcile,
    started,
)
from e3.shared.storage import StorageUnavailable
from tests.fakes.attempt_credentials import ATTEMPT_KEYS, FixedCredentials, StsUnavailable
from tests.fakes.spark_applications import InMemorySparkApplications
from tests.logs import TraceLogs
from tests.unit.conftest import UNIT_S3_SECRET

IMAGE = "localhost:5001/e3-read@sha256:" + "a" * 64
CONFIG = WorkloadConfig(
    namespace="default",
    service_account="spark-operator-spark",
    spark_version="3.5.9",
    s3_endpoint_url="http://minio:9000",
)


class RecordedEvents:
    def __init__(self) -> None:
        self.sent: list[ExecutionEvent] = []

    async def send(self, event: ExecutionEvent) -> None:
        self.sent.append(event)


class StoredResults:
    def __init__(self) -> None:
        self.results: dict[str, ExecutionResult] = {}
        self.errors: dict[str, ExecutionError] = {}

    async def fetch(self, context: ExecutionContext) -> ExecutionResult | None:
        return self.results.get(context.output_prefix)

    async def fetch_error(self, context: ExecutionContext) -> ExecutionError | None:
        return self.errors.get(context.output_prefix)


class RecordedArtifacts:
    """Объекты под `artifacts/` глазами S3; `fail_once` — следующая запись упадёт."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.puts = 0
        self._failures = 0

    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        if self._failures:
            self._failures -= 1
            raise StorageUnavailable("SlowDown")
        self.puts += 1
        self.objects[key] = data

    def fail_once(self) -> None:
        self._failures += 1


def command(pod: PodSize | None = None) -> SubmitExecution:
    node_run_id, attempt_id = uuid4(), uuid4()
    context = ExecutionContext(
        node_run_id=node_run_id,
        attempt_id=attempt_id,
        inputs={},
        parameters={},
        output_prefix=f"s3://e3-lake/staging/executions/{attempt_id}",
        trace_id="run-1",
    )
    return SubmitExecution(
        node_run_id=node_run_id,
        attempt_id=attempt_id,
        trace_id="run-1",
        payload=Submission(executable_ref=IMAGE, context=context, pod=pod or PodSize()),
    )


def controller(
    credentials: FixedCredentials | None = None,
    artifacts: RecordedArtifacts | None = None,
) -> tuple[ExecutionController, InMemorySparkApplications, RecordedEvents, StoredResults]:
    applications, events, results = InMemorySparkApplications(), RecordedEvents(), StoredResults()
    execution = ExecutionController(
        applications,
        events,
        results,
        credentials or FixedCredentials(),
        artifacts or RecordedArtifacts(),
        CONFIG,
    )
    return execution, applications, events, results


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


async def test_spark_application_carries_trace_id_in_label_and_pods_env() -> None:
    execution, applications, _, _ = controller()
    submit = command()

    await execution.submit(submit)

    application = applications.items[name(submit)]
    assert application["metadata"]["labels"][TRACE_LABEL] == "run-1"
    for role in ("driver", "executor"):
        pod = application["spec"][role]
        assert pod["labels"][TRACE_LABEL] == "run-1"
        assert {"name": "E3_TRACE_ID", "value": "run-1"} in pod["env"]


async def test_command_handling_logs_carry_its_trace_id(trace_logs: TraceLogs) -> None:
    execution, _, _, _ = controller()
    submit = command()

    await execution.handle(submit)

    assert f"SubmitExecution попытки {submit.attempt_id} обработана" in trace_logs("run-1")


async def test_reported_events_are_logged_with_attempt_trace_id(trace_logs: TraceLogs) -> None:
    execution, applications, _, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", "OOM")

    await execution.report()

    assert trace_logs("run-1") == [f"ExecutionFailed попытки {submit.attempt_id} отправлено"]


async def test_attempt_keys_reach_spark_pods_through_secret_owned_by_its_application() -> None:
    credentials = FixedCredentials()
    execution, applications, _, _ = controller(credentials)
    submit = command()

    await execution.submit(submit)

    application = applications.items[name(submit)]
    [secret] = applications.secrets.values()
    assert credentials.issued == [submit.payload.context]
    assert secret["stringData"] == {
        "AWS_ACCESS_KEY_ID": "attempt-access",
        "AWS_SECRET_ACCESS_KEY": "attempt-secret",
        "AWS_SESSION_TOKEN": "attempt-token",
    }
    assert secret["metadata"]["ownerReferences"] == [
        {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "SparkApplication",
            "name": name(submit),
            "uid": application["metadata"]["uid"],
            "controller": True,
            "blockOwnerDeletion": True,
        }
    ]
    secret_ref = [{"secretRef": {"name": secret["metadata"]["name"]}}]
    assert application["spec"]["driver"]["envFrom"] == secret_ref
    assert application["spec"]["executor"]["envFrom"] == secret_ref


async def test_submit_redelivered_after_sts_failure_still_gives_attempt_its_keys() -> None:
    credentials = FixedCredentials()
    execution, applications, _, _ = controller(credentials)
    submit = command()
    credentials.fail_once()
    with pytest.raises(StsUnavailable):
        await execution.submit(submit)

    await execution.submit(submit)

    assert list(applications.items) == [name(submit)]
    assert [s["stringData"] for s in applications.secrets.values()] == [ATTEMPT_KEYS.env()]


async def test_redelivered_submit_issues_no_extra_attempt_keys() -> None:
    credentials = FixedCredentials()
    execution, _, _, _ = controller(credentials)
    submit = command()

    await execution.submit(submit)
    await execution.submit(submit)

    assert credentials.issued == [submit.payload.context]


async def test_attempt_fence_gets_no_attempt_keys() -> None:
    credentials = FixedCredentials()
    execution, applications, _, _ = controller(credentials)
    submit = command()
    await execution.handle(reconcile(submit, 1))

    await execution.handle(submit)

    assert applications.items[name(submit)]["spec"]["suspend"] is True
    assert applications.secrets == {}
    assert credentials.issued == []


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
    assert events.sent[0].payload.reason == "SPARK_FAILED"


async def test_failed_block_with_its_error_is_reported_block_failed_with_its_message() -> None:
    execution, applications, events, results = controller()
    submit = command()
    results.errors[submit.payload.context.output_prefix] = ExecutionError(
        code="CAST_INVALID_INPUT",
        message="колонка amount: значения не приводятся к long",
        column="amount",
        target_type="long",
    )
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="driver exited with 1")

    await execution.report()

    [event] = events.sent
    assert isinstance(event, ExecutionFailed)
    assert event.payload.reason == "BLOCK_FAILED"
    assert event.payload.code == "CAST_INVALID_INPUT"
    assert event.payload.error.startswith("колонка amount: значения не приводятся к long")
    assert "driver exited with 1" not in event.payload.error


@pytest.mark.parametrize("block_error", [None, "блок упал"])
async def test_failure_points_to_driver_logs(block_error: str | None) -> None:
    execution, applications, events, results = controller()
    submit = command()
    if block_error:
        results.errors[submit.payload.context.output_prefix] = ExecutionError(
            code="INVALID_PARAMETERS", message=block_error
        )
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="driver exited with 1")

    await execution.report()

    [event] = events.sent
    assert isinstance(event, ExecutionFailed)
    logs = f"SparkApplication default/{name(submit)}, attempt_id {submit.attempt_id}"
    assert logs in event.payload.error
    assert event.payload.driver_logs == DriverLogs(
        namespace="default", spark_application=name(submit), attempt_id=submit.attempt_id
    )


async def test_completed_application_without_block_result_is_reported_failed() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(f"e3-{submit.attempt_id}", "COMPLETED")

    await execution.report()

    assert [event.type for event in events.sent] == ["ExecutionStarted", "ExecutionFailed"]
    assert isinstance(events.sent[1], ExecutionFailed)
    assert events.sent[1].payload.reason == "NO_RESULT"


async def test_application_not_yet_running_is_not_reported() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(f"e3-{submit.attempt_id}", "SUBMITTED")

    pending = await execution.report()

    assert (pending, events.sent) == (1, [])


async def test_spark_pods_request_size_from_submit_execution() -> None:
    execution, applications, _, _ = controller()

    await execution.submit(command(PodSize(cpu_millicores=250, memory_mib=1024)))

    [spec] = [app["spec"] for app in applications.items.values()]
    for pod in (spec["driver"], spec["executor"]):
        assert (pod["cores"], pod["coreRequest"], pod["memory"]) == (1, "250m", "1024m")


async def test_spark_pods_round_cores_up_to_cover_request() -> None:
    execution, applications, _, _ = controller()

    await execution.submit(command(PodSize(cpu_millicores=1500, memory_mib=512)))

    [spec] = [app["spec"] for app in applications.items.values()]
    assert (spec["driver"]["cores"], spec["driver"]["coreRequest"]) == (2, "1500m")


async def test_submit_execution_without_size_requests_whole_core_and_512m() -> None:
    execution, applications, _, _ = controller()
    old_format = command().model_dump(mode="json")
    del old_format["payload"]["pod"]

    await execution.submit(SubmitExecution.model_validate(old_format))

    [spec] = [app["spec"] for app in applications.items.values()]
    assert (spec["executor"]["coreRequest"], spec["executor"]["memory"]) == ("1000m", "512m")


def name(submit: SubmitExecution) -> str:
    return f"e3-{submit.attempt_id}"


async def test_failed_submission_is_reported_with_its_own_reason() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "SUBMISSION_FAILED", error="no such image")

    await execution.report()

    [event] = events.sent
    assert isinstance(event, ExecutionFailed)
    assert event.payload.reason == "SUBMISSION_FAILED"


@pytest.mark.parametrize(
    "pod", [Pod(phase="Running"), Pod(phase="Unknown"), Pod(phase="Failed", terminating=True)]
)
async def test_failed_application_is_not_reported_while_its_pod_may_live(pod: Pod) -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="executor lost")
    applications.set_pods(submit.attempt_id, Pod(phase="Failed"), pod)

    pending = await execution.report()

    assert (pending, events.sent) == (1, [])


async def test_failed_application_is_reported_once_its_pods_are_gone_or_finished() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="executor lost")
    applications.set_pods(submit.attempt_id, Pod(phase="Running"))
    await execution.report()

    applications.set_pods(submit.attempt_id, Pod(phase="Failed"), Pod(phase="Succeeded"))
    await execution.report()

    assert [event.type for event in events.sent] == ["ExecutionFailed"]


async def test_operator_rerun_is_not_an_outcome() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "PENDING_RERUN")

    await execution.report()

    assert events.sent == []


async def test_reconcile_of_finished_attempt_resends_same_outcome() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="driver exited with 1")
    await execution.report()

    await execution.handle(reconcile(submit, 1))

    first, again = events.sent
    assert again == first.model_copy(update={"occurred_at": again.occurred_at})


async def test_reconcile_of_live_attempt_sends_nothing() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "RUNNING")
    applications.set_pods(submit.attempt_id, Pod(phase="Running"))
    await execution.report()

    await execution.handle(reconcile(submit, 1))

    assert [event.type for event in events.sent] == ["ExecutionStarted"]
    assert FENCED_LABEL not in applications.items[name(submit)]["metadata"]["labels"]


async def test_reconcile_of_vanished_attempt_fences_it_then_reports_lost() -> None:
    execution, applications, events, _ = controller()
    submit = command()

    await execution.handle(reconcile(submit, 1))

    fence = applications.items[name(submit)]
    assert fence["spec"]["suspend"] is True
    original = parse_command(fence["metadata"]["annotations"][COMMAND_ANNOTATION].encode())
    assert isinstance(original, SubmitExecution)
    assert (original.attempt_id, original.payload) == (submit.attempt_id, submit.payload)
    assert fence["metadata"]["labels"][FENCED_LABEL] == "true"
    assert [event.type for event in events.sent] == ["ExecutionLost"]


async def test_fenced_attempt_is_reported_lost_only_after_its_pods_are_gone() -> None:
    execution, applications, events, results = controller()
    submit = command()
    applications.set_pods(submit.attempt_id, Pod(phase="Running", terminating=True))
    await execution.handle(reconcile(submit, 1))
    assert events.sent == []

    applications.set_pods(submit.attempt_id)
    restarted = ExecutionController(
        applications, events, results, FixedCredentials(), RecordedArtifacts(), CONFIG
    )
    await restarted.report()
    await restarted.report()

    assert [event.type for event in events.sent] == ["ExecutionLost"]


async def test_late_submit_after_fence_starts_no_spark() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    applications.set_pods(submit.attempt_id, Pod(phase="Running"))
    await execution.handle(reconcile(submit, 1))

    await execution.handle(submit)
    applications.set_state(name(submit), "RUNNING")
    await execution.report()

    assert applications.items[name(submit)]["spec"]["suspend"] is True
    assert events.sent == []


async def test_existing_real_application_is_never_fenced() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    applications.vanish_once(name(submit))
    await execution.submit(submit)

    await execution.handle(reconcile(submit, 1))

    assert "suspend" not in applications.items[name(submit)]["spec"]
    assert events.sent == []


async def test_cancel_of_attempt_not_yet_in_kubernetes_fences_it_and_reports_cancelled() -> None:
    execution, applications, events, _ = controller()
    submit = command()

    await execution.handle(cancel(submit, 1))

    labels = applications.items[name(submit)]["metadata"]["labels"]
    assert labels[FENCED_LABEL] == "true"
    assert labels[CANCELLED_LABEL] == "true"
    assert [event.type for event in events.sent] == ["ExecutionCancelled"]


async def test_cancel_of_running_attempt_replaces_it_with_fence_and_waits_for_pods() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "RUNNING")
    applications.set_pods(submit.attempt_id, Pod(phase="Running"))
    await execution.report()

    await execution.handle(cancel(submit, 1))
    assert applications.items[name(submit)]["spec"]["suspend"] is True
    assert [event.type for event in events.sent] == ["ExecutionStarted"]

    applications.set_pods(submit.attempt_id)
    await execution.report()

    assert [event.type for event in events.sent] == ["ExecutionStarted", "ExecutionCancelled"]


async def test_cancel_of_finished_attempt_reports_its_real_outcome() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="driver exited with 1")

    await execution.handle(cancel(submit, 1))

    assert FENCED_LABEL not in applications.items[name(submit)]["metadata"]["labels"]
    assert [event.type for event in events.sent] == ["ExecutionFailed"]


async def test_name_reoccupied_by_late_submit_is_freed_and_fenced_by_next_cancel() -> None:
    execution, applications, events, _ = controller()
    submit = command()
    await execution.submit(submit)
    applications.reoccupy_after_delete(spark_application(submit, CONFIG))
    await execution.handle(cancel(submit, 1))
    assert FENCED_LABEL not in applications.items[name(submit)]["metadata"]["labels"]

    await execution.handle(cancel(submit, 2))

    assert applications.items[name(submit)]["metadata"]["labels"][CANCELLED_LABEL] == "true"
    assert [event.type for event in events.sent] == ["ExecutionCancelled"]


DRIVER_LOG = PodLog(name="driver", text=b"INFO SparkContext: Running Spark\n")
EXECUTOR_LOG = PodLog(name="executor-1", text=b"INFO Executor: Finished task 0.0\n")


def artifact(submit: SubmitExecution, log: PodLog) -> str:
    return f"artifacts/{submit.node_run_id}/{submit.attempt_id}/{log.name}.log"


@pytest.mark.parametrize("state", ["COMPLETED", "FAILED"])
async def test_finished_attempt_leaves_driver_and_executor_logs_under_its_artifacts(
    state: str,
) -> None:
    artifacts = RecordedArtifacts()
    execution, applications, events, _ = controller(artifacts=artifacts)
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), state)
    applications.set_pods(submit.attempt_id, Pod(phase="Succeeded"), Pod(phase="Failed"))
    applications.set_logs(submit.attempt_id, DRIVER_LOG, EXECUTOR_LOG)

    await execution.report()

    assert artifacts.objects == {
        artifact(submit, DRIVER_LOG): DRIVER_LOG.text,
        artifact(submit, EXECUTOR_LOG): EXECUTOR_LOG.text,
    }
    assert events.sent[-1].type in {"ExecutionSucceeded", "ExecutionFailed"}


async def test_failed_artifact_write_is_retried_and_keeps_attempt_outcome() -> None:
    artifacts = RecordedArtifacts()
    execution, applications, events, _ = controller(artifacts=artifacts)
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="driver exited with 1")
    applications.set_logs(submit.attempt_id, DRIVER_LOG)
    artifacts.fail_once()
    await execution.report()
    assert artifacts.objects == {}

    await execution.report()

    assert artifacts.objects == {artifact(submit, DRIVER_LOG): DRIVER_LOG.text}
    [outcome] = events.sent
    assert isinstance(outcome, ExecutionFailed)
    assert outcome.payload.reason == "SPARK_FAILED"


async def test_failed_log_preservation_is_logged_with_attempt_trace_id(
    trace_logs: TraceLogs,
) -> None:
    artifacts = RecordedArtifacts()
    execution, applications, _, _ = controller(artifacts=artifacts)
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="driver exited with 1")
    applications.set_logs(submit.attempt_id, DRIVER_LOG)
    artifacts.fail_once()

    await execution.report()

    assert f"логи попытки {submit.attempt_id} не сохранены в artifacts/" in trace_logs("run-1")


async def test_failed_kubernetes_log_read_is_retried_and_keeps_attempt_outcome() -> None:
    artifacts = RecordedArtifacts()
    execution, applications, events, _ = controller(artifacts=artifacts)
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="driver exited with 1")
    applications.set_logs(submit.attempt_id, DRIVER_LOG)
    applications.fail_logs_once()
    await execution.report()
    assert artifacts.objects == {}

    await execution.report()

    assert artifacts.objects == {artifact(submit, DRIVER_LOG): DRIVER_LOG.text}
    assert [event.type for event in events.sent] == ["ExecutionFailed"]


async def test_attempt_whose_pods_are_deleted_is_reported_without_logs() -> None:
    artifacts = RecordedArtifacts()
    execution, _, events, _ = controller(artifacts=artifacts)
    submit = command()

    await execution.handle(reconcile(submit, 1))
    await execution.report()

    assert [event.type for event in events.sent] == ["ExecutionLost"]
    assert artifacts.objects == {}


async def test_saved_logs_are_not_written_again_on_later_passes() -> None:
    artifacts = RecordedArtifacts()
    execution, applications, _, _ = controller(artifacts=artifacts)
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "FAILED", error="driver exited with 1")
    applications.set_logs(submit.attempt_id, DRIVER_LOG, EXECUTOR_LOG)

    await execution.report()
    await execution.report()
    await execution.handle(reconcile(submit, 1))
    await execution.report()

    assert set(artifacts.objects) == {artifact(submit, DRIVER_LOG), artifact(submit, EXECUTOR_LOG)}
    assert artifacts.puts == 2


async def test_cancel_keeps_logs_of_running_attempt_before_its_pods_go() -> None:
    artifacts = RecordedArtifacts()
    execution, applications, events, _ = controller(artifacts=artifacts)
    submit = command()
    await execution.submit(submit)
    applications.set_state(name(submit), "RUNNING")
    applications.set_pods(submit.attempt_id, Pod(phase="Running"), Pod(phase="Running"))
    applications.set_logs(submit.attempt_id, DRIVER_LOG, EXECUTOR_LOG)

    await execution.handle(cancel(submit, 1))
    applications.set_pods(submit.attempt_id)
    applications.set_logs(submit.attempt_id)
    await execution.report()

    assert artifacts.objects == {
        artifact(submit, DRIVER_LOG): DRIVER_LOG.text,
        artifact(submit, EXECUTOR_LOG): EXECUTOR_LOG.text,
    }
    assert events.sent[-1].type == "ExecutionCancelled"


@pytest.mark.parametrize("missing", ["E3_S3_ACCESS_KEY", "E3_S3_SECRET_KEY"])
def test_controller_settings_require_explicit_s3_keys(
    monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    monkeypatch.delenv(missing)

    with pytest.raises(ValidationError, match=missing.removeprefix("E3_").lower()):
        ExecutionSettings(_env_file=None)  # pyright: ignore[reportCallIssue]


def test_controller_settings_hide_s3_keys_from_repr() -> None:
    settings = ExecutionSettings(_env_file=None)  # pyright: ignore[reportCallIssue]

    assert UNIT_S3_SECRET not in repr(settings)


class RecordedProducer:
    def __init__(self) -> None:
        self.headers: list[list[tuple[str, bytes]]] = []

    async def send_and_wait(
        self, topic: str, value: bytes, *, key: bytes, headers: list[tuple[str, bytes]]
    ) -> None:
        self.headers.append(headers)


async def test_event_goes_to_kafka_with_trace_id_header() -> None:
    producer = RecordedProducer()

    await KafkaEvents(cast("AIOKafkaProducer", producer)).send(started(command()))

    assert producer.headers == [[("trace_id", b"run-1")]]
