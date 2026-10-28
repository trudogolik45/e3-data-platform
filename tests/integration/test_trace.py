"""Сквозной `trace_id` PipelineRun: от `POST /pipeline-runs` до публикации DatasetVersion.

`trace_id` запуска — его `pipeline_run_id`. Логи API, Run Orchestrator, outbox
relay и Execution Controller несут его отдельным полем, а Kafka-сообщения —
заголовком. Спаны OpenTelemetry всех шагов складываются в одну трассу с этим id.
"""

from uuid import UUID

import pytest
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from e3.runs import Publisher
from tests.integration.conftest import Plant, drain, new_read_pipeline, run_request
from tests.logs import TraceLogs

pytestmark = pytest.mark.integration


class RecordingPublisher:
    """Отправляет через настоящий Publisher и запоминает заголовки сообщений."""

    def __init__(self, publisher: Publisher) -> None:
        self._publisher = publisher
        self.headers: list[list[tuple[str, bytes]]] = []

    async def send(
        self, topic: str, *, key: bytes, value: bytes, headers: list[tuple[str, bytes]]
    ) -> None:
        await self._publisher.send(topic, key=key, value=value, headers=headers)
        self.headers.append(headers)


async def test_submit_execution_carries_run_trace_id_in_kafka_header(plant: Plant) -> None:
    run_id = await plant.start()
    await drain(plant.orchestrator.dispatch_pending)
    recording = RecordingPublisher(plant.publisher)

    await drain(plant.orchestrator.outbox_relay(recording).publish_pending)

    assert any(("trace_id", run_id.encode()) in headers for headers in recording.headers)


async def test_block_receives_run_trace_id_in_execution_context(plant: Plant) -> None:
    run_id = await plant.start()

    command = await plant.submitted(run_id)

    assert command.trace_id == run_id
    assert command.payload.context.trace_id == run_id


def without_status_requests(messages: list[str]) -> list[str]:
    """Тест сам опрашивает состояние запуска: эти GET не часть его пути."""
    return [message for message in messages if not message.startswith("GET ")]


async def test_logs_of_one_run_are_found_by_its_trace_id_from_request_to_publication(
    plant: Plant, trace_logs: TraceLogs
) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    controller = plant.fake.controller()
    await controller.handle(command)
    plant.fake.complete_spark(command)
    await controller.report()
    await plant.consume_until(run_id, "read", "FINALIZING")
    await plant.finish()

    version_id = (await plant.node(run_id, "publish"))["published_version_id"]
    attempt = command.attempt_id
    assert without_status_requests(trace_logs(run_id)) == [
        f"PipelineRun {run_id} принят",
        f"PipelineRun {run_id} запущен",
        f"попытка {attempt} NodeRun {command.node_run_id} поставлена",
        f"SubmitExecution попытки {attempt} отправлена в Kafka",
        f"SubmitExecution попытки {attempt} обработана",
        f"ExecutionStarted попытки {attempt} отправлено",
        f"ExecutionSucceeded попытки {attempt} отправлено",
        f"ExecutionStarted попытки {attempt} применено",
        f"ExecutionSucceeded попытки {attempt} применено",
        f"NodeOutput NodeRun {command.node_run_id} принят",
        f"DatasetVersion {version_id} опубликована",
    ]


async def test_requests_about_run_are_logged_with_its_trace_id(
    plant: Plant, trace_logs: TraceLogs
) -> None:
    run_id = await plant.start()

    await plant.api.run(run_id)
    await plant.api.run_nodes(run_id)
    await plant.api.cancel_run(run_id)

    path = f"/api/v1/pipeline-runs/{UUID(run_id)}"
    assert trace_logs(run_id)[1:] == [
        f"GET {path}",
        f"GET {path}/nodes",
        f"POST {path}/cancel",
        f"запрошена отмена PipelineRun {run_id}",
    ]


def of_run(spans: InMemorySpanExporter, run_id: str) -> list[ReadableSpan]:
    trace_id = UUID(run_id).int
    return [s for s in spans.get_finished_spans() if s.context and s.context.trace_id == trace_id]


async def test_spans_of_one_run_form_one_trace_from_request_to_publication(
    plant: Plant, spans: InMemorySpanExporter
) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    controller = plant.fake.controller()
    await controller.handle(command)
    plant.fake.complete_spark(command)
    await controller.report()
    await plant.consume_until(run_id, "read", "FINALIZING")
    await plant.finish()

    found = of_run(spans, run_id)
    span_ids = {s.context.span_id for s in found if s.context}
    assert [s.name for s in found if s.parent is None] == ["POST /pipeline-runs"]
    assert all(s.parent.span_id in span_ids for s in found if s.parent)
    assert {
        "accept PipelineRun",
        "dispatch PipelineRun",
        "submit NodeRun",
        "send SubmitExecution",
        "handle SubmitExecution",
        "Spark attempt",
        "handle ExecutionSucceeded",
        "finalize NodeRun",
        "publish NodeRun",
    } <= {s.name for s in found}


async def test_event_handling_is_child_of_the_spark_attempt_that_sent_it(
    plant: Plant, spans: InMemorySpanExporter
) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    controller = plant.fake.controller()
    await controller.handle(command)
    plant.fake.complete_spark(command)
    await controller.report()

    await plant.consume_until(run_id, "read", "FINALIZING")

    named = {s.name: s for s in of_run(spans, run_id)}
    attempt, handled = named["Spark attempt"], named["handle ExecutionSucceeded"]
    assert attempt.context and handled.parent
    assert handled.parent.span_id == attempt.context.span_id


async def test_repeated_post_adds_span_to_trace_of_the_same_run(
    plant: Plant, spans: InMemorySpanExporter
) -> None:
    request = await run_request(plant.api, new_read_pipeline)
    first = await plant.api.start_run(request, key="same-key")
    again = await plant.api.start_run(request, key="same-key")

    run_id = first.json()["pipeline_run_id"]
    assert again.json()["pipeline_run_id"] == run_id
    names = [s.name for s in of_run(spans, run_id)]
    assert names.count("POST /pipeline-runs") == 1
    assert names.count("accept PipelineRun") == 2
