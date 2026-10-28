"""Сквозной `trace_id` PipelineRun: от `POST /pipeline-runs` до публикации DatasetVersion.

`trace_id` запуска — его `pipeline_run_id`. Логи API, Run Orchestrator, outbox
relay и Execution Controller несут его отдельным полем, а Kafka-сообщения —
заголовком.
"""

from uuid import UUID

import pytest

from e3.runs import Publisher
from tests.integration.conftest import Plant, drain
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

    assert [("trace_id", run_id.encode())] in recording.headers


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
