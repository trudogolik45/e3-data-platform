"""Жизненный цикл `Read → Publish Dataset` через Kafka и Fake Execution Controller.

Шаги Run Orchestrator тест вызывает сам, по одному: так видны все
промежуточные состояния, и порядок не зависит от фоновых задач.
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from mypy_boto3_s3 import S3Client

from e3.api.app import create_app
from e3.runs import EventConsumer, OutboxRelay, RunOrchestrator, event_consumer, kafka_publisher
from e3.settings import Settings
from e3.shared.contracts import DatasetVersionRef
from e3.shared.messages import SubmitExecution, started, succeeded
from tests.fakes.execution_controller import FakeExecutionController
from tests.integration.conftest import Api, read_object
from tests.integration.test_start_pipeline_run import run_request

pytestmark = pytest.mark.integration


@dataclass
class Plant:
    api: Api
    orchestrator: RunOrchestrator
    relay: OutboxRelay
    events: EventConsumer
    fake: FakeExecutionController
    s3: S3Client
    settings: Settings

    async def start(self) -> str:
        response = await self.api.start_run(await run_request(self.api), key=str(uuid4()))
        assert response.status_code == 202, response.text
        return response.json()["pipeline_run_id"]

    async def node(self, run_id: str, node_id: str) -> dict[str, Any]:
        return (await self.api.run_nodes(run_id))[node_id]

    async def state(self, run_id: str, node_id: str) -> str:
        return (await self.node(run_id, node_id))["state"]

    async def submitted(self, run_id: str) -> SubmitExecution:
        """Запускает узлы, отправляет outbox и ждёт команду для `read` этого запуска."""
        await drain(self.orchestrator.dispatch_pending)
        await drain(self.relay.publish_pending)
        read = UUID((await self.node(run_id, "read"))["node_run_id"])
        while True:
            command = await self.fake.next_command()
            if command.node_run_id == read:
                return command

    async def consume_until(self, run_id: str, node_id: str, state: str) -> None:
        for _ in range(EVENT_POLLS):
            if await self.state(run_id, node_id) == state:
                return
            await self.events.poll(timeout_ms=200)
        assert await self.state(run_id, node_id) == state

    async def finish(self) -> None:
        """Доводит запуск после принятого события: FINALIZING и публикация."""
        await self.orchestrator.finalize_pending()
        await self.orchestrator.publish_pending()


EVENT_POLLS = 50


async def drain(step: Callable[[], Awaitable[int]]) -> None:
    while await step():
        pass


@pytest.fixture
async def plant(kafka: str, settings: Settings, s3: S3Client) -> AsyncIterator[Plant]:
    app = create_app(settings)
    orchestrator: RunOrchestrator = app.state.runs
    transport = httpx.ASGITransport(app=app)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://e3") as client,
        kafka_publisher(kafka) as publisher,
        event_consumer(kafka, f"test-{uuid4()}", orchestrator) as events,
        FakeExecutionController(kafka, s3, settings.s3_bucket) as fake,
    ):
        relay = orchestrator.outbox_relay(publisher)
        api = Api(client, settings.dev_project_id)
        yield Plant(api, orchestrator, relay, events, fake, s3, settings)


async def test_read_and_publish_pass_through_every_state(plant: Plant) -> None:
    run_id = await plant.start()
    assert (await plant.api.run(run_id))["state"] == "PENDING"
    assert await plant.state(run_id, "read") == "PENDING"

    command = await plant.submitted(run_id)
    assert (await plant.api.run(run_id))["state"] == "RUNNING"
    assert await plant.state(run_id, "read") == "QUEUED"

    result = plant.fake.execute(command)
    await plant.fake.send(started(command))
    await plant.consume_until(run_id, "read", "RUNNING")

    await plant.fake.send(succeeded(command, result))
    await plant.consume_until(run_id, "read", "FINALIZING")

    await plant.orchestrator.finalize_pending()
    assert await plant.state(run_id, "read") == "SUCCEEDED"
    assert await plant.state(run_id, "publish") == "RUNNING"

    await plant.orchestrator.publish_pending()
    assert await plant.state(run_id, "publish") == "SUCCEEDED"
    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"


async def test_publication_carries_node_output_manifest_into_parquet_version(
    plant: Plant,
) -> None:
    run_id = await plant.start()
    await plant.fake.succeed(await plant.submitted(run_id))
    await plant.consume_until(run_id, "read", "FINALIZING")

    await plant.finish()

    target = (await plant.api.run(run_id))["outputs"]["result"]["dataset_id"]
    [version] = await plant.api.versions(UUID(target))
    publish = await plant.node(run_id, "publish")
    [output] = (await plant.node(run_id, "read"))["outputs"]
    assert version["format"] == "PARQUET"
    assert version["version_id"] == publish["published_version_id"]
    assert published_manifest(plant, target, version) == read_object(
        plant.s3, plant.settings, output["manifest_uri"]
    )


async def test_submit_execution_carries_resolved_csv_input(plant: Plant) -> None:
    run_id = await plant.start()

    command = await plant.submitted(run_id)

    source = command.payload.context.inputs["source"]
    run = await plant.api.run(run_id)
    assert source.format == "CSV"
    assert source.manifest_uri.endswith("/manifest.json")
    assert isinstance(source.source, DatasetVersionRef)
    assert str(source.source.version_id) == run["inputs"]["source"]["version_id"]
    assert command.payload.context.output_prefix.endswith(f"executions/{command.attempt_id}")


async def test_redelivered_succeeded_creates_no_second_output_or_version(plant: Plant) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    event = await plant.fake.succeed(command)
    await plant.consume_until(run_id, "read", "FINALIZING")
    await plant.finish()

    await plant.fake.send(event)
    await plant.fake.send(succeeded(command, event.payload))
    await plant.events.poll(timeout_ms=1000)
    await plant.finish()

    target = (await plant.api.run(run_id))["outputs"]["result"]["dataset_id"]
    assert len(await plant.api.versions(UUID(target))) == 1
    assert len((await plant.node(run_id, "read"))["outputs"]) == 1
    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"


async def test_failed_execution_fails_run_without_publication(plant: Plant) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)

    await plant.fake.fail(command, "executor lost")
    await plant.consume_until(run_id, "read", "FAILED")
    await plant.finish()

    run = await plant.api.run(run_id)
    read = await plant.node(run_id, "read")
    assert run["state"] == "FAILED"
    assert read["error"] == "executor lost"
    assert await plant.state(run_id, "publish") == "SKIPPED"
    assert await plant.api.versions(UUID(run["outputs"]["result"]["dataset_id"])) == []


class BrokerDown:
    async def send(self, topic: str, *, key: bytes, value: bytes) -> None:
        raise ConnectionError("Kafka недоступна")


async def test_command_survives_crash_between_state_change_and_publication(
    plant: Plant,
) -> None:
    run_id = await plant.start()
    await drain(plant.orchestrator.dispatch_pending)
    with pytest.raises(ConnectionError):
        await plant.orchestrator.outbox_relay(BrokerDown()).publish_pending()
    assert await plant.state(run_id, "read") == "QUEUED"

    command = await plant.submitted(run_id)

    assert str(command.node_run_id) == (await plant.node(run_id, "read"))["node_run_id"]


def published_manifest(plant: Plant, dataset_id: str, version: dict[str, Any]) -> bytes:
    uri = f"s3://{plant.settings.s3_bucket}/datasets/{dataset_id}/{version['version_id']}/manifest.json"
    return read_object(plant.s3, plant.settings, uri)
