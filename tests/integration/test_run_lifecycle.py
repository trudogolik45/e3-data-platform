"""Жизненный цикл PipelineRun через Kafka и Fake Execution Controller.

Основной сценарий — `Read → Publish Dataset`, многоузловой — `Read → Filter →
Aggregate → Publish Dataset`.

Шаги Run Orchestrator тест вызывает сам, по одному: так видны все
промежуточные состояния, и порядок не зависит от фоновых задач.
"""

import json
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from e3.api.app import create_app
from e3.runs import RunOrchestrator
from e3.runs.models import InboxRow, OutboxRow
from e3.shared.contracts import DatasetVersionRef, NodeOutputRef
from e3.shared.messages import failed, started, succeeded
from e3.shared.storage import ObjectStorage, StorageUnavailable
from tests.integration.conftest import (
    Plant,
    drain,
    new_read_filter_aggregate_pipeline,
    read_object,
    storage_like,
)

pytestmark = pytest.mark.integration


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


CHAIN = ["read", "filter", "aggregate"]


async def finished_chain(plant: Plant) -> dict[str, Any]:
    """Проводит `Read → Filter → Aggregate → Publish Dataset` до конца; отдаёт PipelineRun."""
    run_id = await plant.start(new_read_filter_aggregate_pipeline)
    for node_id in CHAIN:
        await plant.execute(run_id, node_id)
    await plant.orchestrator.publish_pending()
    return await plant.api.run(run_id)


async def test_multi_node_pipeline_publishes_one_version_from_last_block(plant: Plant) -> None:
    run = await finished_chain(plant)

    [version] = await plant.api.versions(UUID(run["outputs"]["result"]["dataset_id"]))
    [output] = (await plant.node(run["pipeline_run_id"], "aggregate"))["outputs"]
    assert run["state"] == "SUCCEEDED"
    assert published_manifest(plant, run["outputs"]["result"]["dataset_id"], version) == (
        read_object(plant.s3, plant.settings, output["manifest_uri"])
    )


async def test_intermediate_node_outputs_do_not_reach_catalog(plant: Plant) -> None:
    run = await finished_chain(plant)

    source = await plant.api.versions(UUID(run["inputs"]["source"]["dataset_id"]))
    target = await plant.api.versions(UUID(run["outputs"]["result"]["dataset_id"]))
    assert [v["version_id"] for v in source] == [run["inputs"]["source"]["version_id"]]
    assert len(target) == 1


async def test_downstream_receives_upstream_node_output_as_resolved_input(plant: Plant) -> None:
    run_id = await plant.start(new_read_filter_aggregate_pipeline)
    await plant.execute(run_id, "read")

    command = await plant.submitted(run_id, "filter")

    source = command.payload.context.inputs["source"]
    read = await plant.node(run_id, "read")
    [output] = read["outputs"]
    assert source.source == NodeOutputRef(node_run_id=UUID(read["node_run_id"]), port="data")
    assert source.manifest_uri == output["manifest_uri"]
    assert source.format == "PARQUET"


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


async def test_inbox_drops_event_already_received_even_if_state_accepts_it(
    plant: Plant,
) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)
    received = await plant.fake.send(started(command))
    await plant.consume_until(run_id, "read", "RUNNING")

    await plant.fake.send(
        failed(command, "повтор", "SPARK_FAILED").model_copy(update={"event_id": received.event_id})
    )
    await plant.events.poll(timeout_ms=1000)

    assert await plant.state(run_id, "read") == "RUNNING"
    assert await inbox_has(plant, received.event_id)
    await plant.fake.fail(command, "нет результата", "NO_RESULT")
    await plant.consume_until(run_id, "read", "FAILED")


async def inbox_has(plant: Plant, event_id: UUID) -> bool:
    engine = create_async_engine(plant.settings.database_url)
    try:
        async with engine.connect() as connection:
            found = await connection.scalar(
                select(InboxRow.event_id).where(InboxRow.event_id == event_id)
            )
    finally:
        await engine.dispose()
    return found is not None


async def test_failed_execution_fails_run_without_publication(plant: Plant) -> None:
    run_id = await plant.start()
    command = await plant.submitted(run_id)

    await plant.fake.fail(command, "нет результата", "NO_RESULT")
    await plant.consume_until(run_id, "read", "FAILED")
    await plant.finish()

    run = await plant.api.run(run_id)
    read = await plant.node(run_id, "read")
    assert run["state"] == "FAILED"
    assert read["error"] == "нет результата"
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


class StorageDown(ObjectStorage):
    async def read(self, key: str) -> bytes:
        raise StorageUnavailable("503 SlowDown")


class FirstReadBreaks(ObjectStorage):
    broken = False

    async def read(self, key: str) -> bytes:
        if not self.broken:
            self.broken = True
            raise RuntimeError("неожиданный сбой")
        return await super().read(key)


async def finalizing(plant: Plant) -> str:
    run_id = await plant.start()
    await plant.fake.succeed(await plant.submitted(run_id))
    await plant.consume_until(run_id, "read", "FINALIZING")
    return run_id


def orchestrator_with(plant: Plant, storage: ObjectStorage) -> RunOrchestrator:
    return create_app(plant.settings, storage=storage).state.runs


async def test_storage_outage_during_finalizing_is_not_reported_as_progress(
    plant: Plant,
) -> None:
    run_id = await finalizing(plant)
    outage = orchestrator_with(plant, storage_like(StorageDown, plant.settings))

    progressed = await outage.finalize_pending()

    assert progressed == 0
    assert await plant.state(run_id, "read") == "FINALIZING"
    await plant.finish()
    assert (await plant.api.run(run_id))["state"] == "SUCCEEDED"


async def test_unexpected_failure_of_one_node_does_not_stop_finalizing_others(
    plant: Plant,
) -> None:
    runs = [await finalizing(plant), await finalizing(plant)]
    flaky = orchestrator_with(plant, storage_like(FirstReadBreaks, plant.settings))

    progressed = await flaky.finalize_pending()

    states = sorted([await plant.state(run_id, "read") for run_id in runs])
    assert (progressed, states) == (1, ["FINALIZING", "SUCCEEDED"])


class ChecksOutboxIsFree:
    """Broker, который во время отправки проверяет, что строка outbox не заблокирована."""

    def __init__(self, plant: Plant) -> None:
        self._plant = plant
        self.resent_concurrently = 0

    async def send(self, topic: str, *, key: bytes, value: bytes) -> None:
        engine = create_async_engine(self._plant.settings.database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    select(OutboxRow.id)
                    .where(OutboxRow.id == UUID(json.loads(value)["event_id"]))
                    .with_for_update(nowait=True)
                )
        finally:
            await engine.dispose()
        concurrent = self._plant.orchestrator.outbox_relay(BrokerDown())
        self.resent_concurrently += await concurrent.publish_pending()


async def test_relay_holds_no_outbox_locks_while_waiting_for_broker(plant: Plant) -> None:
    run_id = await plant.start()
    await drain(plant.orchestrator.dispatch_pending)
    broker = ChecksOutboxIsFree(plant)

    sent = await plant.orchestrator.outbox_relay(broker).publish_pending()

    assert sent >= 1
    assert broker.resent_concurrently == 0
    assert await plant.orchestrator.outbox_relay(BrokerDown()).publish_pending() == 0
    assert await plant.state(run_id, "read") == "QUEUED"


def published_manifest(plant: Plant, dataset_id: str, version: dict[str, Any]) -> bytes:
    uri = f"s3://{plant.settings.s3_bucket}/datasets/{dataset_id}/{version['version_id']}/manifest.json"
    return read_object(plant.s3, plant.settings, uri)
