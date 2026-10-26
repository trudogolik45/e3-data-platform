"""Сквозной запуск с фоновыми шагами Run Orchestrator, как в процессе Control Plane."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from mypy_boto3_s3 import S3Client

from n3.api.app import create_app
from n3.settings import Settings
from tests.fakes.execution_controller import FakeExecutionController
from tests.integration.conftest import Api, free_port, run_request

pytestmark = pytest.mark.integration

WAIT_SECONDS = 30


async def eventually(
    probe: Callable[[], Awaitable[dict[str, Any]]], done: Callable[[dict[str, Any]], bool]
) -> dict[str, Any]:
    async with asyncio.timeout(WAIT_SECONDS):
        while True:
            value = await probe()
            if done(value):
                return value
            await asyncio.sleep(0.2)


async def test_api_starts_and_accepts_runs_while_kafka_is_down(settings: Settings) -> None:
    unreachable = f"localhost:{free_port()}"
    app = create_app(settings.model_copy(update={"kafka_bootstrap_servers": unreachable}))
    transport = httpx.ASGITransport(app=app)
    async with (
        asyncio.timeout(10),
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://n3") as client,
    ):
        api = Api(client, settings.dev_project_id)
        response = await api.start_run(await run_request(api), key=str(uuid4()))

    assert response.status_code == 202, response.text


async def test_uploaded_csv_is_published_as_parquet_version(
    kafka: str, settings: Settings, s3: S3Client
) -> None:
    app = create_app(
        settings.model_copy(
            update={
                "kafka_bootstrap_servers": kafka,
                "kafka_group_id": f"test-{uuid4()}",
                "runs_idle_seconds": 0.1,
            }
        )
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://n3") as client,
        FakeExecutionController(kafka, s3, settings.s3_bucket) as fake,
    ):
        api = Api(client, settings.dev_project_id)
        started = await api.start_run(await run_request(api), key=str(uuid4()))
        run_id = started.json()["pipeline_run_id"]
        read = UUID((await api.run_nodes(run_id))["read"]["node_run_id"])
        command = await fake.submission(read)

        await fake.succeed(command)

        run = await eventually(lambda: api.run(run_id), lambda r: r["state"] == "SUCCEEDED")
        versions = await api.versions(UUID(run["outputs"]["result"]["dataset_id"]))
    assert [version["format"] for version in versions] == ["PARQUET"]
