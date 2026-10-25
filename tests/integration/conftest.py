import asyncio
import base64
import hashlib
import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from aiokafka.errors import KafkaError
from alembic import command
from alembic.config import Config
from fastapi import FastAPI
from mypy_boto3_s3 import S3Client
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.minio import MinioContainer
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer

from e3.api.app import create_app, create_storage
from e3.runs import (
    EventConsumer,
    OutboxRelay,
    Publisher,
    RunOrchestrator,
    ensure_topics,
    event_consumer,
    kafka_publisher,
)
from e3.settings import Settings
from e3.shared.messages import ExecutionEvent, SubmitExecution
from e3.shared.storage import ObjectStorage
from tests.fakes.execution_controller import FakeExecutionController
from tests.fakes.oidc import AUDIENCE as OIDC_AUDIENCE
from tests.fakes.oidc import ISSUER as OIDC_ISSUER
from tests.fakes.oidc import JWKS_URL as OIDC_JWKS_URL
from tests.fakes.oidc import FakeOidcProvider
from tests.integration.graphs import (
    block_spec,
    port,
    read_beside_side_graph,
    read_filter_aggregate_graph,
    read_graph,
)

pytestmark = pytest.mark.integration

BUCKET = "e3-lake"
PLATFORM_TOKEN = "platform-team-secret"
KAFKA_IMAGE = "apache/kafka:4.4.0"
KAFKA_STARTUP_ATTEMPTS = 60


@pytest.fixture(scope="session")
def postgres() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:17-alpine", driver="asyncpg") as container:
        yield container


@pytest.fixture(scope="session")
def minio() -> Iterator[MinioContainer]:
    with MinioContainer("cgr.dev/chainguard/minio:latest") as container:
        yield container


def kafka_container(host_port: int) -> DockerContainer:
    """Официальный `apache/kafka` в KRaft: KafkaContainer из testcontainers умеет только cp-kafka.

    Advertised listener должен знать внешний порт заранее, поэтому он выбран до старта.
    """
    env = {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://localhost:{host_port}",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
    }
    container = DockerContainer(KAFKA_IMAGE).with_bind_ports(9092, host_port)
    for name, value in env.items():
        container = container.with_env(name, value)
    return container


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("", 0))
        return probe.getsockname()[1]


@pytest.fixture(scope="session")
def kafka_container_bootstrap() -> Iterator[str]:
    port = free_port()
    with kafka_container(port):
        yield f"localhost:{port}"


@pytest.fixture(scope="session")
async def kafka(kafka_container_bootstrap: str) -> str:
    """Bootstrap брокера с созданными топиками `commands.v1` и `events.v1`."""
    for _ in range(KAFKA_STARTUP_ATTEMPTS):
        try:
            await ensure_topics(kafka_container_bootstrap)
        except KafkaError:
            await asyncio.sleep(1)
            continue
        return kafka_container_bootstrap
    raise RuntimeError("Kafka не поднялась")


@pytest.fixture(scope="session")
def base_settings(postgres: PostgresContainer, minio: MinioContainer) -> Settings:
    endpoint = f"{minio.get_container_host_ip()}:{minio.get_exposed_port(minio.port)}"
    settings = Settings(
        environment="dev",
        database_url=postgres.get_connection_url(),
        s3_endpoint_url=f"http://{endpoint}",
        s3_access_key=minio.access_key,
        s3_secret_key=minio.secret_key,
        s3_bucket=BUCKET,
        platform_token=SecretStr(PLATFORM_TOKEN),
    )
    _migrate(settings.database_url)
    _create_bucket(settings)
    return settings


def _migrate(database_url: str) -> None:
    alembic_config = Config("alembic.ini")
    alembic_config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_config, "head")


def _create_bucket(settings: Settings) -> None:
    s3_admin(settings).create_bucket(Bucket=settings.s3_bucket)


def s3_admin(settings: Settings) -> S3Client:
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
    )


@pytest.fixture
def settings(base_settings: Settings) -> Settings:
    return base_settings


def client_for(settings: Settings, storage: ObjectStorage | None = None) -> httpx.AsyncClient:
    app = create_app(settings, storage=storage)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://e3")


@pytest.fixture
async def client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    async with client_for(settings) as c:
        yield c


def sha256_b64(data: bytes) -> str:
    return base64.b64encode(hashlib.sha256(data).digest()).decode()


@dataclass
class UploadTicket:
    upload_id: UUID
    upload_url: str
    headers: dict[str, str]


class Api:
    """Тонкая обёртка над REST API v1, чтобы тесты читались как сценарии."""

    def __init__(self, client: httpx.AsyncClient, dev_project_id: UUID) -> None:
        self.client = client
        self.dev_project_id = dev_project_id

    async def create_dataset(self, name: str, project_id: UUID | None = None) -> httpx.Response:
        body = {"project_id": str(project_id or self.dev_project_id), "name": name}
        return await self.client.post("/api/v1/datasets", json=body)

    async def new_dataset(self, name: str) -> UUID:
        response = await self.create_dataset(name)
        assert response.status_code == 201, response.text
        return UUID(response.json()["dataset_id"])

    async def start_upload(self, dataset_id: UUID, data: bytes) -> UploadTicket:
        response = await self.client.post(
            f"/api/v1/datasets/{dataset_id}/uploads", json={"checksum_sha256": sha256_b64(data)}
        )
        assert response.status_code == 201, response.text
        body = response.json()
        return UploadTicket(UUID(body["upload_id"]), body["upload_url"], body["upload_headers"])

    async def put_file(self, upload: UploadTicket, data: bytes) -> None:
        async with httpx.AsyncClient() as s3:
            response = await s3.put(upload.upload_url, content=data, headers=upload.headers)
        assert response.status_code == 200, response.text

    async def complete(self, dataset_id: UUID, upload_id: UUID) -> httpx.Response:
        return await self.client.post(f"/api/v1/datasets/{dataset_id}/uploads/{upload_id}/complete")

    async def archive_dataset(self, dataset_id: UUID | str) -> httpx.Response:
        return await self.client.post(f"/api/v1/datasets/{dataset_id}/archive")

    async def versions(self, dataset_id: UUID) -> list[dict[str, object]]:
        response = await self.client.get(f"/api/v1/datasets/{dataset_id}/versions")
        assert response.status_code == 200, response.text
        return response.json()

    async def register_block(
        self, spec: dict[str, Any], token: str | None = PLATFORM_TOKEN
    ) -> httpx.Response:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return await self.client.post("/api/v1/blocks", json=spec, headers=headers)

    async def create_pipeline(self, name: str, project_id: UUID | None = None) -> httpx.Response:
        body = {"project_id": str(project_id or self.dev_project_id), "name": name}
        return await self.client.post("/api/v1/pipelines", json=body)

    async def new_pipeline(self, name: str) -> UUID:
        response = await self.create_pipeline(name)
        assert response.status_code == 201, response.text
        return UUID(response.json()["pipeline_id"])

    async def save_draft(self, pipeline_id: UUID, graph: dict[str, Any]) -> httpx.Response:
        return await self.client.put(f"/api/v1/pipelines/{pipeline_id}/draft", json=graph)

    async def publish(self, pipeline_id: UUID) -> httpx.Response:
        return await self.client.post(f"/api/v1/pipelines/{pipeline_id}/versions")

    async def archive_pipeline(self, pipeline_id: UUID | str) -> httpx.Response:
        return await self.client.post(f"/api/v1/pipelines/{pipeline_id}/archive")

    async def pipeline_version(self, pipeline_id: UUID, version_number: int) -> httpx.Response:
        return await self.client.get(f"/api/v1/pipelines/{pipeline_id}/versions/{version_number}")

    async def start_run(self, body: dict[str, Any], key: str | None) -> httpx.Response:
        headers = {"Idempotency-Key": key} if key else {}
        return await self.client.post("/api/v1/pipeline-runs", json=body, headers=headers)

    async def run(self, run_id: str) -> dict[str, Any]:
        response = await self.client.get(f"/api/v1/pipeline-runs/{run_id}")
        assert response.status_code == 200, response.text
        return response.json()

    async def cancel_run(self, run_id: str) -> httpx.Response:
        return await self.client.post(f"/api/v1/pipeline-runs/{run_id}/cancel")

    async def run_nodes(self, run_id: str) -> dict[str, dict[str, Any]]:
        """NodeRun запуска по id узла графа."""
        response = await self.client.get(f"/api/v1/pipeline-runs/{run_id}/nodes")
        assert response.status_code == 200, response.text
        return {node["node_id"]: node for node in response.json()}

    async def upload_csv(self, dataset_id: UUID, data: bytes) -> httpx.Response:
        upload = await self.start_upload(dataset_id, data)
        await self.put_file(upload, data)
        return await self.complete(dataset_id, upload.upload_id)


@pytest.fixture
def s3(settings: Settings) -> S3Client:
    return s3_admin(settings)


def object_exists(s3: S3Client, settings: Settings, key: str) -> bool:
    found = s3.list_objects_v2(Bucket=settings.s3_bucket, Prefix=key)
    return any(item.get("Key") == key for item in found.get("Contents", []))


def read_object(s3: S3Client, settings: Settings, uri: str) -> bytes:
    key = uri.removeprefix(f"s3://{settings.s3_bucket}/")
    return s3.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()


@pytest.fixture
def api(client: httpx.AsyncClient, settings: Settings) -> Api:
    return Api(client, settings.dev_project_id)


RUN_CSV = b"id,name\n1,alpha\n2,beta\n"


PipelineMaker = Callable[["Api"], Awaitable[tuple[str, int]]]


async def new_block(api: Api, kind: str, output: str) -> str:
    name = f"{kind}-{uuid4().hex}"
    registered = await api.register_block(block_spec(name, [port("source")], [port(output)]))
    assert registered.status_code == 201, registered.text
    return name


async def publish_pipeline(api: Api, graph: dict[str, Any]) -> tuple[str, int]:
    pipeline_id = await api.new_pipeline(f"pipeline-{uuid4()}")
    await api.save_draft(pipeline_id, graph)
    published = await api.publish(pipeline_id)
    assert published.status_code == 201, published.text
    return str(pipeline_id), published.json()["version_number"]


async def new_read_pipeline(api: Api) -> tuple[str, int]:
    return await publish_pipeline(api, read_graph(await new_block(api, "read", "data")))


async def new_read_beside_side_pipeline(api: Api) -> tuple[str, int]:
    read, side = await new_block(api, "read", "data"), await new_block(api, "side", "data")
    return await publish_pipeline(api, read_beside_side_graph(read, side))


async def new_read_filter_aggregate_pipeline(api: Api) -> tuple[str, int]:
    read = await new_block(api, "read", "data")
    filter_block = await new_block(api, "filter", "result")
    aggregate = await new_block(api, "aggregate", "result")
    return await publish_pipeline(api, read_filter_aggregate_graph(read, filter_block, aggregate))


async def run_request(api: Api, pipeline: PipelineMaker = new_read_pipeline) -> dict[str, Any]:
    """Тело `POST /pipeline-runs` для пайплайна с Input Slot `source` и Output Slot `result`."""
    pipeline_id, version_number = await pipeline(api)
    source = await api.new_dataset(f"source-{uuid4()}")
    uploaded = await api.upload_csv(source, RUN_CSV)
    assert uploaded.status_code == 200, uploaded.text
    target = await api.new_dataset(f"result-{uuid4()}")
    return {
        "pipeline_id": pipeline_id,
        "pipeline_version": version_number,
        "inputs": {
            "source": {
                "dataset_id": str(source),
                "version_id": uploaded.json()["version"]["version_id"],
            }
        },
        "outputs": {"result": {"dataset_id": str(target)}},
    }


def storage_like[S: ObjectStorage](cls: type[S], settings: Settings) -> S:
    """Подменное хранилище `cls` поверх настоящего MinIO стенда."""
    real = create_storage(settings)
    storage = cls.__new__(cls)
    storage.__dict__.update(real.__dict__)
    return storage


class Handler:
    """Обработчик событий Plant: после рестарта события идут новому Run Orchestrator."""

    def __init__(self, orchestrator: RunOrchestrator) -> None:
        self.orchestrator = orchestrator

    async def handle_event(self, event: ExecutionEvent) -> None:
        await self.orchestrator.handle_event(event)


@dataclass
class Plant:
    """Control Plane стенда, Kafka и Fake Execution Controller; шаги вызывает тест."""

    api: Api
    orchestrator: RunOrchestrator
    publisher: Publisher
    handler: Handler
    events: EventConsumer
    fake: FakeExecutionController
    s3: S3Client
    settings: Settings

    @property
    def relay(self) -> OutboxRelay:
        return self.orchestrator.outbox_relay(self.publisher)

    def restart(self) -> None:
        """Новый Run Orchestrator на той же БД: в памяти ничего не переживает."""
        self.orchestrator = create_app(self.settings).state.runs
        self.handler.orchestrator = self.orchestrator

    async def start(self, pipeline: PipelineMaker = new_read_pipeline) -> str:
        request = await run_request(self.api, pipeline)
        response = await self.api.start_run(request, key=str(uuid4()))
        assert response.status_code == 202, response.text
        return response.json()["pipeline_run_id"]

    async def node(self, run_id: str, node_id: str) -> dict[str, Any]:
        return (await self.api.run_nodes(run_id))[node_id]

    async def state(self, run_id: str, node_id: str) -> str:
        return (await self.node(run_id, node_id))["state"]

    async def attempts(self, run_id: str, node_id: str = "read") -> list[dict[str, Any]]:
        return (await self.node(run_id, node_id))["attempts"]

    async def submitted(self, run_id: str, node_id: str = "read") -> SubmitExecution:
        """Запускает узлы, отправляет outbox и ждёт SubmitExecution для `node_id` этого запуска."""
        await drain(self.orchestrator.dispatch_pending)
        await drain(self.relay.publish_pending)
        return await self.fake.submission(UUID((await self.node(run_id, node_id))["node_run_id"]))

    async def execute(self, run_id: str, node_id: str) -> SubmitExecution:
        """Проводит BLOCK-узел через Execution Plane и принимает его NodeOutput."""
        command = await self.submitted(run_id, node_id)
        await self.fake.succeed(command)
        await self.consume_until(run_id, node_id, "FINALIZING")
        await self.orchestrator.finalize_pending()
        assert await self.state(run_id, node_id) == "SUCCEEDED"
        return command

    async def consume_until(self, run_id: str, node_id: str, state: str) -> None:
        await self.consume_while(lambda: self._not_in(run_id, node_id, state))
        assert await self.state(run_id, node_id) == state

    async def consume_attempts(self, run_id: str, count: int, node_id: str = "read") -> None:
        """Читает события, пока у узла не станет `count` попыток."""

        async def fewer() -> bool:
            return len(await self.attempts(run_id, node_id)) < count

        await self.consume_while(fewer)
        assert len(await self.attempts(run_id, node_id)) == count

    async def consume_while(self, waiting: Callable[[], Awaitable[bool]]) -> None:
        for _ in range(EVENT_POLLS):
            if not await waiting():
                return
            await self.events.poll(timeout_ms=200)

    async def _not_in(self, run_id: str, node_id: str, state: str) -> bool:
        return await self.state(run_id, node_id) != state

    async def finish(self) -> None:
        """Доводит запуск после принятого события: FINALIZING и публикация."""
        await self.orchestrator.finalize_pending()
        await self.orchestrator.publish_pending()


EVENT_POLLS = 50


async def drain(step: Callable[[], Awaitable[int]]) -> None:
    while await step():
        pass


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


@pytest.fixture
async def plant(kafka: str, settings: Settings, s3: S3Client) -> AsyncIterator[Plant]:
    app = create_app(settings)
    orchestrator: RunOrchestrator = app.state.runs
    handler = Handler(orchestrator)
    transport = httpx.ASGITransport(app=app)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://e3") as client,
        kafka_publisher(kafka) as publisher,
        event_consumer(kafka, f"test-{uuid4()}", handler) as events,
        FakeExecutionController(kafka, s3, settings.s3_bucket) as fake,
    ):
        api = Api(client, settings.dev_project_id)
        yield Plant(api, orchestrator, publisher, handler, events, fake, s3, settings)


@pytest.fixture(scope="session")
def oidc() -> FakeOidcProvider:
    return FakeOidcProvider()


@pytest.fixture
def oidc_settings(base_settings: Settings) -> Settings:
    return base_settings.model_copy(
        update={
            "auth_mode": "oidc",
            "oidc_issuer": OIDC_ISSUER,
            "oidc_audience": OIDC_AUDIENCE,
            "oidc_jwks_url": OIDC_JWKS_URL,
        }
    )


@dataclass
class User:
    subject: str
    token: str
    user_id: UUID


class World:
    """OIDC-стенд: пользователи с токенами, их Project и клиенты от их имени."""

    def __init__(self, app: FastAPI, oidc: FakeOidcProvider, clients: AsyncExitStack) -> None:
        self.app = app
        self.oidc = oidc
        self._clients = clients

    async def client(self, token: str | None) -> httpx.AsyncClient:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://e3", headers=headers
        )
        return await self._clients.enter_async_context(client)

    async def api(self, user: User, project_id: UUID) -> Api:
        return Api(await self.client(user.token), project_id)

    async def user(self) -> User:
        """Новый User: первый вход через `GET /me`."""
        subject = uuid4().hex
        token = self.oidc.token(subject)
        me = await (await self.client(token)).get("/api/v1/me")
        assert me.status_code == 200, me.text
        return User(subject, token, UUID(me.json()["user_id"]))

    async def project(self, admin: User) -> UUID:
        platform = await self.client(PLATFORM_TOKEN)
        organization = await platform.post("/api/v1/organizations", json={"name": "org"})
        assert organization.status_code == 201, organization.text
        project = await platform.post(
            "/api/v1/projects",
            json={
                "organization_id": organization.json()["organization_id"],
                "name": f"project-{uuid4()}",
                "admin": {"user_id": str(admin.user_id)},
            },
        )
        assert project.status_code == 201, project.text
        return UUID(project.json()["project_id"])

    async def member(self, project_id: UUID, admin: User, role: str) -> User:
        user = await self.user()
        added = await (await self.client(admin.token)).post(
            f"/api/v1/projects/{project_id}/members",
            json={"user_id": str(user.user_id), "role": role},
        )
        assert added.status_code == 201, added.text
        return user

    async def set_role(self, project_id: UUID, admin: User, user: User, role: str) -> None:
        changed = await (await self.client(admin.token)).put(
            f"/api/v1/projects/{project_id}/members/{user.user_id}", json={"role": role}
        )
        assert changed.status_code == 200, changed.text


@pytest.fixture
async def world(oidc_settings: Settings, oidc: FakeOidcProvider) -> AsyncIterator[World]:
    app = create_app(oidc_settings, oidc_http=oidc.http())
    async with AsyncExitStack() as clients:
        yield World(app, oidc, clients)


@dataclass
class OidcPlant:
    """Plant на OIDC-стенде: `plant.api` — от имени `admin` нового `project_id`."""

    plant: Plant
    world: World
    admin: User
    project_id: UUID


@pytest.fixture
async def oidc_plant(
    kafka: str, world: World, oidc_settings: Settings, s3: S3Client
) -> AsyncIterator[OidcPlant]:
    orchestrator: RunOrchestrator = world.app.state.runs
    handler = Handler(orchestrator)
    admin = await world.user()
    project_id = await world.project(admin)
    async with (
        kafka_publisher(kafka) as publisher,
        event_consumer(kafka, f"test-{uuid4()}", handler) as events,
        FakeExecutionController(kafka, s3, oidc_settings.s3_bucket) as fake,
    ):
        api = await world.api(admin, project_id)
        plant = Plant(api, orchestrator, publisher, handler, events, fake, s3, oidc_settings)
        yield OidcPlant(plant, world, admin, project_id)
