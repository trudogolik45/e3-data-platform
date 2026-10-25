import asyncio
import base64
import hashlib
import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from aiokafka.errors import KafkaError
from alembic import command
from alembic.config import Config
from mypy_boto3_s3 import S3Client
from pydantic import SecretStr
from testcontainers.community.minio import MinioContainer
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer

from e3.api.app import create_app, create_storage
from e3.runs import ensure_topics
from e3.settings import Settings
from e3.shared.storage import ObjectStorage
from tests.integration.graphs import block_spec, dag_graph, port, read_graph

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

    async def pipeline_version(self, pipeline_id: UUID, version_number: int) -> httpx.Response:
        return await self.client.get(f"/api/v1/pipelines/{pipeline_id}/versions/{version_number}")

    async def start_run(self, body: dict[str, Any], key: str | None) -> httpx.Response:
        headers = {"Idempotency-Key": key} if key else {}
        return await self.client.post("/api/v1/pipeline-runs", json=body, headers=headers)

    async def run(self, run_id: str) -> dict[str, Any]:
        response = await self.client.get(f"/api/v1/pipeline-runs/{run_id}")
        assert response.status_code == 200, response.text
        return response.json()

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
    pipeline_id = await api.new_pipeline("run")
    await api.save_draft(pipeline_id, graph)
    published = await api.publish(pipeline_id)
    assert published.status_code == 201, published.text
    return str(pipeline_id), published.json()["version_number"]


async def new_read_pipeline(api: Api) -> tuple[str, int]:
    return await publish_pipeline(api, read_graph(await new_block(api, "read", "data")))


async def new_dag_pipeline(api: Api) -> tuple[str, int]:
    read = await new_block(api, "read", "data")
    filter_block = await new_block(api, "filter", "result")
    aggregate = await new_block(api, "aggregate", "result")
    return await publish_pipeline(api, dag_graph(read, filter_block, aggregate))


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
