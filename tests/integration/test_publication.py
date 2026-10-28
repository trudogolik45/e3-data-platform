from uuid import uuid4

import pytest
from mypy_boto3_s3 import S3Client

from e3.api.app import create_app
from e3.catalog import Catalog, PublicationRejected
from e3.settings import Settings
from e3.shared.contracts import NodeOutputRef
from tests.fakes.execution_controller import write_parquet_output
from tests.integration.conftest import Api, read_object

pytestmark = pytest.mark.integration


@pytest.fixture
def catalog(settings: Settings) -> Catalog:
    return create_app(settings).state.catalog


def node_output_ref() -> NodeOutputRef:
    return NodeOutputRef(node_run_id=uuid4(), port="data")


def staged_output(s3: S3Client, settings: Settings) -> str:
    prefix = f"staging/executions/{uuid4()}"
    key = write_parquet_output(
        s3, settings.s3_bucket, prefix, {"id": ["1", "2"], "name": ["a", "b"]}
    )
    return f"s3://{settings.s3_bucket}/{key}"


async def test_publication_creates_parquet_version_with_same_manifest(
    api: Api, catalog: Catalog, s3: S3Client, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"result-{uuid4()}")
    manifest_uri = staged_output(s3, settings)

    version = await catalog.publish(
        project_id=settings.dev_project_id,
        dataset_id=dataset_id,
        source=node_output_ref(),
        manifest_uri=manifest_uri,
        by=settings.dev_user_id,
        trace_id="trace",
    )

    assert version.format == "PARQUET"
    assert [column.name for column in version.data_schema.columns] == ["id", "name"]
    assert read_object(s3, settings, version.manifest_uri) == read_object(
        s3, settings, manifest_uri
    )
    assert [v["version_id"] for v in await api.versions(dataset_id)] == [str(version.version_id)]


async def test_republishing_same_node_output_returns_existing_version(
    api: Api, catalog: Catalog, s3: S3Client, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"result-{uuid4()}")
    manifest_uri, source = staged_output(s3, settings), node_output_ref()

    first, second = [
        await catalog.publish(
            project_id=settings.dev_project_id,
            dataset_id=dataset_id,
            source=source,
            manifest_uri=manifest_uri,
            by=settings.dev_user_id,
            trace_id="trace",
        )
        for _ in range(2)
    ]

    assert first == second
    assert len(await api.versions(dataset_id)) == 1


async def test_publication_into_foreign_project_is_rejected(
    api: Api, catalog: Catalog, s3: S3Client, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"result-{uuid4()}")

    with pytest.raises(PublicationRejected):
        await catalog.publish(
            project_id=uuid4(),
            dataset_id=dataset_id,
            source=node_output_ref(),
            manifest_uri=staged_output(s3, settings),
            by=settings.dev_user_id,
            trace_id="trace",
        )

    assert await api.versions(dataset_id) == []
