"""Attempt Credentials на настоящем MinIO STS: ключи попытки видят только свои входы и выход.

Ключи выдаёт пользователь Execution Controller с политикой из
`scripts/minio/execution-controller-policy.json`, как в docker compose.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import boto3
import pytest
from botocore.exceptions import ClientError
from minio.credentials.providers import StaticProvider
from minio.minioadmin import MinioAdmin
from mypy_boto3_s3 import S3Client
from testcontainers.community.minio import MinioContainer

from e3.execution.credentials import S3Keys, StsCredentials
from e3.settings import Settings
from e3.shared.contracts import DatasetVersionRef, ExecutionContext, ResolvedInput
from e3.shared.manifest import Column, DataSchema
from tests.integration.conftest import BUCKET

pytestmark = pytest.mark.integration

CONTROLLER_USER = "e3-execution-controller"
CONTROLLER_SECRET = "e3-execution-controller-secret"
CONTROLLER_POLICY = Path(__file__).parents[2] / "scripts/minio/execution-controller-policy.json"
# Не значение по умолчанию: тест видит, что настройка доходит до STS.
LIFETIME_SECONDS = 3600
CLOCK_SKEW = timedelta(seconds=5)


@pytest.fixture(scope="module")
def controller_user(minio: MinioContainer, base_settings: Settings) -> None:
    admin = MinioAdmin(
        endpoint=base_settings.s3_endpoint_url.removeprefix("http://"),
        credentials=StaticProvider(minio.access_key, minio.secret_key),
        secure=False,
    )
    admin.user_add(CONTROLLER_USER, CONTROLLER_SECRET)
    admin.policy_add(CONTROLLER_USER, str(CONTROLLER_POLICY))
    admin.attach_policy([CONTROLLER_USER], user=CONTROLLER_USER)


@pytest.fixture
def credentials(controller_user: None, base_settings: Settings) -> StsCredentials:
    sts = boto3.client(
        "sts",
        endpoint_url=base_settings.s3_endpoint_url,
        aws_access_key_id=CONTROLLER_USER,
        aws_secret_access_key=CONTROLLER_SECRET,
        region_name=base_settings.s3_region,
    )
    return StsCredentials(sts, duration_seconds=LIFETIME_SECONDS)


@dataclass(frozen=True)
class Lake:
    """Объекты рядом с попыткой: её вход и чужие данные, которых ей видеть нельзя."""

    context: ExecutionContext
    input_file: str
    output: str
    other_attempt: str
    other_version: str
    upload: str


@pytest.fixture
def lake(base_settings: Settings) -> Lake:
    admin = s3_client(base_settings, None)
    dataset, attempt_id = uuid4(), uuid4()
    lake = Lake(
        context=ExecutionContext(
            node_run_id=uuid4(),
            attempt_id=attempt_id,
            inputs={"source": _input(f"s3://{BUCKET}/datasets/{dataset}/v1/manifest.json")},
            parameters={},
            output_prefix=f"s3://{BUCKET}/staging/executions/{attempt_id}",
        ),
        input_file=f"datasets/{dataset}/v1/data/part-0.csv",
        output=f"staging/executions/{attempt_id}",
        other_attempt=f"staging/executions/{uuid4()}/data/manifest.json",
        other_version=f"datasets/{dataset}/v2/manifest.json",
        upload=f"staging/uploads/{uuid4()}",
    )
    keys = [f"datasets/{dataset}/v1/manifest.json", lake.input_file]
    for key in [*keys, lake.other_attempt, lake.other_version, lake.upload]:
        admin.put_object(Bucket=BUCKET, Key=key, Body=b"{}")
    return lake


def s3_client(settings: Settings, keys: S3Keys | None) -> S3Client:
    """Клиент на ключах попытки; без них — root MinIO из `base_settings`."""
    if keys is None:
        access, secret, token = (
            settings.s3_access_key.get_secret_value(),
            settings.s3_secret_key.get_secret_value(),
            None,
        )
    else:
        access, secret, token = keys.access_key_id, keys.secret_access_key, keys.session_token
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        aws_session_token=token,
        region_name=settings.s3_region,
    )


def _input(manifest_uri: str) -> ResolvedInput:
    return ResolvedInput(
        manifest_uri=manifest_uri,
        format="CSV",
        data_schema=DataSchema(columns=[Column(name="city", type="string")]),
        source=DatasetVersionRef(dataset_id=uuid4(), version_id=uuid4()),
    )


def _denied(call: Callable[[], object]) -> str:
    with pytest.raises(ClientError) as error:
        call()
    return error.value.response.get("Error", {}).get("Code", "")


async def test_attempt_keys_read_own_input_and_write_own_output(
    credentials: StsCredentials, lake: Lake, base_settings: Settings
) -> None:
    keys = await credentials.issue(lake.context)
    s3 = s3_client(base_settings, keys)

    data = s3.get_object(Bucket=BUCKET, Key=lake.input_file)["Body"].read()
    s3.put_object(Bucket=BUCKET, Key=f"{lake.output}/data/part-0.parquet", Body=b"parquet")
    listed = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{lake.output}/")
    s3.delete_object(Bucket=BUCKET, Key=f"{lake.output}/data/part-0.parquet")

    assert data == b"{}"
    assert [item.get("Key") for item in listed.get("Contents", [])] == [
        f"{lake.output}/data/part-0.parquet"
    ]


async def test_attempt_keys_cannot_read_foreign_data(
    credentials: StsCredentials, lake: Lake, base_settings: Settings
) -> None:
    keys = await credentials.issue(lake.context)
    s3 = s3_client(base_settings, keys)

    codes = [
        _denied(lambda key=key: s3.get_object(Bucket=BUCKET, Key=key))
        for key in (lake.other_attempt, lake.other_version, lake.upload)
    ]
    listing = _denied(lambda: s3.list_objects_v2(Bucket=BUCKET, Prefix="staging/"))

    assert codes == ["AccessDenied"] * 3
    assert listing == "AccessDenied"


async def test_attempt_keys_cannot_write_outside_output_prefix(
    credentials: StsCredentials, lake: Lake, base_settings: Settings
) -> None:
    keys = await credentials.issue(lake.context)
    s3 = s3_client(base_settings, keys)

    codes = [
        _denied(lambda key=key: s3.put_object(Bucket=BUCKET, Key=key, Body=b"x"))
        for key in (lake.input_file, lake.other_attempt, f"{lake.output}-sibling/x")
    ]
    deleted = _denied(lambda: s3.delete_object(Bucket=BUCKET, Key=lake.other_version))

    assert codes == ["AccessDenied"] * 3
    assert deleted == "AccessDenied"


async def test_attempt_keys_live_as_long_as_configured(
    credentials: StsCredentials, lake: Lake
) -> None:
    before = datetime.now(UTC)

    keys = await credentials.issue(lake.context)

    lifetime = timedelta(seconds=LIFETIME_SECONDS)
    assert before + lifetime - CLOCK_SKEW <= keys.expires_at <= datetime.now(UTC) + lifetime
