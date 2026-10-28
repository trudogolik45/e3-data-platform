"""Стенд E2E мимо Execution Controller: Secret'ы в kind и Diagnostic Artifact в MinIO."""

import asyncio
from uuid import UUID

import boto3
from kubernetes_asyncio.client import ApiClient, CoreV1Api, V1Secret
from kubernetes_asyncio.client.exceptions import ApiException

from e3.execution.kubernetes import NOT_FOUND
from e3.execution.settings import ExecutionSettings
from e3.execution.workload import credentials_secret_name

# Root MinIO compose-стенда: у ключей контроллера на `artifacts/` есть только запись.
MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"
LOGS_TIMEOUT_SECONDS = 60


async def secret(name: str) -> V1Secret | None:
    settings = ExecutionSettings()  # pyright: ignore[reportCallIssue]
    api = ApiClient()
    try:
        return await CoreV1Api(api).read_namespaced_secret(name, settings.spark_namespace)
    except ApiException as error:
        if error.status != NOT_FOUND:
            raise
        return None
    finally:
        await api.close()


async def attempt_secret(attempt_id: UUID | str) -> V1Secret | None:
    """Secret с Attempt Credentials попытки."""
    return await secret(credentials_secret_name(UUID(str(attempt_id))))


async def attempt_logs(
    node_run_id: UUID | str, attempt_id: UUID | str, *names: str
) -> dict[str, bytes]:
    """Логи попытки под `artifacts/{node_run_id}/{attempt_id}/`, как только есть все `names`.

    Контроллер пишет их на проходе после итога, поэтому ждём.
    """
    prefix = f"artifacts/{node_run_id}/{attempt_id}/"
    async with asyncio.timeout(LOGS_TIMEOUT_SECONDS):
        while True:
            files = await asyncio.to_thread(_read_artifacts, prefix)
            if set(names) <= set(files):
                return files
            await asyncio.sleep(1)


def _read_artifacts(prefix: str) -> dict[str, bytes]:
    settings = ExecutionSettings()  # pyright: ignore[reportCallIssue]
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
        region_name=settings.s3_region,
    )
    listed = s3.list_objects_v2(Bucket=settings.s3_bucket, Prefix=prefix)
    keys = [item.get("Key", "") for item in listed.get("Contents", [])]
    return {
        key.removeprefix(prefix): s3.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()
        for key in keys
    }
