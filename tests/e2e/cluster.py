"""Secret'ы в kind глазами E2E: мимо Execution Controller, как `kubectl get secret`."""

from uuid import UUID

from kubernetes_asyncio.client import ApiClient, CoreV1Api, V1Secret
from kubernetes_asyncio.client.exceptions import ApiException

from e3.execution.kubernetes import NOT_FOUND
from e3.execution.settings import ExecutionSettings
from e3.execution.workload import credentials_secret_name


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
