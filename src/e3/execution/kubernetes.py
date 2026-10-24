"""SparkApplications в Kubernetes API (CRD Kubeflow Spark Operator)."""

from typing import Any

from kubernetes_asyncio.client import CustomObjectsApi
from kubernetes_asyncio.client.exceptions import ApiException

from e3.execution.workload import (
    ATTEMPT_LABEL,
    GROUP,
    PLURAL,
    REPORTED_LABEL,
    VERSION,
    Reported,
)

ALREADY_EXISTS = 409


class KubernetesSparkApplications:
    def __init__(self, api: CustomObjectsApi, namespace: str) -> None:
        self._api = api
        self._namespace = namespace

    async def create(self, manifest: dict[str, Any]) -> None:
        try:
            await self._api.create_namespaced_custom_object(
                GROUP, VERSION, self._namespace, PLURAL, manifest
            )
        except ApiException as error:
            if error.status != ALREADY_EXISTS:
                raise

    async def unreported(self) -> list[dict[str, Any]]:
        listed: dict[str, Any] = await self._api.list_namespaced_custom_object(
            GROUP,
            VERSION,
            self._namespace,
            PLURAL,
            label_selector=f"{ATTEMPT_LABEL},{REPORTED_LABEL}!={Reported.DONE}",
        )
        return listed["items"]

    async def mark_reported(self, name: str, reported: Reported) -> None:
        # Клиент шлёт PATCH custom object как JSON Patch, поэтому тело — список операций.
        label = "/metadata/labels/" + REPORTED_LABEL.replace("/", "~1")
        await self._api.patch_namespaced_custom_object(
            GROUP,
            VERSION,
            self._namespace,
            PLURAL,
            name,
            [{"op": "add", "path": label, "value": str(reported)}],
        )
