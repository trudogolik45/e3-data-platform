"""SparkApplications (CRD Kubeflow Spark Operator), pod'ы и Secret'ы попыток в Kubernetes API."""

from typing import Any
from uuid import UUID

from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi
from kubernetes_asyncio.client.exceptions import ApiException

from e3.execution.workload import (
    ATTEMPT_LABEL,
    GROUP,
    PLURAL,
    REPORTED_LABEL,
    VERSION,
    Pod,
    Reported,
)

NOT_FOUND = 404
ALREADY_EXISTS = 409


class KubernetesSparkApplications:
    def __init__(self, api: CustomObjectsApi, core: CoreV1Api, namespace: str) -> None:
        self._api = api
        self._core = core
        self._namespace = namespace

    async def create(self, manifest: dict[str, Any]) -> bool:
        try:
            await self._api.create_namespaced_custom_object(
                GROUP, VERSION, self._namespace, PLURAL, manifest
            )
        except ApiException as error:
            if error.status != ALREADY_EXISTS:
                raise
            return False
        return True

    async def get(self, name: str) -> dict[str, Any] | None:
        try:
            found: dict[str, Any] = await self._api.get_namespaced_custom_object(
                GROUP, VERSION, self._namespace, PLURAL, name
            )
        except ApiException as error:
            if error.status != NOT_FOUND:
                raise
            return None
        return found

    async def has_secret(self, name: str) -> bool:
        try:
            await self._core.read_namespaced_secret(name, self._namespace)
        except ApiException as error:
            if error.status != NOT_FOUND:
                raise
            return False
        return True

    async def create_secret(self, manifest: dict[str, Any]) -> None:
        try:
            # Клиент принимает и dict, как тело custom object; заглушки ждут V1Secret.
            await self._core.create_namespaced_secret(
                self._namespace,
                manifest,  # pyright: ignore[reportArgumentType]
            )
        except ApiException as error:
            if error.status != ALREADY_EXISTS:
                raise

    async def delete(self, name: str) -> None:
        try:
            await self._api.delete_namespaced_custom_object(
                GROUP, VERSION, self._namespace, PLURAL, name
            )
        except ApiException as error:
            if error.status != NOT_FOUND:
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

    async def pods(self, attempt_id: UUID) -> list[Pod]:
        listed = await self._core.list_namespaced_pod(
            self._namespace, label_selector=f"{ATTEMPT_LABEL}={attempt_id}"
        )
        return [
            Pod(
                phase=pod.status.phase or "Unknown",
                # Заглушки типов клиента обещают datetime, а у живого pod'а там None.
                terminating=bool(pod.metadata.deletion_timestamp),
            )
            for pod in listed.items
        ]
