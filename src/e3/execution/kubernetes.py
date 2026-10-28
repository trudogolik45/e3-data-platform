"""SparkApplications (CRD Kubeflow Spark Operator), pod'ы и Secret'ы попыток в Kubernetes API."""

from typing import Any
from uuid import UUID

from kubernetes_asyncio.client import CoreV1Api, CustomObjectsApi, V1PodList
from kubernetes_asyncio.client.exceptions import ApiException

from e3.execution.workload import (
    ATTEMPT_LABEL,
    GROUP,
    PLURAL,
    PRESERVED_LABEL,
    REPORTED_LABEL,
    VERSION,
    Pod,
    PodLog,
    Reported,
)

BAD_REQUEST = 400
NOT_FOUND = 404
ALREADY_EXISTS = 409
# Метки, которые Spark ставит pod'ам driver и executor.
SPARK_ROLE_LABEL = "spark-role"
SPARK_EXECUTOR_ID_LABEL = "spark-exec-id"
# Имена контейнеров, которые Spark даёт driver и executor.
DRIVER_CONTAINER = "spark-kubernetes-driver"
EXECUTOR_CONTAINER = "spark-kubernetes-executor"


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

    async def unpreserved(self) -> list[dict[str, Any]]:
        listed: dict[str, Any] = await self._api.list_namespaced_custom_object(
            GROUP,
            VERSION,
            self._namespace,
            PLURAL,
            label_selector=f"{ATTEMPT_LABEL},{REPORTED_LABEL}={Reported.DONE},!{PRESERVED_LABEL}",
        )
        return listed["items"]

    async def mark_reported(self, name: str, reported: Reported) -> None:
        await self._label(name, REPORTED_LABEL, str(reported))

    async def mark_preserved(self, name: str) -> None:
        await self._label(name, PRESERVED_LABEL, "true")

    async def _label(self, name: str, label: str, value: str) -> None:
        # Клиент шлёт PATCH custom object как JSON Patch, поэтому тело — список операций.
        path = "/metadata/labels/" + label.replace("/", "~1")
        await self._api.patch_namespaced_custom_object(
            GROUP,
            VERSION,
            self._namespace,
            PLURAL,
            name,
            [{"op": "add", "path": path, "value": value}],
        )

    async def pods(self, attempt_id: UUID) -> list[Pod]:
        listed = await self._attempt_pods(attempt_id)
        return [
            Pod(
                phase=pod.status.phase or "Unknown",
                # Заглушки типов клиента обещают datetime, а у живого pod'а там None.
                terminating=bool(pod.metadata.deletion_timestamp),
            )
            for pod in listed.items
        ]

    async def pod_logs(self, attempt_id: UUID) -> list[PodLog]:
        """`Pending` pod ещё не писал логов; удалённый после списка — тоже без них."""
        listed = await self._attempt_pods(attempt_id)
        logs: list[PodLog] = []
        for pod in listed.items:
            if pod.status.phase == "Pending":
                continue
            container, name = _spark_container(pod.metadata.labels or {}, pod.metadata.name)
            text = await self._log(pod.metadata.name, container)
            if text is not None:
                logs.append(PodLog(name=name, text=text))
        return logs

    async def _log(self, pod: str, container: str) -> bytes | None:
        """404 — pod удалён, 400 — у контейнера логов нет (не стартовал, вытеснен)."""
        try:
            text: str = await self._core.read_namespaced_pod_log(
                pod, self._namespace, container=container
            )
        except ApiException as error:
            if error.status not in {NOT_FOUND, BAD_REQUEST}:
                raise
            return None
        return text.encode()

    async def _attempt_pods(self, attempt_id: UUID) -> V1PodList:
        return await self._core.list_namespaced_pod(
            self._namespace, label_selector=f"{ATTEMPT_LABEL}={attempt_id}"
        )


def _spark_container(labels: dict[str, str], pod: str) -> tuple[str, str]:
    """Контейнер Spark в pod'е и имя его лога: `driver` или `executor-{id}`.

    Контейнер называется явно: рядом могут оказаться sidecar'ы.
    """
    if labels.get(SPARK_ROLE_LABEL) != "executor":
        return DRIVER_CONTAINER, "driver"
    return EXECUTOR_CONTAINER, f"executor-{labels.get(SPARK_EXECUTOR_ID_LABEL, pod)}"
