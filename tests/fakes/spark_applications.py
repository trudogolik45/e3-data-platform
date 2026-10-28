"""In-memory SparkApplications и pod'ы: как Kubernetes API, но без кластера.

Ресурс идентифицируется именем: повторное создание с тем же именем — no-op,
как ответ 409 AlreadyExists у настоящего API. Secret уходит вместе со своей
`SparkApplication`, как при сборке мусора по ownerReference.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from e3.execution.workload import PRESERVED_LABEL, REPORTED_LABEL, Pod, PodLog, Reported

# `metadata.creationTimestamp` в формате Kubernetes API.
TIMESTAMP = "%Y-%m-%dT%H:%M:%SZ"


class KubernetesUnavailable(Exception):
    """Kubernetes API не ответил на чтение логов."""


class InMemorySparkApplications:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.secrets: dict[str, dict[str, Any]] = {}
        self._pods: dict[UUID, list[Pod]] = {}
        self._logs: dict[UUID, list[PodLog]] = {}
        self._log_failures = 0
        self._hidden: set[str] = set()
        self._reoccupy: dict[str, dict[str, Any]] = {}

    async def create(self, manifest: dict[str, Any]) -> bool:
        name = manifest["metadata"]["name"]
        if name in self.items:
            return False
        self._store(manifest)
        return True

    def _store(self, manifest: dict[str, Any]) -> None:
        """Как API server: у каждого созданного ресурса свой `metadata.uid` и время создания."""
        manifest["metadata"]["uid"] = str(uuid4())
        manifest["metadata"]["creationTimestamp"] = datetime.now(UTC).strftime(TIMESTAMP)
        self.items[manifest["metadata"]["name"]] = manifest

    async def has_secret(self, name: str) -> bool:
        return name in self.secrets

    async def create_secret(self, manifest: dict[str, Any]) -> None:
        self.secrets.setdefault(manifest["metadata"]["name"], manifest)

    async def get(self, name: str) -> dict[str, Any] | None:
        if name in self._hidden:
            self._hidden.discard(name)
            return None
        return self.items.get(name)

    async def unreported(self) -> list[dict[str, Any]]:
        return [
            app
            for app in self.items.values()
            if app["metadata"]["labels"].get(REPORTED_LABEL) != Reported.DONE
        ]

    async def mark_reported(self, name: str, reported: Reported) -> None:
        self.items[name]["metadata"]["labels"][REPORTED_LABEL] = reported

    async def unpreserved(self) -> list[dict[str, Any]]:
        return [
            app
            for app in self.items.values()
            if app["metadata"]["labels"].get(REPORTED_LABEL) == Reported.DONE
            and PRESERVED_LABEL not in app["metadata"]["labels"]
        ]

    async def mark_preserved(self, name: str) -> None:
        self.items[name]["metadata"]["labels"][PRESERVED_LABEL] = "true"

    async def pods(self, attempt_id: UUID) -> list[Pod]:
        return list(self._pods.get(attempt_id, []))

    async def pod_logs(self, attempt_id: UUID) -> list[PodLog]:
        if self._log_failures:
            self._log_failures -= 1
            raise KubernetesUnavailable
        return list(self._logs.get(attempt_id, []))

    def fail_logs_once(self) -> None:
        self._log_failures += 1

    def set_logs(self, attempt_id: UUID, *logs: PodLog) -> None:
        """Логи pod'ов попытки, которые ещё можно прочитать."""
        self._logs[attempt_id] = list(logs)

    def set_state(self, name: str, state: str, error: str | None = None) -> None:
        """Имитирует Spark Operator: пишет `status.applicationState`."""
        application_state = {"state": state} | ({"errorMessage": error} if error else {})
        self.items[name]["status"] = {"applicationState": application_state}

    def set_pods(self, attempt_id: UUID, *pods: Pod) -> None:
        self._pods[attempt_id] = list(pods)

    async def delete(self, name: str) -> None:
        deleted = self.items.pop(name, None)
        if deleted is not None:
            self._collect_secrets(deleted["metadata"]["uid"])
        if name in self._reoccupy:
            self._store(self._reoccupy.pop(name))

    def _collect_secrets(self, owner_uid: str) -> None:
        self.secrets = {
            name: secret
            for name, secret in self.secrets.items()
            if all(o["uid"] != owner_uid for o in secret["metadata"]["ownerReferences"])
        }

    def reoccupy_after_delete(self, manifest: dict[str, Any]) -> None:
        """Сразу после следующего удаления имя снова занимает `manifest`.

        Гонка: опоздавшая SubmitExecution создала задание между удалением и забором.
        """
        self._reoccupy[manifest["metadata"]["name"]] = manifest

    def vanish_once(self, name: str) -> None:
        """Следующий `get` не видит ресурс: гонка удаления и создания по имени."""
        self._hidden.add(name)
