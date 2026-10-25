"""In-memory SparkApplications и pod'ы: как Kubernetes API, но без кластера.

Ресурс идентифицируется именем: повторное создание с тем же именем — no-op,
как ответ 409 AlreadyExists у настоящего API.
"""

from typing import Any
from uuid import UUID

from e3.execution.workload import REPORTED_LABEL, Pod, Reported


class InMemorySparkApplications:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self._pods: dict[UUID, list[Pod]] = {}
        self._hidden: set[str] = set()
        self._reoccupy: dict[str, dict[str, Any]] = {}

    async def create(self, manifest: dict[str, Any]) -> bool:
        name = manifest["metadata"]["name"]
        if name in self.items:
            return False
        self.items[name] = manifest
        return True

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

    async def pods(self, attempt_id: UUID) -> list[Pod]:
        return list(self._pods.get(attempt_id, []))

    def set_state(self, name: str, state: str, error: str | None = None) -> None:
        """Имитирует Spark Operator: пишет `status.applicationState`."""
        application_state = {"state": state} | ({"errorMessage": error} if error else {})
        self.items[name]["status"] = {"applicationState": application_state}

    def set_pods(self, attempt_id: UUID, *pods: Pod) -> None:
        self._pods[attempt_id] = list(pods)

    async def delete(self, name: str) -> None:
        self.items.pop(name, None)
        if name in self._reoccupy:
            self.items[name] = self._reoccupy.pop(name)

    def reoccupy_after_delete(self, manifest: dict[str, Any]) -> None:
        """Сразу после следующего удаления имя снова занимает `manifest`.

        Гонка: опоздавшая SubmitExecution создала задание между удалением и забором.
        """
        self._reoccupy[manifest["metadata"]["name"]] = manifest

    def vanish_once(self, name: str) -> None:
        """Следующий `get` не видит ресурс: гонка удаления и создания по имени."""
        self._hidden.add(name)
