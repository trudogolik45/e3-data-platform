"""In-memory SparkApplications: как Kubernetes API, но без кластера.

Ресурс идентифицируется именем: повторное создание с тем же именем — no-op,
как ответ 409 AlreadyExists у настоящего API.
"""

from typing import Any

from e3.execution.workload import REPORTED_LABEL, Reported


class InMemorySparkApplications:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}

    async def create(self, manifest: dict[str, Any]) -> None:
        self.items.setdefault(manifest["metadata"]["name"], manifest)

    async def unreported(self) -> list[dict[str, Any]]:
        return [
            app
            for app in self.items.values()
            if app["metadata"]["labels"].get(REPORTED_LABEL) != Reported.DONE
        ]

    async def mark_reported(self, name: str, reported: Reported) -> None:
        self.items[name]["metadata"]["labels"][REPORTED_LABEL] = reported

    def set_state(self, name: str, state: str, error: str | None = None) -> None:
        """Имитирует Spark Operator: пишет `status.applicationState`."""
        application_state = {"state": state} | ({"errorMessage": error} if error else {})
        self.items[name]["status"] = {"applicationState": application_state}
