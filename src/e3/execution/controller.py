"""Execution Controller: SubmitExecution → SparkApplication → события попытки."""

from typing import Any, Protocol

from e3.execution.workload import (
    COMMAND_ANNOTATION,
    REPORTED_LABEL,
    Reported,
    WorkloadConfig,
    spark_application,
)
from e3.shared.contracts import ExecutionContext, ExecutionResult
from e3.shared.messages import (
    ExecutionEvent,
    SubmitExecution,
    failed,
    parse_command,
    started,
    succeeded,
)

# `status.applicationState.state` Spark Operator.
COMPLETED = "COMPLETED"
STARTED_STATES = {"RUNNING", "SUCCEEDING", COMPLETED}
FAILED_STATES = {"FAILED", "SUBMISSION_FAILED"}


class SparkApplications(Protocol):
    async def create(self, manifest: dict[str, Any]) -> None:
        """Создаёт ресурс; уже существующий с тем же именем — не ошибка."""
        ...

    async def unreported(self) -> list[dict[str, Any]]:
        """Ресурсы попыток, о финале которых ещё не сообщили."""
        ...

    async def mark_reported(self, name: str, reported: Reported) -> None: ...


class Events(Protocol):
    async def send(self, event: ExecutionEvent) -> None: ...


class Results(Protocol):
    async def fetch(self, context: ExecutionContext) -> ExecutionResult | None:
        """ExecutionResult, который блок записал в `output_prefix`; нет — None."""
        ...


class ExecutionController:
    def __init__(
        self,
        applications: SparkApplications,
        events: Events,
        results: Results,
        config: WorkloadConfig,
    ) -> None:
        self._applications = applications
        self._events = events
        self._results = results
        self._config = config

    async def submit(self, command: SubmitExecution) -> None:
        await self._applications.create(spark_application(command, self._config))

    async def report(self) -> int:
        """Сообщает о сменах состояния попыток; возвращает число ещё не завершённых."""
        applications = await self._applications.unreported()
        for application in applications:
            await self._report(application)
        return len(applications)

    async def _report(self, application: dict[str, Any]) -> None:
        """Событие уходит до пометки: сбой между ними даёт повтор.

        Повтор несёт тот же `event_id`, поэтому его гасит inbox Run Orchestrator.
        """
        metadata = application["metadata"]
        command = parse_command(metadata["annotations"][COMMAND_ANNOTATION].encode())
        state = _application_state(application)
        if state.get("state") in STARTED_STATES and REPORTED_LABEL not in metadata["labels"]:
            await self._events.send(started(command))
            await self._applications.mark_reported(metadata["name"], Reported.STARTED)
        outcome = await self._outcome(command, state)
        if outcome is None:
            return
        await self._events.send(outcome)
        await self._applications.mark_reported(metadata["name"], Reported.DONE)

    async def _outcome(
        self, command: SubmitExecution, state: dict[str, Any]
    ) -> ExecutionEvent | None:
        if state.get("state") in FAILED_STATES:
            return failed(command, f"{state['state']}: {state.get('errorMessage', '')}")
        if state.get("state") != COMPLETED:
            return None
        result = await self._results.fetch(command.payload.context)
        if result is None:
            return failed(command, "блок завершился, но не записал ExecutionResult")
        return succeeded(command, result)


def _application_state(application: dict[str, Any]) -> dict[str, Any]:
    return application.get("status", {}).get("applicationState", {})
