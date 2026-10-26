"""Execution Controller: команды → SparkApplication, его состояния → события попытки.

Итог попытки (`Succeeded`, `Failed`, `Lost`, `Cancelled`) уходит, только когда она доказуемо
мертва (ADR-0003): `SparkApplication` терминален или заменён Attempt Fence, и у
попытки не осталось pod'ов, которые ещё работают или могут заработать. Сам
Controller ничего не перезапускает: ретраи решает Run Orchestrator.
"""

import logging
from typing import Any, Protocol
from uuid import UUID

from n3.execution.workload import (
    COMMAND_ANNOTATION,
    REPORTED_LABEL,
    Pod,
    Reported,
    WorkloadConfig,
    attempt_fence,
    is_cancelled,
    is_fence,
    spark_application,
    workload_name,
)
from n3.shared.contracts import ExecutionContext, ExecutionResult
from n3.shared.messages import (
    CancelExecution,
    Command,
    ExecutionEvent,
    FailureReason,
    ReconcileExecution,
    SubmitExecution,
    cancelled,
    failed,
    lost,
    parse_command,
    started,
    succeeded,
)

log = logging.getLogger(__name__)

# `status.applicationState.state` Spark Operator.
COMPLETED = "COMPLETED"
STARTED_STATES = {"RUNNING", "SUCCEEDING", COMPLETED}
# Провалы, из которых оператор уже ничего не перезапустит: `restartPolicy: Never`.
FAILURE_REASONS: dict[str, FailureReason] = {
    "FAILED": "SPARK_FAILED",
    "SUBMISSION_FAILED": "SUBMISSION_FAILED",
}
TERMINAL_STATES = {COMPLETED, *FAILURE_REASONS}
# Фазы pod'а, в которых он точно не работает; `Unknown` ничего не доказывает.
FINISHED_PHASES = {"Succeeded", "Failed"}


class SparkApplications(Protocol):
    async def create(self, manifest: dict[str, Any]) -> bool:
        """Создаёт ресурс; `False` — ресурс с таким именем уже есть."""
        ...

    async def get(self, name: str) -> dict[str, Any] | None: ...

    async def unreported(self) -> list[dict[str, Any]]:
        """Ресурсы попыток, о финале которых ещё не сообщили."""
        ...

    async def mark_reported(self, name: str, reported: Reported) -> None: ...

    async def delete(self, name: str) -> None:
        """Удаляет ресурс; уже удалённый — no-op."""
        ...

    async def pods(self, attempt_id: UUID) -> list[Pod]:
        """Pod'ы driver и executor с меткой попытки."""
        ...


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

    async def handle(self, command: Command) -> None:
        match command:
            case SubmitExecution():
                await self.submit(command)
            case ReconcileExecution():
                await self.reconcile(command)
            case CancelExecution():
                await self.cancel(command)

    async def submit(self, command: SubmitExecution) -> None:
        if await self._applications.create(spark_application(command, self._config)):
            return
        existing = await self._applications.get(workload_name(command.attempt_id))
        if existing is not None and is_fence(existing):
            log.warning("попытка %s огорожена Attempt Fence, Spark не запущен", command.attempt_id)

    async def reconcile(self, command: ReconcileExecution) -> None:
        """Заново шлёт доказанный итог; пропавший ресурс сначала заменяет Attempt Fence.

        Живая попытка или недоказанная смерть — ничего: Orchestrator спросит снова.
        Занятое имя (`409`) смерти не доказывает: решает перечитанный ресурс.
        """
        await self._fence_and_settle(command)

    async def cancel(self, command: CancelExecution) -> None:
        """Ставит на имя попытки Attempt Fence с меткой отмены и шлёт итог, когда она мертва.

        Живое задание удаляется; если опоздавшая SubmitExecution успеет снова занять имя,
        следующая CancelExecution удалит его ещё раз. Терминальное задание не трогаем:
        его итог уже доказуем.
        """
        name = workload_name(command.attempt_id)
        application = await self._applications.get(name)
        if application is not None and _live(application):
            await self._applications.delete(name)
        await self._fence_and_settle(command)

    async def _fence_and_settle(self, command: ReconcileExecution | CancelExecution) -> None:
        """Пустое имя попытки занимает Attempt Fence; итог уходит, если смерть доказана."""
        name = workload_name(command.attempt_id)
        application = await self._applications.get(name)
        if application is None:
            await self._applications.create(attempt_fence(command, self._config))
            application = await self._applications.get(name)
        if application is not None:
            await self._settle(application)

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
        if (
            not is_fence(application)
            and _state(application).get("state") in STARTED_STATES
            and REPORTED_LABEL not in metadata["labels"]
        ):
            await self._events.send(started(_command(application)))
            await self._applications.mark_reported(metadata["name"], Reported.STARTED)
        await self._settle(application)

    async def _settle(self, application: dict[str, Any]) -> None:
        outcome = await self._outcome(application)
        if outcome is None:
            return
        await self._events.send(outcome)
        await self._applications.mark_reported(application["metadata"]["name"], Reported.DONE)

    async def _outcome(self, application: dict[str, Any]) -> ExecutionEvent | None:
        """Итог мёртвой попытки; жива или смерть не доказана — None."""
        command = _command(application)
        if is_fence(application):
            if not await self._dead(command.attempt_id):
                return None
            return cancelled(command) if is_cancelled(application) else lost(command)
        state = _state(application)
        if state.get("state") not in TERMINAL_STATES or not await self._dead(command.attempt_id):
            return None
        if state["state"] in FAILURE_REASONS:
            error = f"{state['state']}: {state.get('errorMessage', '')}"
            return failed(command, error, FAILURE_REASONS[state["state"]])
        result = await self._results.fetch(command.payload.context)
        if result is None:
            return failed(command, "блок завершился, но не записал ExecutionResult", "NO_RESULT")
        return succeeded(command, result)

    async def _dead(self, attempt_id: UUID) -> bool:
        """Ни один pod попытки уже не работает и не может заработать."""
        pods = await self._applications.pods(attempt_id)
        return all(pod.phase in FINISHED_PHASES and not pod.terminating for pod in pods)


def _command(application: dict[str, Any]) -> SubmitExecution:
    command = parse_command(application["metadata"]["annotations"][COMMAND_ANNOTATION].encode())
    if not isinstance(command, SubmitExecution):
        raise ValueError(f"в {COMMAND_ANNOTATION} не SubmitExecution: {command.type}")
    return command


def _live(application: dict[str, Any]) -> bool:
    return not is_fence(application) and _state(application).get("state") not in TERMINAL_STATES


def _state(application: dict[str, Any]) -> dict[str, Any]:
    return application.get("status", {}).get("applicationState", {})
