"""Execution Controller: команды → SparkApplication, его состояния → события попытки.

Итог попытки (`Succeeded`, `Failed`, `Lost`, `Cancelled`) уходит, только когда она доказуемо
мертва (ADR-0003): `SparkApplication` терминален или заменён Attempt Fence, и у
попытки не осталось pod'ов, которые ещё работают или могут заработать. Сам
Controller ничего не перезапускает: ретраи решает Run Orchestrator.

Каждая запущенная попытка получает свои Attempt Credentials (ADR-0006) в Secret,
которым владеет её `SparkApplication`. Secret создаётся после ресурса: без
владельца он не останется. До его появления pod ждёт в `CreateContainerConfigError`.

Упавшая попытка с `error.json` блока — `BLOCK_FAILED` с его текстом, без него —
текст Spark Operator. В обоих случаях в ошибке указано, где логи driver.

После итога логи driver и executor попытки уходят в Diagnostic Artifact под
`artifacts/{node_run_id}/{attempt_id}/` ключами самого контроллера: pod мог упасть
по OOM, а Attempt Credentials на `artifacts/` не распространяются.
"""

import logging
from contextlib import AbstractContextManager
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from opentelemetry.trace import Span

from e3.execution.credentials import S3Keys
from e3.execution.workload import (
    COMMAND_ANNOTATION,
    REPORTED_LABEL,
    Pod,
    PodLog,
    Reported,
    WorkloadConfig,
    attempt_fence,
    credentials_secret,
    credentials_secret_name,
    is_cancelled,
    is_fence,
    spark_application,
    workload_name,
)
from e3.shared.contracts import (
    ExecutionContext,
    ExecutionError,
    ExecutionResult,
    diagnostic_artifact_prefix,
)
from e3.shared.messages import (
    CancelExecution,
    Command,
    DriverLogs,
    ExecutionEvent,
    ExecutionFailed,
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
from e3.shared.tracing import span, traced

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
LOG_CONTENT_TYPE = "text/plain; charset=utf-8"
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

    async def unpreserved(self) -> list[dict[str, Any]]:
        """Ресурсы попыток, об итоге которых сообщили, а логи ещё не сохранили."""
        ...

    async def mark_preserved(self, name: str) -> None:
        """Логи попытки уже в `artifacts/`: больше их не читать."""
        ...

    async def pods(self, attempt_id: UUID) -> list[Pod]:
        """Pod'ы driver и executor с меткой попытки."""
        ...

    async def pod_logs(self, attempt_id: UUID) -> list[PodLog]:
        """Логи pod'ов попытки; удалённый pod логов не даёт."""
        ...

    async def has_secret(self, name: str) -> bool: ...

    async def create_secret(self, manifest: dict[str, Any]) -> None:
        """Создаёт Secret; с таким именем уже есть — no-op."""
        ...


class Events(Protocol):
    async def send(self, event: ExecutionEvent) -> None: ...


class Results(Protocol):
    async def fetch(self, context: ExecutionContext) -> ExecutionResult | None:
        """ExecutionResult, который блок записал в `output_prefix`; нет — None."""
        ...

    async def fetch_error(self, context: ExecutionContext) -> ExecutionError | None:
        """Ошибка, которую упавший блок записал в `output_prefix`; нет или не читается — None."""
        ...


class AttemptCredentials(Protocol):
    async def issue(self, context: ExecutionContext) -> S3Keys:
        """Ключи только на входы из `context` и его `output_prefix`."""
        ...


class Artifacts(Protocol):
    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        """Пишет объект; тот же ключ — перезапись, а не второй объект."""
        ...


class ExecutionController:
    def __init__(
        self,
        applications: SparkApplications,
        events: Events,
        results: Results,
        credentials: AttemptCredentials,
        artifacts: Artifacts,
        config: WorkloadConfig,
    ) -> None:
        self._applications = applications
        self._events = events
        self._results = results
        self._credentials = credentials
        self._artifacts = artifacts
        self._config = config

    async def handle(self, command: Command) -> None:
        """Логи и спан обработки команды несут её `trace_id`."""
        with span(f"handle {command.type}", command.trace_id, attributes=command.span_attributes()):
            await self._handle(command)
            log.info("%s попытки %s обработана", command.type, command.attempt_id)

    async def _handle(self, command: Command) -> None:
        match command:
            case SubmitExecution():
                await self.submit(command)
            case ReconcileExecution():
                await self.reconcile(command)
            case CancelExecution():
                await self.cancel(command)

    async def submit(self, command: SubmitExecution) -> None:
        """Повтор команды дозаводит Secret, если прошлая обработка упала до него.

        Есть Secret — новых ключей не выдаём: лишнюю STS-сессию не отозвать.
        """
        await self._applications.create(spark_application(command, self._config))
        application = await self._applications.get(workload_name(command.attempt_id))
        if application is None:
            return
        if is_fence(application):
            log.warning("попытка %s огорожена Attempt Fence, Spark не запущен", command.attempt_id)
            return
        if await self._applications.has_secret(credentials_secret_name(command.attempt_id)):
            return
        keys = await self._credentials.issue(command.payload.context)
        await self._applications.create_secret(credentials_secret(application, keys))

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

        Логи живого задания сохраняются до удаления: вместе с ним уйдут и pod'ы. Сбой
        сохранения отмену не задерживает.
        """
        name = workload_name(command.attempt_id)
        application = await self._applications.get(name)
        if application is not None and _live(application):
            await self._save_logs(_command(application))
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
        """Сообщает о сменах состояния попыток и сохраняет логи завершённых.

        Возвращает число ещё не завершённых попыток.
        """
        applications = await self._applications.unreported()
        for application in applications:
            with traced(_command(application).trace_id):
                await self._report(application)
        for application in await self._applications.unpreserved():
            with traced(_command(application).trace_id):
                await self._preserve(application)
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
            await self._send(started(_command(application)))
            await self._applications.mark_reported(metadata["name"], Reported.STARTED)
        await self._settle(application)

    async def _settle(self, application: dict[str, Any]) -> None:
        outcome = await self._outcome(application)
        if outcome is None:
            return
        with _attempt_span(application, outcome):
            await self._send(outcome)
        await self._applications.mark_reported(application["metadata"]["name"], Reported.DONE)

    async def _send(self, event: ExecutionEvent) -> None:
        await self._events.send(event)
        log.info("%s попытки %s отправлено", event.type, event.attempt_id)

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
            return await self._failure(application, command, state)
        result = await self._results.fetch(command.payload.context)
        if result is None:
            error = "блок завершился, но не записал ExecutionResult"
            return _failed(application, command, error, "NO_RESULT")
        return succeeded(command, result)

    async def _failure(
        self, application: dict[str, Any], command: SubmitExecution, state: dict[str, Any]
    ) -> ExecutionFailed:
        """Ошибка блока из `error.json` точнее текста Spark Operator, и её повтор не исправит.

        Нет файла (OOM, вытеснение pod'а) или он не читается — сбой Spark, его ретраят.
        """
        error = await self._results.fetch_error(command.payload.context)
        if error is not None:
            return _failed(application, command, error.message, "BLOCK_FAILED", code=error.code)
        operator_error = f"{state['state']}: {state.get('errorMessage', '')}"
        return _failed(application, command, operator_error, FAILURE_REASONS[state["state"]])

    async def _preserve(self, application: dict[str, Any]) -> None:
        """Итог уже сообщён: сбой Kubernetes или S3 его не меняет, следующий проход повторит."""
        if await self._save_logs(_command(application)):
            await self._applications.mark_preserved(application["metadata"]["name"])

    async def _save_logs(self, command: SubmitExecution) -> bool:
        """Логи pod'ов попытки в её Diagnostic Artifact; сбой пишется в лог контроллера."""
        try:
            for pod_log in await self._applications.pod_logs(command.attempt_id):
                key = diagnostic_artifact_key(command, pod_log)
                await self._artifacts.put(key, pod_log.text, content_type=LOG_CONTENT_TYPE)
        except Exception:
            log.exception("логи попытки %s не сохранены в artifacts/", command.attempt_id)
            return False
        return True

    async def _dead(self, attempt_id: UUID) -> bool:
        """Ни один pod попытки уже не работает и не может заработать."""
        pods = await self._applications.pods(attempt_id)
        return all(pod.phase in FINISHED_PHASES and not pod.terminating for pod in pods)


def diagnostic_artifact_key(command: SubmitExecution, pod_log: PodLog) -> str:
    """Ключ выводится из попытки и pod'а: повторное сохранение перезаписывает тот же объект."""
    prefix = diagnostic_artifact_prefix(command.node_run_id, command.attempt_id)
    return f"{prefix}/{pod_log.name}.log"


def _command(application: dict[str, Any]) -> SubmitExecution:
    command = parse_command(application["metadata"]["annotations"][COMMAND_ANNOTATION].encode())
    if not isinstance(command, SubmitExecution):
        raise ValueError(f"в {COMMAND_ANNOTATION} не SubmitExecution: {command.type}")
    return command


def _failed(
    application: dict[str, Any],
    command: SubmitExecution,
    error: str,
    reason: FailureReason,
    *,
    code: str | None = None,
) -> ExecutionFailed:
    """Провал попытки с указателем на логи driver: подробности остаются там."""
    logs = _driver_logs(application, command.attempt_id)
    pointer = f"логи driver: SparkApplication {logs.namespace}/{logs.spark_application}"
    error = f"{error} ({pointer}, attempt_id {logs.attempt_id})"
    return failed(command, error, reason, code=code, driver_logs=logs)


def _driver_logs(application: dict[str, Any], attempt_id: UUID) -> DriverLogs:
    metadata = application["metadata"]
    return DriverLogs(
        namespace=metadata["namespace"], spark_application=metadata["name"], attempt_id=attempt_id
    )


def _live(application: dict[str, Any]) -> bool:
    return not is_fence(application) and _state(application).get("state") not in TERMINAL_STATES


def _state(application: dict[str, Any]) -> dict[str, Any]:
    return application.get("status", {}).get("applicationState", {})


def _attempt_span(
    application: dict[str, Any], outcome: ExecutionEvent
) -> AbstractContextManager[Span]:
    """Спан попытки Spark: от создания её ресурса до доказанного итога.

    Строится целиком в момент итога, из метаданных ресурса: переживает рестарт контроллера.
    """
    created = application["metadata"].get("creationTimestamp")
    return span(
        "Spark attempt",
        outcome.trace_id,
        start_time=datetime.fromisoformat(created) if created else None,
        attributes=outcome.span_attributes() | {"outcome": outcome.type},
    )
