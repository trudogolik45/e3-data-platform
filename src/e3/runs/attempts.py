"""ExecutionAttempt: состояния, причины провала и правило ретрая (ADR-0003).

Ретраи решает только Run Orchestrator: 1 попытка и до 3 ретраев, следующая —
после доказанной смерти предыдущей, как только для неё есть место в Project Quota.
"""

from enum import StrEnum

MAX_ATTEMPTS = 4


class AttemptState(StrEnum):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    LOST = "LOST"
    # Остановлена по отмене или Fail-fast; не ретраится.
    CANCELLED = "CANCELLED"


class AttemptReason(StrEnum):
    SPARK_FAILED = "SPARK_FAILED"
    SUBMISSION_FAILED = "SUBMISSION_FAILED"
    # Блок сам сообщил ошибку: она в данных или параметрах, повтор её не исправит.
    BLOCK_FAILED = "BLOCK_FAILED"
    NO_RESULT = "NO_RESULT"
    LOST = "LOST"
    # Ставит сам Orchestrator: проверка manifest в FINALIZING отвергла результат.
    OUTPUT_REJECTED = "OUTPUT_REJECTED"
    UNKNOWN = "UNKNOWN"


LIVE = frozenset({AttemptState.SUBMITTED, AttemptState.RUNNING})
RETRYABLE = frozenset(
    {AttemptReason.SPARK_FAILED, AttemptReason.SUBMISSION_FAILED, AttemptReason.LOST}
)


def should_retry(number: int, reason: AttemptReason) -> bool:
    """Попытка `number` провалилась по `reason`: нужна ли следующая."""
    return reason in RETRYABLE and number < MAX_ATTEMPTS
