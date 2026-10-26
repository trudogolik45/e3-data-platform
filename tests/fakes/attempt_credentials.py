"""Attempt Credentials без MinIO STS: одни и те же ключи на любую попытку."""

from datetime import UTC, datetime

from e3.execution.credentials import S3Keys
from e3.shared.contracts import ExecutionContext

ATTEMPT_KEYS = S3Keys(
    access_key_id="attempt-access",
    secret_access_key="attempt-secret",
    session_token="attempt-token",
    expires_at=datetime(2030, 1, 1, tzinfo=UTC),
)


class StsUnavailable(Exception):
    """STS не ответил: Execution Controller упадёт, команда придёт снова."""


class FixedCredentials:
    def __init__(self) -> None:
        self.issued: list[ExecutionContext] = []
        self._failures = 0

    async def issue(self, context: ExecutionContext) -> S3Keys:
        if self._failures:
            self._failures -= 1
            raise StsUnavailable
        self.issued.append(context)
        return ATTEMPT_KEYS

    def fail_once(self) -> None:
        self._failures += 1
