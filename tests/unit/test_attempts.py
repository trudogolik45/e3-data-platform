from typing import get_args

import pytest

from e3.runs.attempts import MAX_ATTEMPTS, AttemptReason, should_retry
from e3.shared.messages import FailureReason


def test_every_controller_reason_is_attempt_reason() -> None:
    assert set(get_args(FailureReason)) <= {reason.value for reason in AttemptReason}


@pytest.mark.parametrize("reason", ["SPARK_FAILED", "SUBMISSION_FAILED", "LOST"])
def test_retryable_failure_is_retried_until_fourth_attempt(reason: str) -> None:
    assert [should_retry(n, AttemptReason(reason)) for n in range(1, MAX_ATTEMPTS + 1)] == [
        True,
        True,
        True,
        False,
    ]


@pytest.mark.parametrize("reason", ["NO_RESULT", "OUTPUT_REJECTED", "UNKNOWN"])
def test_non_retryable_failure_is_never_retried(reason: str) -> None:
    assert not should_retry(1, AttemptReason(reason))
