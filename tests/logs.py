"""Фикстура `trace_logs` для unit- и integration-тестов; их conftest её импортируют.

Не в корневом conftest: тесты блоков идут в образе Spark и код Control Plane не грузят.
"""

import logging
from collections.abc import Callable

import pytest

from e3.shared.tracing import TraceFilter

TraceLogs = Callable[[str], list[str]]


@pytest.fixture
def trace_logs(caplog: pytest.LogCaptureFixture) -> TraceLogs:
    """Тексты записей лога с данным `trace_id`, как их найдёт поиск по полю."""
    caplog.handler.addFilter(TraceFilter())
    caplog.set_level(logging.INFO)

    def messages(trace_id: str) -> list[str]:
        return [
            record.getMessage()
            for record in caplog.records
            if getattr(record, "trace_id", None) == trace_id
        ]

    return messages
