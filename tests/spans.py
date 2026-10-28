"""Фикстура `spans`: спаны OpenTelemetry процесса теста в памяти.

Глобальный TracerProvider ставится один раз на процесс, как в сервисах;
между тестами память чистится.
"""

from collections.abc import Iterator

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from e3.shared.tracing import tracer_provider

_exporter = InMemorySpanExporter()
_installed = False


@pytest.fixture
def spans() -> Iterator[InMemorySpanExporter]:
    global _installed
    if not _installed:
        provider = tracer_provider("e3-tests")
        provider.add_span_processor(SimpleSpanProcessor(_exporter))
        trace.set_tracer_provider(provider)
        _installed = True
    _exporter.clear()
    yield _exporter
