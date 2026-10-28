"""Сквозной `trace_id` PipelineRun в логах и трассе OpenTelemetry обеих плоскостей.

`trace_id` запуска — его `pipeline_run_id`; он же лежит в envelope команд и
событий. Код, работающий на один запуск, оборачивается в `traced`: каждая
запись лога внутри получает `trace_id` отдельным полем JSON-строки, и по нему
grep находит путь от `POST /pipeline-runs` до публикации.

`span` к тому же пишет спан OpenTelemetry. У трассы запуска W3C trace_id — это
`pipeline_run_id` (UUID — те же 128 бит), а корневой спан `run_root_span`
открывает `POST /pipeline-runs` со span_id, выведенным из того же id. Поэтому
любой процесс, зная только `trace_id` запуска, вешает свои спаны на настоящий
корень, без похода в базу. Через Kafka контекст едет W3C-заголовком
`traceparent` (`propagation_headers`), и получатель (`continued`) становится
дочерним спаном отправителя. Спаны уходят в OTLP, только если задан
`OTEL_EXPORTER_OTLP_ENDPOINT`; иначе они не покидают процесс.
"""

import json
import logging
import os
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from uuid import UUID, uuid5

from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.trace import NonRecordingSpan, Span, SpanContext, TraceFlags
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)
# (trace_id, span_id) корневого спана запуска, который сейчас открывается.
_root_ids: ContextVar[tuple[int, int] | None] = ContextVar("root_ids", default=None)

_ROOT_SPAN_NAMESPACE = UUID("2b7e1f4a-9c3d-4e8b-a1f6-5d0c7e9b3a24")
_SPAN_ID_BITS = 64
# Сколько процесс ждёт отправки спанов при остановке.
FLUSH_MILLIS = 5000
_propagator = TraceContextTextMapPropagator()

# Логгеры uvicorn пишут своими обработчиками: их направляем в общий JSON.
_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


@contextmanager
def traced(trace_id: str) -> Generator[None]:
    token = _trace_id.set(trace_id)
    try:
        yield
    finally:
        _trace_id.reset(token)


def current_trace_id() -> str | None:
    return _trace_id.get()


@contextmanager
def span(
    name: str,
    trace_id: str,
    *,
    start_time: datetime | None = None,
    attributes: Mapping[str, str] | None = None,
) -> Generator[Span]:
    """Спан работы на запуск: дочерний текущего спана той же трассы, иначе — корня запуска."""
    with (
        traced(trace_id),
        _tracer().start_as_current_span(
            name, context=_parent(trace_id), start_time=_nanos(start_time), attributes=attributes
        ) as current,
    ):
        yield current


@contextmanager
def run_root_span(
    name: str, trace_id: str, *, start_time: datetime | None = None
) -> Generator[Span]:
    """Корневой спан трассы запуска; открывается один раз, при его создании."""
    run = _run_trace(trace_id)
    if run is None:
        with span(name, trace_id, start_time=start_time) as current:
            yield current
        return
    token = _root_ids.set((run, _root_span_id(run)))
    try:
        root = _tracer().start_span(name, context=Context(), start_time=_nanos(start_time))
    finally:
        _root_ids.reset(token)
    with traced(trace_id), trace.use_span(root, end_on_exit=True):
        yield root


def propagation_headers() -> list[tuple[str, bytes]]:
    """W3C `traceparent` текущего спана для заголовков Kafka."""
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return [(key, value.encode()) for key, value in carrier.items()]


@contextmanager
def continued(headers: Sequence[tuple[str, bytes | None]]) -> Generator[None]:
    """Делает спан отправителя сообщения текущим: спаны получателя — его дочерние."""
    carrier = {key: value.decode() for key, value in headers if value is not None}
    token = otel_context.attach(_propagator.extract(carrier))
    try:
        yield
    finally:
        otel_context.detach(token)


class RunIdGenerator(RandomIdGenerator):
    """Случайные id, кроме корня запуска: его ids задаёт `run_root_span`."""

    def generate_trace_id(self) -> int:
        forced = _root_ids.get()
        return forced[0] if forced else super().generate_trace_id()

    def generate_span_id(self) -> int:
        forced = _root_ids.get()
        return forced[1] if forced else super().generate_span_id()


def tracer_provider(service_name: str) -> TracerProvider:
    return TracerProvider(
        resource=Resource.create({"service.name": service_name}), id_generator=RunIdGenerator()
    )


def configure_tracing(service_name: str) -> None:
    """Спаны процесса уходят в OTLP по `OTEL_EXPORTER_OTLP_ENDPOINT`; без него — никуда."""
    provider = tracer_provider(service_name)
    if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)


def flush_spans(timeout_millis: int = FLUSH_MILLIS) -> None:
    """Досылает накопленные спаны процесса, не дольше `timeout_millis`."""
    provider = trace.get_tracer_provider()
    if isinstance(provider, TracerProvider):
        provider.force_flush(timeout_millis)


def _tracer() -> trace.Tracer:
    return trace.get_tracer("e3")


def _run_trace(trace_id: str) -> int | None:
    """W3C trace_id трассы запуска; `None` — `trace_id` не id запуска."""
    try:
        return UUID(trace_id).int
    except ValueError:
        return None


def _root_span_id(run: int) -> int:
    span_id = uuid5(_ROOT_SPAN_NAMESPACE, str(run)).int >> (128 - _SPAN_ID_BITS)
    return span_id or 1


def _parent(trace_id: str) -> Context | None:
    """`None` — текущий контекст: в нём уже спан этой трассы или трассы запуска нет."""
    run = _run_trace(trace_id)
    if run is None:
        return None
    current = trace.get_current_span().get_span_context()
    if current.is_valid and current.trace_id == run:
        return None
    root = SpanContext(
        run, _root_span_id(run), is_remote=True, trace_flags=TraceFlags(TraceFlags.SAMPLED)
    )
    return trace.set_span_in_context(NonRecordingSpan(root))


def _nanos(moment: datetime | None) -> int | None:
    return int(moment.timestamp() * 1e9) if moment else None


class TraceFilter(logging.Filter):
    """Ставит записи `trace_id` текущего запуска; вне запуска — `None`."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = current_trace_id()
        return True


class JsonFormatter(logging.Formatter):
    """Одна запись — одна JSON-строка: время, уровень, логгер, текст и `trace_id`."""

    def format(self, record: logging.LogRecord) -> str:
        line: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": getattr(record, "trace_id", None),
        }
        if record.exc_info:
            line["exception"] = self.formatException(record.exc_info)
        return json.dumps(line, ensure_ascii=False)


def configure_logging(level: int = logging.INFO) -> None:
    """Все логи процесса, включая uvicorn, — JSON-строки в stderr."""
    handler = logging.StreamHandler()
    handler.addFilter(TraceFilter())
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
    for name in _UVICORN_LOGGERS:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
