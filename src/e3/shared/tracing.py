"""Сквозной `trace_id` PipelineRun в логах обеих плоскостей.

`trace_id` запуска — его `pipeline_run_id`; он же лежит в envelope команд и
событий. Код, работающий на один запуск, оборачивается в `traced`: каждая
запись лога внутри получает `trace_id` отдельным полем JSON-строки, и по нему
grep находит путь от `POST /pipeline-runs` до публикации.
"""

import json
import logging
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime

_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)

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
