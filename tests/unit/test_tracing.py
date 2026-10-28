import json
import logging
import sys
from datetime import UTC, datetime
from uuid import UUID

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from e3.shared.tracing import (
    JsonFormatter,
    TraceFilter,
    continued,
    current_trace_id,
    propagation_headers,
    run_root_span,
    span,
    traced,
)

log = logging.getLogger("tests.tracing")


def formatted(message: str, *args: object) -> dict[str, object]:
    record = log.makeRecord(log.name, logging.INFO, __file__, 1, message, args, None)
    TraceFilter().filter(record)
    return json.loads(JsonFormatter().format(record))


def test_log_record_inside_trace_carries_trace_id_as_separate_field() -> None:
    with traced("run-1"):
        line = formatted("NodeRun %s продвинулся", "n-1")

    assert line["trace_id"] == "run-1"
    assert line["message"] == "NodeRun n-1 продвинулся"
    assert line["level"] == "INFO"
    assert line["logger"] == "tests.tracing"


def test_log_record_outside_trace_has_null_trace_id() -> None:
    assert formatted("без запуска")["trace_id"] is None


def test_trace_ends_with_its_block_and_restores_outer_one() -> None:
    with traced("outer"):
        with traced("inner"):
            assert current_trace_id() == "inner"
        assert current_trace_id() == "outer"

    assert current_trace_id() is None


def test_exception_goes_into_the_same_json_line() -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        record = log.makeRecord(log.name, logging.ERROR, __file__, 1, "упал", (), sys.exc_info())

    line = json.loads(JsonFormatter().format(record))

    assert "RuntimeError: boom" in line["exception"]


RUN_ID = UUID("0f8c3a52-6b1e-4d2a-9c7f-3e5d1a2b4c6d")


def finished(spans: InMemorySpanExporter, name: str) -> ReadableSpan:
    [found] = [span for span in spans.get_finished_spans() if span.name == name]
    return found


def ids(span: ReadableSpan) -> tuple[int, int | None]:
    """trace_id спана и span_id его родителя."""
    assert span.context is not None
    return span.context.trace_id, span.parent.span_id if span.parent else None


def test_root_span_of_run_has_run_id_as_trace_id_and_no_parent(
    spans: InMemorySpanExporter,
) -> None:
    started = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)

    with run_root_span("POST /pipeline-runs", str(RUN_ID), start_time=started):
        pass

    root = finished(spans, "POST /pipeline-runs")
    assert ids(root) == (RUN_ID.int, None)
    assert root.start_time == int(started.timestamp() * 1e9)


def test_span_of_run_without_current_span_hangs_on_run_root(
    spans: InMemorySpanExporter,
) -> None:
    with run_root_span("POST /pipeline-runs", str(RUN_ID)):
        pass

    with span("publish", str(RUN_ID)):
        assert current_trace_id() == str(RUN_ID)

    root = finished(spans, "POST /pipeline-runs")
    assert root.context is not None
    assert ids(finished(spans, "publish")) == (RUN_ID.int, root.context.span_id)


def test_span_continued_from_message_headers_is_child_of_sender(
    spans: InMemorySpanExporter,
) -> None:
    with span("send SubmitExecution", str(RUN_ID)):
        headers = propagation_headers()

    with continued(headers), span("handle SubmitExecution", str(RUN_ID)):
        pass

    sender = finished(spans, "send SubmitExecution")
    assert sender.context is not None
    assert ids(finished(spans, "handle SubmitExecution")) == (RUN_ID.int, sender.context.span_id)


def test_headers_of_other_trace_do_not_pull_span_out_of_its_run(
    spans: InMemorySpanExporter,
) -> None:
    other = UUID("11111111-2222-4333-8444-555555555555")
    with span("foreign", str(other)):
        headers = propagation_headers()

    with continued(headers), span("handle", str(RUN_ID)):
        pass

    assert ids(finished(spans, "handle"))[0] == RUN_ID.int


def test_span_with_non_uuid_trace_id_is_still_recorded(spans: InMemorySpanExporter) -> None:
    with span("submit", "e2e-suspend"):
        assert current_trace_id() == "e2e-suspend"

    assert finished(spans, "submit").name == "submit"
