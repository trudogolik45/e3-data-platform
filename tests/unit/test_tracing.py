import json
import logging
import sys

from e3.shared.tracing import JsonFormatter, TraceFilter, current_trace_id, traced

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
