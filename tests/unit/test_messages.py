from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from e3.shared.contracts import ExecutionContext, ExecutionResult
from e3.shared.messages import (
    ExecutionFailed,
    ExecutionLost,
    ExecutionSucceeded,
    Failure,
    ReconcileExecution,
    Submission,
    SubmitExecution,
    failed,
    lost,
    parse_command,
    parse_event,
    reconcile,
    started,
)
from e3.shared.schemas import json_schemas


def succeeded() -> ExecutionSucceeded:
    node_run_id, attempt_id = uuid4(), uuid4()
    return ExecutionSucceeded(
        node_run_id=node_run_id,
        attempt_id=attempt_id,
        trace_id="trace-1",
        payload=ExecutionResult(
            node_run_id=node_run_id,
            attempt_id=attempt_id,
            outputs={"data": "s3://e3-lake/staging/executions/a/manifest.json"},
        ),
    )


def test_event_survives_round_trip_through_bytes() -> None:
    event = succeeded()

    assert parse_event(event.to_bytes()) == event


def test_event_type_selects_payload_model() -> None:
    failed = ExecutionFailed(
        node_run_id=uuid4(), attempt_id=uuid4(), trace_id="t", payload=Failure(error="boom")
    )

    parsed = parse_event(failed.to_bytes())

    assert isinstance(parsed, ExecutionFailed)
    assert parsed.payload.error == "boom"


def test_envelope_follows_prd_shape() -> None:
    body = succeeded().model_dump(mode="json")

    assert set(body) == {
        "event_id",
        "schema_version",
        "node_run_id",
        "attempt_id",
        "trace_id",
        "occurred_at",
        "type",
        "payload",
    }
    assert body["schema_version"] == 1
    assert body["type"] == "ExecutionSucceeded"


def test_events_of_different_attempts_get_own_event_id_and_time() -> None:
    first, second = succeeded(), succeeded()

    assert first.event_id != second.event_id
    assert first.occurred_at <= datetime.now(UTC)


def command() -> SubmitExecution:
    node_run_id, attempt_id = uuid4(), uuid4()
    return SubmitExecution(
        node_run_id=node_run_id,
        attempt_id=attempt_id,
        trace_id="trace-1",
        payload=Submission(
            executable_ref="registry.local/e3/read@sha256:" + "a" * 64,
            context=ExecutionContext(
                node_run_id=node_run_id,
                attempt_id=attempt_id,
                inputs={},
                parameters={},
                output_prefix=f"s3://e3-lake/staging/executions/{attempt_id}",
            ),
        ),
    )


def test_resent_event_of_attempt_keeps_event_id_for_inbox() -> None:
    submitted = command()

    assert started(submitted).event_id == started(submitted).event_id
    assert (
        failed(submitted, "a", "SPARK_FAILED").event_id
        == failed(submitted, "b", "NO_RESULT").event_id
    )


def test_events_of_different_types_or_attempts_have_different_ids() -> None:
    submitted = command()

    assert started(submitted).event_id != failed(submitted, "boom", "SPARK_FAILED").event_id
    assert started(submitted).event_id != started(command()).event_id
    assert started(submitted).event_id != submitted.event_id


def test_unknown_event_type_is_rejected() -> None:
    raw = succeeded().to_bytes().replace(b"ExecutionSucceeded", b"ExecutionExploded")

    with pytest.raises(ValidationError):
        parse_event(raw)


def test_json_schema_publishes_kafka_messages() -> None:
    schemas = json_schemas()

    assert {"SubmitExecution", "ReconcileExecution", "ExecutionCommand", "ExecutionEvent"} <= set(
        schemas
    )


def test_failure_of_old_format_has_unknown_reason() -> None:
    raw = (
        failed(command(), "boom", "SPARK_FAILED")
        .to_bytes()
        .replace(b',"reason":"SPARK_FAILED"', b"")
    )

    parsed = parse_event(raw)

    assert isinstance(parsed, ExecutionFailed)
    assert parsed.payload.reason == "UNKNOWN"


def test_controller_failure_carries_reason() -> None:
    event = parse_event(failed(command(), "boom", "SUBMISSION_FAILED").to_bytes())

    assert isinstance(event, ExecutionFailed)
    assert (event.payload.error, event.payload.reason) == ("boom", "SUBMISSION_FAILED")


def test_lost_attempt_is_own_event_with_stable_id() -> None:
    submitted = command()

    event = parse_event(lost(submitted).to_bytes())

    assert isinstance(event, ExecutionLost)
    assert event.event_id == lost(submitted).event_id
    assert event.event_id != failed(submitted, "boom", "SPARK_FAILED").event_id


def test_reconcile_carries_original_submission_and_id_per_check() -> None:
    submitted = command()

    first, second = reconcile(submitted, 1), reconcile(submitted, 2)

    assert parse_command(first.to_bytes()) == first
    assert first.payload == submitted.payload
    assert first.event_id == reconcile(submitted, 1).event_id
    assert first.event_id != second.event_id


def test_command_type_selects_model() -> None:
    submitted = command()

    assert parse_command(submitted.to_bytes()) == submitted
    assert isinstance(parse_command(reconcile(submitted, 1).to_bytes()), ReconcileExecution)
