from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from e3.shared.contracts import ExecutionResult
from e3.shared.messages import ExecutionFailed, ExecutionSucceeded, Failure, parse_event
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


def test_each_message_gets_own_event_id_and_time() -> None:
    first, second = succeeded(), succeeded()

    assert first.event_id != second.event_id
    assert first.occurred_at <= datetime.now(UTC)


def test_unknown_event_type_is_rejected() -> None:
    raw = succeeded().to_bytes().replace(b"ExecutionSucceeded", b"ExecutionExploded")

    with pytest.raises(ValidationError):
        parse_event(raw)


def test_json_schema_publishes_kafka_messages() -> None:
    schemas = json_schemas()

    assert {"SubmitExecution", "ExecutionEvent"} <= set(schemas)
