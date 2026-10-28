import sys
from uuid import uuid4

import pytest
import spark_io
from spark_io import BlockError, put_error

from e3.shared.contracts import (
    ERROR_FILE_LIMIT,
    DatasetVersionRef,
    ExecutionContext,
    ExecutionError,
    ResolvedInput,
)
from e3.shared.manifest import Column, DataSchema
from tests.blocks.conftest import block

filter_block = block("filter")


class RecordedS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket: str, Key: str, Body: bytes, ContentType: str) -> None:
        self.objects[f"s3://{Bucket}/{Key}"] = Body


def context() -> ExecutionContext:
    attempt_id = uuid4()
    return ExecutionContext(
        node_run_id=uuid4(),
        attempt_id=attempt_id,
        inputs={
            "source": ResolvedInput(
                manifest_uri="s3://e3-lake/datasets/source/manifest.json",
                format="PARQUET",
                data_schema=DataSchema(columns=[Column(name="qty", type="long")]),
                source=DatasetVersionRef(dataset_id=uuid4(), version_id=uuid4()),
            )
        },
        parameters={},
        output_prefix=f"s3://e3-lake/staging/executions/{attempt_id}",
    )


def test_block_error_is_written_verbatim_to_error_file_under_output_prefix() -> None:
    s3, attempt = RecordedS3(), context()

    put_error(s3, attempt, BlockError("sum по колонке city: нужен long или double, а не string"))

    written = ExecutionError.model_validate_json(s3.objects[f"{attempt.output_prefix}/error.json"])
    assert written == ExecutionError(
        code="INVALID_PARAMETERS",
        message="sum по колонке city: нужен long или double, а не string",
    )


def test_long_block_error_still_fits_error_file_limit() -> None:
    s3, attempt = RecordedS3(), context()

    put_error(s3, attempt, BlockError("types: нет колонок " + "я" * 10_000))

    [written] = s3.objects.values()
    assert len(written) <= ERROR_FILE_LIMIT
    assert ExecutionError.model_validate_json(written).message.startswith("types: нет колонок я")


def test_error_with_long_column_and_escaped_text_still_fits_error_file_limit() -> None:
    s3, attempt = RecordedS3(), context()
    column = '"' * 300 + "😀" * 300

    put_error(s3, attempt, BlockError(f"колонка {column}: ошибка", column=column))

    [written] = s3.objects.values()
    error = ExecutionError.model_validate_json(written)
    assert len(written) <= ERROR_FILE_LIMIT
    assert error.column is None
    assert error.message.startswith('колонка "')


class StoppedSpark:
    def stop(self) -> None:
        """Сессию тестов останавливает её фикстура."""


def test_filter_block_error_is_written_to_error_file_and_fails_block(
    spark, monkeypatch: pytest.MonkeyPatch
) -> None:
    s3, attempt = RecordedS3(), context()
    frame = spark.createDataFrame([(5,)], "qty long")
    monkeypatch.setattr(sys, "argv", ["main.py", attempt.model_dump_json()])
    monkeypatch.setenv("E3_S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setattr(spark_io.boto3, "client", lambda *_, **__: s3)
    monkeypatch.setattr(spark_io, "session", lambda _: StoppedSpark())
    monkeypatch.setattr(spark_io, "load", lambda *_: frame)
    parameters = {"conditions": [{"column": "qty", "op": "eq", "value": "5"}]}

    with pytest.raises(BlockError):
        spark_io.run("filter", "result", lambda f, _: filter_block.transform(f, parameters))

    written = ExecutionError.model_validate_json(s3.objects[f"{attempt.output_prefix}/error.json"])
    assert written.message == "qty: значение '5' не подходит к типу long"
    assert f"{attempt.output_prefix}/result.json" not in s3.objects
