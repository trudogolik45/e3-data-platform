"""Общая обвязка Spark-блоков: ExecutionContext → DataFrame → Parquet с manifest v1.

Spark Operator запускает `main.py '<ExecutionContext JSON>'`. Блок читает вход
`source` по его manifest, преобразует DataFrame и пишет только под
`output_prefix`: данные и manifest выхода, затем свой ExecutionResult в
`result.json` — его забирает Execution Controller. Ключи S3 — Attempt Credentials
из окружения (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`):
читают только входы попытки и пишут только под её `output_prefix`. Адрес —
`E3_S3_ENDPOINT_URL`.

Spark работает в ANSI-режиме: неверное приведение типа и переполнение роняют
блок, а не превращаются молча в `null`.

Распознанную ошибку (`BlockError`) блок пишет в `error.json` под `output_prefix`
и падает: Execution Controller отдаст её текст в `NodeRun.error`. Ошибки Spark
ленивые и всплывают при записи, поэтому распознаются вокруг неё.
"""

import base64
import hashlib
import os
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

import boto3
from botocore.config import Config
from py4j.protocol import Py4JError, Py4JJavaError
from pydantic import ValidationError
from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType

from e3.shared.contracts import (
    ERROR_FILE,
    ERROR_FILE_LIMIT,
    RESULT_FILE,
    ExecutionContext,
    ExecutionError,
    ExecutionResult,
    ResolvedInput,
)
from e3.shared.manifest import Column, DataSchema, Manifest, ManifestFile

INPUT_PORT = "source"
DATA_DIR = "parquet"
MANIFEST_FILE = "manifest.json"
NUMERIC = frozenset({"long", "double"})
# Длиннее — колонка не попадёт в `error.json`: иначе текст ошибки не влезет в лимит.
COLUMN_LIMIT = 256

Parameters = dict[str, Any]
Transform = Callable[[DataFrame, Parameters], DataFrame]

_COLUMN_TYPES = {StringType(): "string", LongType(): "long", DoubleType(): "double"}


class BlockError(Exception):
    """Параметры или данные не подходят блоку: текст — причина падения для `NodeRun.error`.

    Значений из данных в тексте нет: только колонка и тип.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str = "INVALID_PARAMETERS",
        column: str | None = None,
        target_type: str | None = None,
    ) -> None:
        super().__init__(message)
        self.execution_error = _fitting(
            ExecutionError(code=code, message=message, column=column, target_type=target_type)
        )


def _fitting(error: ExecutionError) -> ExecutionError:
    """Укорачивает текст, пока `error.json` не влезет в ERROR_FILE_LIMIT байт.

    Длинное имя колонки не укоротить без искажения: такая ошибка уходит без `column`.
    """
    if error.column is not None and len(error.column) > COLUMN_LIMIT:
        error = error.model_copy(update={"column": None})
    while len(error.model_dump_json().encode()) > ERROR_FILE_LIMIT:
        error = error.model_copy(update={"message": error.message[: len(error.message) * 3 // 4]})
    return error


def run(name: str, output_port: str, transform: Transform) -> None:
    context = ExecutionContext.model_validate_json(sys.argv[1])
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["E3_S3_ENDPOINT_URL"],
        config=Config(s3={"addressing_style": "path"}),
    )
    spark = session(f"e3-{name}-{context.attempt_id}")
    try:
        result = execute(spark, s3, context, output_port, transform)
    except BlockError as error:
        put_error(s3, context, error)
        raise
    finally:
        spark.stop()
    put(s3, f"{context.output_prefix}/{RESULT_FILE}", result.model_dump_json())


def put_error(s3, context: ExecutionContext, error: BlockError) -> None:
    put(s3, f"{context.output_prefix}/{ERROR_FILE}", error.execution_error.model_dump_json())


def fail_row(error: BlockError) -> SparkColumn:
    """Выражение, которое роняет запись ошибкой `error`; её узнаёт `recognized_errors`."""
    return F.raise_error(F.lit(error.execution_error.model_dump_json()))


@contextmanager
def recognized_errors() -> Iterator[None]:
    """Ошибки Spark, которые блок может объяснить, становятся `BlockError`."""
    try:
        yield
    except Py4JJavaError as error:
        recognized = _recognize(error.java_exception)
        if recognized is None:
            raise
        raise recognized from error


def _recognize(throwable: Any) -> BlockError | None:
    """Ищет по цепочке причин ошибку `fail_row` или переполнение ANSI-режима."""
    while throwable is not None:
        recognized = _raised_by_block(throwable) or _overflow(throwable)
        if recognized is not None:
            return recognized
        throwable = throwable.getCause()
    return None


def _raised_by_block(throwable: Any) -> BlockError | None:
    """В Spark 3.5 текст исключения `raise_error` — ровно его аргумент, без префикса."""
    message = throwable.getMessage()
    if not message:
        return None
    try:
        error = ExecutionError.model_validate_json(message)
    except ValidationError:
        return None
    return BlockError(
        error.message, code=error.code, column=error.column, target_type=error.target_type
    )


def _overflow(throwable: Any) -> BlockError | None:
    """`ARITHMETIC_OVERFLOW` в тексте называет только тип, например `long overflow`."""
    try:
        error_class = throwable.getErrorClass()
    except Py4JError:
        return None
    if error_class != "ARITHMETIC_OVERFLOW":
        return None
    return BlockError(
        f"переполнение: {throwable.getMessageParameters().get('message')}", code=error_class
    )


def session(name: str) -> SparkSession:
    return SparkSession.builder.appName(name).config("spark.sql.ansi.enabled", "true").getOrCreate()


def execute(
    spark: SparkSession, s3, context: ExecutionContext, output_port: str, transform: Transform
) -> ExecutionResult:
    frame = transform(load(spark, s3, context.inputs[INPUT_PORT]), context.parameters)
    output = f"{context.output_prefix}/{output_port}"
    with recognized_errors():
        frame.write.mode("overwrite").parquet(s3a(f"{output}/{DATA_DIR}"))
    files = describe_parquet(s3, output)
    manifest = Manifest(
        format="PARQUET",
        data_schema=schema_of(frame),
        files=files,
        size_bytes=sum(file.size_bytes for file in files),
        row_count=spark.read.parquet(s3a(f"{output}/{DATA_DIR}")).count(),
    )
    put(s3, f"{output}/{MANIFEST_FILE}", manifest.model_dump_json(indent=2))
    return ExecutionResult(
        node_run_id=context.node_run_id,
        attempt_id=context.attempt_id,
        outputs={output_port: f"{output}/{MANIFEST_FILE}"},
        metrics={"rows": float(manifest.row_count or 0)},
    )


def load(spark: SparkSession, s3, source: ResolvedInput) -> DataFrame:
    manifest = Manifest.model_validate_json(get(s3, source.manifest_uri))
    directory = source.manifest_uri.rpartition("/")[0]
    paths = [s3a(f"{directory}/{file.path}") for file in manifest.files]
    if manifest.format == "PARQUET":
        return spark.read.parquet(*paths)
    # Без inferSchema все колонки — строки, как в схеме CSV-версии.
    return spark.read.csv(paths, header=True)


def schema_of(frame: DataFrame) -> DataSchema:
    """Схема для manifest: только типы, которые знает Manifest v1."""
    columns = []
    for field in frame.schema.fields:
        column_type = _COLUMN_TYPES.get(field.dataType)
        if column_type is None:
            raise BlockError(f"колонка {field.name}: тип {field.dataType} не поддерживается")
        columns.append(Column(name=field.name, type=column_type))
    return DataSchema(columns=columns)


def column_types(frame: DataFrame) -> dict[str, str]:
    """Колонка → тип Manifest; неизвестные типы — их имя в Spark."""
    return {
        field.name: _COLUMN_TYPES.get(field.dataType, field.dataType.simpleString())
        for field in frame.schema.fields
    }


def require_column(types: dict[str, str], column: str) -> str:
    if column not in types:
        raise BlockError(f"нет колонки {column}")
    return types[column]


def require_numeric(types: dict[str, str], column: str, operation: str) -> None:
    column_type = require_column(types, column)
    if column_type not in NUMERIC:
        raise BlockError(
            f"{operation} по колонке {column}: нужен long или double, а не {column_type}"
        )


def describe_parquet(s3, output: str) -> list[ManifestFile]:
    """Файлы Parquet с размером и SHA-256 в base64, путь — относительно manifest."""
    bucket, prefix = split(f"{output}/{DATA_DIR}/")
    listed = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    keys = sorted(
        item["Key"]
        for page in listed
        for item in page.get("Contents", [])
        if item["Key"].endswith(".parquet")
    )
    return [describe(s3, bucket, key, output) for key in keys]


def describe(s3, bucket: str, key: str, output: str) -> ManifestFile:
    data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return ManifestFile(
        path=key.removeprefix(split(output)[1] + "/"),
        size_bytes=len(data),
        checksum_sha256=base64.b64encode(hashlib.sha256(data).digest()).decode(),
    )


def split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


def s3a(uri: str) -> str:
    return "s3a://" + uri.removeprefix("s3://")


def get(s3, uri: str) -> bytes:
    bucket, key = split(uri)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def put(s3, uri: str, body: str) -> None:
    bucket, key = split(uri)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode(), ContentType="application/json")
