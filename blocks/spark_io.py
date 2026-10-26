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
"""

import base64
import hashlib
import os
import sys
from collections.abc import Callable
from typing import Any

import boto3
from botocore.config import Config
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType

from e3.shared.contracts import RESULT_FILE, ExecutionContext, ExecutionResult, ResolvedInput
from e3.shared.manifest import Column, DataSchema, Manifest, ManifestFile

INPUT_PORT = "source"
DATA_DIR = "parquet"
MANIFEST_FILE = "manifest.json"
NUMERIC = frozenset({"long", "double"})

Parameters = dict[str, Any]
Transform = Callable[[DataFrame, Parameters], DataFrame]

_COLUMN_TYPES = {StringType(): "string", LongType(): "long", DoubleType(): "double"}


class BlockError(Exception):
    """Параметры не подходят к данным: текст — причина падения блока."""


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
    finally:
        spark.stop()
    put(s3, f"{context.output_prefix}/{RESULT_FILE}", result.model_dump_json())


def session(name: str) -> SparkSession:
    return SparkSession.builder.appName(name).config("spark.sql.ansi.enabled", "true").getOrCreate()


def execute(
    spark: SparkSession, s3, context: ExecutionContext, output_port: str, transform: Transform
) -> ExecutionResult:
    frame = transform(load(spark, s3, context.inputs[INPUT_PORT]), context.parameters)
    output = f"{context.output_prefix}/{output_port}"
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
