"""Блок `Read`: CSV со входа `source` → Parquet на выходе `data` с manifest v1.

Spark Operator запускает `main.py '<ExecutionContext JSON>'`. Блок пишет только
под `output_prefix`: данные и manifest выхода, затем свой ExecutionResult в
`result.json` — его забирает Execution Controller. Ключи S3 — из окружения
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), адрес — `E3_S3_ENDPOINT_URL`.
"""

import base64
import hashlib
import os
import sys

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession

from e3.shared.contracts import RESULT_FILE, ExecutionContext, ExecutionResult
from e3.shared.manifest import Column, DataSchema, Manifest, ManifestFile

INPUT_PORT = "source"
OUTPUT_PORT = "data"
DATA_DIR = "parquet"
MANIFEST_FILE = "manifest.json"


def main() -> None:
    context = ExecutionContext.model_validate_json(sys.argv[1])
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["E3_S3_ENDPOINT_URL"],
        config=Config(s3={"addressing_style": "path"}),
    )
    spark = SparkSession.builder.appName(f"e3-read-{context.attempt_id}").getOrCreate()
    try:
        result = read(spark, s3, context)
    finally:
        spark.stop()
    put(s3, f"{context.output_prefix}/{RESULT_FILE}", result.model_dump_json())


def read(spark, s3, context: ExecutionContext) -> ExecutionResult:
    source = Manifest.model_validate_json(get(s3, context.inputs[INPUT_PORT].manifest_uri))
    source_dir = context.inputs[INPUT_PORT].manifest_uri.rpartition("/")[0]
    # Без inferSchema все колонки — строки, как в схеме CSV-версии.
    frame = spark.read.csv([s3a(f"{source_dir}/{file.path}") for file in source.files], header=True)
    output = f"{context.output_prefix}/{OUTPUT_PORT}"
    frame.write.mode("overwrite").parquet(s3a(f"{output}/{DATA_DIR}"))
    files = describe_parquet(s3, output)
    columns = [Column(name=name, type="string") for name in frame.columns]
    manifest = Manifest(
        format="PARQUET",
        data_schema=DataSchema(columns=columns),
        files=files,
        size_bytes=sum(file.size_bytes for file in files),
        row_count=spark.read.parquet(s3a(f"{output}/{DATA_DIR}")).count(),
    )
    put(s3, f"{output}/{MANIFEST_FILE}", manifest.model_dump_json(indent=2))
    return ExecutionResult(
        node_run_id=context.node_run_id,
        attempt_id=context.attempt_id,
        outputs={OUTPUT_PORT: f"{output}/{MANIFEST_FILE}"},
        metrics={"rows": float(manifest.row_count or 0)},
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


if __name__ == "__main__":
    main()
