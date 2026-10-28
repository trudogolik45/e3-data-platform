"""SparkApplication попытки: манифест Kubeflow Spark Operator.

Имя ресурса выводится из `attempt_id`, поэтому повторная `SubmitExecution`
попадает в уже созданный ресурс и второго workload не будет. Attempt Fence —
ресурс с тем же именем и `spec.suspend: true`: Spark Operator его не запускает,
а опоздавшая `SubmitExecution` упирается в занятое имя. Метка отмены на заборе
значит, что попытку остановили по `CancelExecution`, а не потеряли.

Attempt Credentials лежат в Secret попытки (ADR-0006): его владелец —
`SparkApplication`, и Kubernetes удаляет Secret вместе с ней. Забор ключей
не получает.
"""

import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

from e3.execution.credentials import S3Keys
from e3.shared.messages import (
    CancelExecution,
    PodSize,
    ReconcileExecution,
    SubmitExecution,
    original_submission,
)

GROUP = "sparkoperator.k8s.io"
VERSION = "v1beta2"
PLURAL = "sparkapplications"

ATTEMPT_LABEL = "e3.io/attempt-id"
NODE_RUN_LABEL = "e3.io/node-run-id"
REPORTED_LABEL = "e3.io/reported"
FENCED_LABEL = "e3.io/fenced"
CANCELLED_LABEL = "e3.io/cancelled"
# `trace_id` PipelineRun: по нему находятся ресурс попытки, её pod'ы и их логи.
TRACE_LABEL = "e3.io/trace-id"
COMMAND_ANNOTATION = "e3.io/command"

# Точка входа образа блока: получает ExecutionContext JSON-аргументом.
ENTRYPOINT = "local:///opt/e3/main.py"


class Reported(StrEnum):
    """Что Execution Controller уже сообщил о попытке в `events.v1`."""

    STARTED = "started"
    DONE = "done"


@dataclass(frozen=True)
class Pod:
    """Pod driver или executor попытки глазами Kubernetes."""

    phase: str
    # Есть `deletionTimestamp`: pod ещё может работать.
    terminating: bool = False


@dataclass(frozen=True)
class WorkloadConfig:
    namespace: str
    service_account: str
    spark_version: str
    s3_endpoint_url: str


def workload_name(attempt_id: UUID) -> str:
    return f"e3-{attempt_id}"


def credentials_secret_name(attempt_id: UUID) -> str:
    return f"{workload_name(attempt_id)}-s3"


def credentials_secret(application: dict[str, Any], keys: S3Keys) -> dict[str, Any]:
    """Secret с Attempt Credentials; уходит вместе с `application` (ownerReference)."""
    metadata = application["metadata"]
    attempt_id = UUID(metadata["labels"][ATTEMPT_LABEL])
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": credentials_secret_name(attempt_id),
            "namespace": metadata["namespace"],
            "labels": {ATTEMPT_LABEL: str(attempt_id)},
            "ownerReferences": [
                {
                    "apiVersion": application["apiVersion"],
                    "kind": application["kind"],
                    "name": metadata["name"],
                    "uid": metadata["uid"],
                    "controller": True,
                    "blockOwnerDeletion": True,
                }
            ],
        },
        "type": "Opaque",
        "stringData": keys.env(),
    }


def is_fence(application: dict[str, Any]) -> bool:
    return application["metadata"]["labels"].get(FENCED_LABEL) == "true"


def is_cancelled(application: dict[str, Any]) -> bool:
    return application["metadata"]["labels"].get(CANCELLED_LABEL) == "true"


def attempt_fence(
    command: ReconcileExecution | CancelExecution, config: WorkloadConfig
) -> dict[str, Any]:
    """Занимает имя попытки ресурсом, который Spark Operator не запускает."""
    fence = spark_application(original_submission(command), config)
    fence["metadata"]["labels"][FENCED_LABEL] = "true"
    fence["spec"]["suspend"] = True
    if isinstance(command, CancelExecution):
        fence["metadata"]["labels"][CANCELLED_LABEL] = "true"
    return fence


def spark_application(command: SubmitExecution, config: WorkloadConfig) -> dict[str, Any]:
    labels = {
        ATTEMPT_LABEL: str(command.attempt_id),
        NODE_RUN_LABEL: str(command.node_run_id),
        TRACE_LABEL: command.trace_id,
    }
    pod = _pod_size(command.payload.pod) | {
        "labels": labels,
        "envFrom": [{"secretRef": {"name": credentials_secret_name(command.attempt_id)}}],
        "env": [
            {"name": "E3_S3_ENDPOINT_URL", "value": config.s3_endpoint_url},
            {"name": "E3_TRACE_ID", "value": command.trace_id},
        ],
    }
    return {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "SparkApplication",
        "metadata": {
            "name": workload_name(command.attempt_id),
            "namespace": config.namespace,
            "labels": dict(labels),
            "annotations": {COMMAND_ANNOTATION: command.model_dump_json()},
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": command.payload.executable_ref,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": ENTRYPOINT,
            "arguments": [command.payload.context.model_dump_json()],
            "sparkVersion": config.spark_version,
            # Ретраи решает только Run Orchestrator (ADR-0003).
            "restartPolicy": {"type": "Never"},
            "sparkConf": _s3a_conf(config.s3_endpoint_url),
            "driver": pod | {"serviceAccount": config.service_account},
            "executor": pod | {"instances": 1},
        },
    }


def _pod_size(size: PodSize) -> dict[str, Any]:
    """`coreRequest` — заявка в Kubernetes; `cores` — потоки Spark, целые, не меньше заявки."""
    return {
        "cores": math.ceil(size.cpu_millicores / 1000),
        "coreRequest": f"{size.cpu_millicores}m",
        "memory": f"{size.memory_mib}m",
    }


def _s3a_conf(endpoint_url: str) -> dict[str, str]:
    return {
        "spark.hadoop.fs.s3a.endpoint": endpoint_url,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint_url.startswith("https")).lower(),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        ),
    }
