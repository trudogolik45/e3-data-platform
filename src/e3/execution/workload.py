"""SparkApplication попытки: манифест Kubeflow Spark Operator.

Имя ресурса выводится из `attempt_id`, поэтому повторная `SubmitExecution`
попадает в уже созданный ресурс и второго workload не будет.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

from e3.shared.messages import SubmitExecution

GROUP = "sparkoperator.k8s.io"
VERSION = "v1beta2"
PLURAL = "sparkapplications"

ATTEMPT_LABEL = "e3.io/attempt-id"
NODE_RUN_LABEL = "e3.io/node-run-id"
REPORTED_LABEL = "e3.io/reported"
COMMAND_ANNOTATION = "e3.io/command"

# Точка входа образа блока: получает ExecutionContext JSON-аргументом.
ENTRYPOINT = "local:///opt/e3/main.py"


class Reported(StrEnum):
    """Что Execution Controller уже сообщил о попытке в `events.v1`."""

    STARTED = "started"
    DONE = "done"


@dataclass(frozen=True)
class WorkloadConfig:
    namespace: str
    service_account: str
    spark_version: str
    s3_endpoint_url: str
    # Secret с AWS_ACCESS_KEY_ID и AWS_SECRET_ACCESS_KEY для driver и executor.
    s3_secret: str
    memory: str = "512m"


def workload_name(attempt_id: UUID) -> str:
    return f"e3-{attempt_id}"


def spark_application(command: SubmitExecution, config: WorkloadConfig) -> dict[str, Any]:
    labels = {ATTEMPT_LABEL: str(command.attempt_id), NODE_RUN_LABEL: str(command.node_run_id)}
    pod: dict[str, Any] = {
        "cores": 1,
        "memory": config.memory,
        "labels": labels,
        "envFrom": [{"secretRef": {"name": config.s3_secret}}],
        "env": [{"name": "E3_S3_ENDPOINT_URL", "value": config.s3_endpoint_url}],
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
