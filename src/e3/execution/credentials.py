"""Attempt Credentials: краткоживущие S3-ключи попытки от MinIO STS (ADR-0006).

Права выводятся из `ExecutionContext` уже авторизованной `SubmitExecution`:
чтение — папки manifest'ов входов (файлы лежат рядом, пути в `Manifest.files`
относительны manifest'у), чтение и запись — только `output_prefix`. Префикс
разрешён и сам по себе, и со всем под ним: S3A проверяет ключ папки без `/`.
"""

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from e3.shared.contracts import ExecutionContext

if TYPE_CHECKING:
    from mypy_boto3_sts import STSClient

# MinIO не смотрит на роль, но boto3 требует ARN; значение из документации MinIO.
ROLE_ARN = "arn:xxx:xxx:xxx:xxxx"

# Те же действия разрешены пользователю контроллера в
# `scripts/minio/execution-controller-policy.json`: права сессии — пересечение.
WRITE_ACTIONS = [
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:AbortMultipartUpload",
    "s3:ListMultipartUploadParts",
]


@dataclass(frozen=True)
class S3Keys:
    access_key_id: str
    secret_access_key: str
    session_token: str
    expires_at: datetime

    def env(self) -> dict[str, str]:
        """Окружение driver и executor: его читают boto3 и S3A."""
        return {
            "AWS_ACCESS_KEY_ID": self.access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.secret_access_key,
            "AWS_SESSION_TOKEN": self.session_token,
        }


class StsCredentials:
    """`AssumeRole` от имени пользователя Execution Controller, без продления."""

    def __init__(self, sts: "STSClient", duration_seconds: int) -> None:
        self._sts = sts
        self._duration_seconds = duration_seconds

    async def issue(self, context: ExecutionContext) -> S3Keys:
        response = await asyncio.to_thread(
            self._sts.assume_role,
            RoleArn=ROLE_ARN,
            RoleSessionName=str(context.attempt_id),
            Policy=json.dumps(session_policy(context)),
            DurationSeconds=self._duration_seconds,
        )
        keys = response["Credentials"]
        return S3Keys(
            access_key_id=keys["AccessKeyId"],
            secret_access_key=keys["SecretAccessKey"],
            session_token=keys["SessionToken"],
            expires_at=keys["Expiration"],
        )


def session_policy(context: ExecutionContext) -> dict[str, Any]:
    """Inline session policy `AssumeRole`: права сессии — пересечение с правами её пользователя."""
    output = _split(context.output_prefix)
    inputs = sorted({_split(i.manifest_uri.rpartition("/")[0]) for i in context.inputs.values()})
    readable = [*inputs, output]
    return {
        "Version": "2012-10-17",
        "Statement": [
            _allow(["s3:GetObject"], _objects(readable)),
            _allow(WRITE_ACTIONS, _objects([output])),
            *(_list(bucket, readable) for bucket in sorted({bucket for bucket, _ in readable})),
        ],
    }


def _allow(actions: list[str], resources: list[str]) -> dict[str, Any]:
    return {"Effect": "Allow", "Action": actions, "Resource": resources}


def _list(bucket: str, prefixes: list[tuple[str, str]]) -> dict[str, Any]:
    keys = [key for own, key in prefixes if own == bucket]
    condition = {"StringLike": {"s3:prefix": _subtree(keys)}}
    return _allow(["s3:ListBucket"], [f"arn:aws:s3:::{bucket}"]) | {"Condition": condition}


def _objects(prefixes: list[tuple[str, str]]) -> list[str]:
    return [
        f"arn:aws:s3:::{bucket}/{key}" for bucket, prefix in prefixes for key in _subtree([prefix])
    ]


def _subtree(prefixes: list[str]) -> list[str]:
    return [key for prefix in prefixes for key in (prefix, f"{prefix}/*")]


def _split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key
