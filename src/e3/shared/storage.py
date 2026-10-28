"""Объектное хранилище S3/MinIO для Control Plane.

Вызовы boto3 синхронные, поэтому уходят в поток. Временные сбои хранилища
превращаются в `StorageUnavailable`: операцию можно безопасно повторить.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from botocore.exceptions import BotoCoreError, ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}


class StorageUnavailable(Exception):
    """Хранилище временно недоступно, операцию можно повторить."""


class ObjectNotFound(Exception):
    """Объекта нет в хранилище."""


@dataclass(frozen=True)
class ObjectInfo:
    size_bytes: int
    checksum_sha256: str | None


@dataclass(frozen=True)
class PresignedPut:
    url: str
    headers: dict[str, str]


class ObjectStorage:
    def __init__(self, client: "S3Client", presign_client: "S3Client", bucket: str) -> None:
        self._client = client
        self._presign_client = presign_client
        self._bucket = bucket

    def uri(self, key: str) -> str:
        return f"s3://{self._bucket}/{key}"

    def key(self, uri: str) -> str:
        """Обратное к `uri`: ключ объекта в своём bucket, чужие URI — `ValueError`."""
        prefix = self.uri("")
        if not uri.startswith(prefix):
            raise ValueError(f"{uri} не в bucket {self._bucket}")
        return uri.removeprefix(prefix)

    def presign_put(self, key: str, *, checksum_sha256: str, ttl_seconds: int) -> PresignedPut:
        url = self._presign_client.generate_presigned_url(
            "put_object",
            Params={"Bucket": self._bucket, "Key": key, "ChecksumSHA256": checksum_sha256},
            ExpiresIn=ttl_seconds,
        )
        return PresignedPut(url=url, headers={"x-amz-checksum-sha256": checksum_sha256})

    async def head(self, key: str) -> ObjectInfo | None:
        try:
            response = await self._call(
                lambda: self._client.head_object(
                    Bucket=self._bucket, Key=key, ChecksumMode="ENABLED"
                )
            )
        except ObjectNotFound:
            return None
        return ObjectInfo(response["ContentLength"], response.get("ChecksumSHA256"))

    async def read_prefix(self, key: str, limit: int) -> bytes:
        response = await self._call(
            lambda: self._client.get_object(
                Bucket=self._bucket, Key=key, Range=f"bytes=0-{limit - 1}"
            )
        )
        return await asyncio.to_thread(response["Body"].read)

    async def read(self, key: str) -> bytes:
        response = await self._call(lambda: self._client.get_object(Bucket=self._bucket, Key=key))
        return await asyncio.to_thread(response["Body"].read)

    async def copy(self, source_key: str, target_key: str) -> None:
        await self._call(
            lambda: self._client.copy_object(
                Bucket=self._bucket,
                Key=target_key,
                CopySource={"Bucket": self._bucket, "Key": source_key},
                ChecksumAlgorithm="SHA256",
            )
        )

    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        await self._call(
            lambda: self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
                ChecksumAlgorithm="SHA256",
            )
        )

    async def delete(self, key: str) -> None:
        await self._call(lambda: self._client.delete_object(Bucket=self._bucket, Key=key))

    async def delete_prefix(self, prefix: str) -> int:
        """Удаляет все объекты под `prefix`; возвращает их число. Пустой префикс — 0."""
        deleted = 0
        while keys := await self._list(prefix):
            await self._delete_many(keys)
            deleted += len(keys)
        return deleted

    async def _list(self, prefix: str) -> list[str]:
        """Первая страница ключей под `prefix`: до 1000, как и в одном DeleteObjects."""
        response = await self._call(
            lambda: self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
        )
        return [item["Key"] for item in response.get("Contents", []) if "Key" in item]

    async def _delete_many(self, keys: list[str]) -> None:
        response = await self._call(
            lambda: self._client.delete_objects(
                Bucket=self._bucket,
                Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
            )
        )
        if errors := response.get("Errors"):
            raise StorageUnavailable(f"не удалено {len(errors)} объектов: {errors[0]}")

    async def _call[T](self, operation: Callable[[], T]) -> T:
        try:
            return await asyncio.to_thread(operation)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code in NOT_FOUND_CODES:
                raise ObjectNotFound(code) from error
            raise StorageUnavailable(code) from error
        except BotoCoreError as error:
            raise StorageUnavailable(str(error)) from error
