import base64
import hashlib

import pytest

from e3.catalog.inspection import (
    HEADER_READ_LIMIT,
    MAX_UPLOAD_BYTES,
    Column,
    StagedObject,
    UploadRejected,
    inspect_staged_object,
)


def sha256_b64(data: bytes) -> str:
    return base64.b64encode(hashlib.sha256(data).digest()).decode()


def staged(data: bytes, *, size: int | None = None, checksum: str | None = None) -> StagedObject:
    return StagedObject(
        size_bytes=len(data) if size is None else size,
        checksum_sha256=sha256_b64(data) if checksum is None else checksum,
        head=data[:HEADER_READ_LIMIT],
    )


def inspect(data: bytes, **kwargs: object) -> list[Column]:
    obj = staged(data, **kwargs)  # type: ignore[arg-type]
    return inspect_staged_object(obj, declared_checksum_sha256=sha256_b64(data))


def test_header_columns_become_string_schema() -> None:
    columns = inspect(b"id,name,amount\n1,a,10\n")

    assert columns == [
        Column(name="id", type="string"),
        Column(name="name", type="string"),
        Column(name="amount", type="string"),
    ]


def test_header_without_rows_and_without_newline_is_accepted() -> None:
    assert inspect(b"id,name") == [
        Column(name="id", type="string"),
        Column(name="name", type="string"),
    ]


def test_crlf_bom_and_quoted_names_are_understood() -> None:
    columns = inspect('﻿"first, name",city\r\nx,y\r\n'.encode())

    assert columns == [
        Column(name="first, name", type="string"),
        Column(name="city", type="string"),
    ]


def test_object_over_5_gib_is_rejected() -> None:
    with pytest.raises(UploadRejected, match="5 ГБ"):
        inspect(b"id\n1\n", size=MAX_UPLOAD_BYTES + 1)


@pytest.mark.parametrize("actual", [sha256_b64(b"other"), None])
def test_checksum_mismatch_is_rejected(actual: str | None) -> None:
    obj = StagedObject(size_bytes=5, checksum_sha256=actual, head=b"id\n1\n")

    with pytest.raises(UploadRejected, match="контрольная сумма"):
        inspect_staged_object(obj, declared_checksum_sha256=sha256_b64(b"id\n1\n"))


@pytest.mark.parametrize("data", [b"", b"\n1,2\n", b"  \r\n"])
def test_missing_header_is_rejected(data: bytes) -> None:
    with pytest.raises(UploadRejected, match="нет заголовка"):
        inspect(data)


@pytest.mark.parametrize("data", [b"id,,name\n", b"id, \n", b'id,""\n'])
def test_empty_column_name_is_rejected(data: bytes) -> None:
    with pytest.raises(UploadRejected, match="пустое имя колонки"):
        inspect(data)


def test_duplicate_column_names_are_rejected() -> None:
    with pytest.raises(UploadRejected, match="повторяются имена колонок: id"):
        inspect(b"id,name,id\n")


def test_non_utf8_header_is_rejected() -> None:
    with pytest.raises(UploadRejected, match="UTF-8"):
        inspect("имя,город\n".encode("cp1251"))


def test_header_longer_than_read_limit_is_rejected() -> None:
    data = b"a" * (HEADER_READ_LIMIT + 10) + b"\n1\n"

    with pytest.raises(UploadRejected, match="заголовок длиннее"):
        inspect(data)


def test_non_utf8_rows_after_header_are_not_inspected() -> None:
    assert inspect(b"id\n" + "строка".encode("cp1251")) == [Column(name="id", type="string")]
