"""Проверка загруженного CSV по размеру, контрольной сумме и заголовку.

Строки целиком не читаются: полную валидность CSV проверяет блок `Read`.
"""

import csv
from collections import Counter
from dataclasses import dataclass

from e3.shared.manifest import Column

MAX_UPLOAD_BYTES = 5 * 1024**3
HEADER_READ_LIMIT = 64 * 1024


class UploadRejected(Exception):
    """Upload не прошёл проверку и уходит в REJECTED."""


@dataclass(frozen=True)
class StagedObject:
    size_bytes: int
    checksum_sha256: str | None
    head: bytes


def inspect_staged_object(obj: StagedObject, *, declared_checksum_sha256: str) -> list[Column]:
    if obj.size_bytes > MAX_UPLOAD_BYTES:
        raise UploadRejected("файл больше 5 ГБ")
    if obj.checksum_sha256 != declared_checksum_sha256:
        raise UploadRejected("контрольная сумма файла не совпадает с заявленной")
    names = _parse_header(_header_line(obj))
    return [Column(name=name, type="string") for name in names]


def _header_line(obj: StagedObject) -> bytes:
    line, newline, _ = obj.head.partition(b"\n")
    if not newline and obj.size_bytes > len(obj.head):
        raise UploadRejected(f"заголовок длиннее {HEADER_READ_LIMIT} байт")
    return line


def _parse_header(line: bytes) -> list[str]:
    try:
        text = line.decode("utf-8-sig").rstrip("\r")
    except UnicodeDecodeError:
        raise UploadRejected("заголовок не в кодировке UTF-8") from None
    if not text.strip():
        raise UploadRejected("нет заголовка")
    names = next(csv.reader([text]))
    if any(not name.strip() for name in names):
        raise UploadRejected("пустое имя колонки")
    duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
    if duplicates:
        raise UploadRejected(f"повторяются имена колонок: {', '.join(duplicates)}")
    return names
