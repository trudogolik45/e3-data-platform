"""Manifest v1 — общая опись данных DatasetVersion и NodeOutput."""

import json
from typing import Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

DataFormat = Literal["CSV", "PARQUET"]


class Column(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    type: Literal["string"]


class DataSchema(BaseModel):
    model_config = ConfigDict(frozen=True)

    columns: list[Column]


class ManifestFile(BaseModel):
    model_config = ConfigDict(frozen=True)

    path: str = Field(description="Путь относительно manifest.json")
    size_bytes: int = Field(ge=0)
    checksum_sha256: str = Field(description="SHA-256 в base64, как в S3 checksum")


class Manifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", serialize_by_alias=True)

    manifest_version: Literal[1] = 1
    format: DataFormat
    data_schema: DataSchema = Field(
        validation_alias=AliasChoices("schema", "data_schema"), serialization_alias="schema"
    )
    files: list[ManifestFile] = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    row_count: int | None = Field(default=None, description="None, если строки не считались")


def print_json_schema() -> None:
    print(json.dumps(Manifest.model_json_schema(), ensure_ascii=False, indent=2))
