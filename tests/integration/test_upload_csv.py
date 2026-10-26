from uuid import uuid4

import pytest
from mypy_boto3_s3 import S3Client

from n3.settings import Settings
from n3.shared.manifest import Manifest
from tests.integration.conftest import Api, object_exists, sha256_b64

pytestmark = pytest.mark.integration

CSV = b"id,name,amount\n1,apple,10\n2,pear,20\n"


async def test_uploaded_csv_becomes_numbered_dataset_versions(api: Api) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")

    first = await api.upload_csv(dataset_id, CSV)
    second = await api.upload_csv(dataset_id, b"id\n3\n")

    assert first.status_code == 200, first.text
    assert first.json()["state"] == "COMPLETED"
    assert first.json()["version"]["version_number"] == 1
    assert second.json()["version"]["version_number"] == 2
    versions = await api.versions(dataset_id)
    assert [(v["version_number"], v["format"]) for v in versions] == [(1, "CSV"), (2, "CSV")]
    assert versions[0]["schema"] == {
        "columns": [
            {"name": "id", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "amount", "type": "string"},
        ]
    }
    assert versions[0]["version_id"] == first.json()["version"]["version_id"]
    assert versions[0]["committed_at"]


async def test_manifest_v1_lies_next_to_data_and_staging_is_removed(
    api: Api, s3: S3Client, settings: Settings
) -> None:
    dataset_id = await api.new_dataset(f"sales-{uuid4()}")
    upload = await api.start_upload(dataset_id, CSV)
    await api.put_file(upload, CSV)

    response = await api.complete(dataset_id, upload.upload_id)

    version_id = response.json()["version"]["version_id"]
    prefix = f"datasets/{dataset_id}/{version_id}"
    raw = s3.get_object(Bucket=settings.s3_bucket, Key=f"{prefix}/manifest.json")["Body"].read()
    manifest = Manifest.model_validate_json(raw)
    assert manifest.format == "CSV"
    assert manifest.size_bytes == len(CSV)
    assert [(f.path, f.size_bytes, f.checksum_sha256) for f in manifest.files] == [
        ("data/source.csv", len(CSV), sha256_b64(CSV))
    ]
    assert [c.name for c in manifest.data_schema.columns] == ["id", "name", "amount"]
    data = s3.get_object(Bucket=settings.s3_bucket, Key=f"{prefix}/data/source.csv")["Body"]
    assert data.read() == CSV
    assert not object_exists(s3, settings, f"staging/uploads/{upload.upload_id}/source.csv")
