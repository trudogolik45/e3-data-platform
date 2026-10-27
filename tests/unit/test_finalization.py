from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest

from e3.runs.finalization import OutputRejected, verify_outputs
from e3.shared.contracts import ExecutionResult
from e3.shared.storage import ObjectStorage

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

OWN = uuid4()
OTHER = uuid4()
# Отказ по manifest_uri — до чтения хранилища: клиент S3 не нужен.
STORAGE = ObjectStorage(cast("S3Client", None), cast("S3Client", None), "e3")


@pytest.mark.parametrize(
    "key",
    [
        f"staging/executions/{OWN}/../{OTHER}/manifest.json",
        f"staging/executions/{OWN}/data/../../{OTHER}/manifest.json",
        f"staging/executions/{OWN}//manifest.json",
        f"staging/executions/{OTHER}/manifest.json",
        f"datasets/{OTHER}/manifest.json",
    ],
)
async def test_manifest_outside_attempt_directory_is_rejected(key: str) -> None:
    result = ExecutionResult(
        node_run_id=uuid4(), attempt_id=OWN, outputs={"data": f"s3://e3/{key}"}
    )

    with pytest.raises(OutputRejected, match="data: manifest вне каталога попытки"):
        await verify_outputs(STORAGE, result, ["data"])
