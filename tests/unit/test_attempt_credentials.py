from uuid import uuid4

from e3.execution.credentials import session_policy
from e3.shared.contracts import (
    DataRef,
    DatasetVersionRef,
    ExecutionContext,
    NodeOutputRef,
    ResolvedInput,
)
from e3.shared.manifest import Column, DataSchema

SCHEMA = DataSchema(columns=[Column(name="city", type="string")])


def resolved(manifest_uri: str, source: DataRef) -> ResolvedInput:
    return ResolvedInput(
        manifest_uri=manifest_uri, format="PARQUET", data_schema=SCHEMA, source=source
    )


def test_session_policy_reads_only_input_manifest_folders_and_writes_only_output_prefix() -> None:
    attempt_id = uuid4()
    context = ExecutionContext(
        node_run_id=uuid4(),
        attempt_id=attempt_id,
        inputs={
            "left": resolved(
                "s3://e3-lake/datasets/d1/v1/manifest.json",
                DatasetVersionRef(dataset_id=uuid4(), version_id=uuid4()),
            ),
            "right": resolved(
                "s3://e3-lake/staging/executions/a0/data/manifest.json",
                NodeOutputRef(node_run_id=uuid4(), port="data"),
            ),
        },
        parameters={},
        output_prefix=f"s3://e3-lake/staging/executions/{attempt_id}",
    )
    output = f"staging/executions/{attempt_id}"

    policy = session_policy(context)

    assert policy == {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": [
                    "arn:aws:s3:::e3-lake/datasets/d1/v1",
                    "arn:aws:s3:::e3-lake/datasets/d1/v1/*",
                    "arn:aws:s3:::e3-lake/staging/executions/a0/data",
                    "arn:aws:s3:::e3-lake/staging/executions/a0/data/*",
                    f"arn:aws:s3:::e3-lake/{output}",
                    f"arn:aws:s3:::e3-lake/{output}/*",
                ],
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:AbortMultipartUpload",
                    "s3:ListMultipartUploadParts",
                ],
                "Resource": [f"arn:aws:s3:::e3-lake/{output}", f"arn:aws:s3:::e3-lake/{output}/*"],
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": ["arn:aws:s3:::e3-lake"],
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [
                            "datasets/d1/v1",
                            "datasets/d1/v1/*",
                            "staging/executions/a0/data",
                            "staging/executions/a0/data/*",
                            output,
                            f"{output}/*",
                        ]
                    }
                },
            },
        ],
    }
