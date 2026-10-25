import pytest
from pyspark.sql import Row
from spark_io import schema_of

from tests.blocks.conftest import block

read = block("read")
BlockError = read.BlockError


def rows(spark, *values):
    return spark.createDataFrame([Row(id=i, amount=a) for i, a in values])


def test_types_cast_columns_and_manifest_carries_them(spark) -> None:
    frame = rows(spark, ("1", "10"), ("2", "2.5"))

    result = read.transform(frame, {"types": {"id": "long", "amount": "double"}})

    assert [(c.name, c.type) for c in schema_of(result).columns] == [
        ("id", "long"),
        ("amount", "double"),
    ]
    assert [tuple(r) for r in result.collect()] == [(1, 10.0), (2, 2.5)]


def test_columns_without_types_stay_strings(spark) -> None:
    result = read.transform(rows(spark, ("1", "10")), {})

    assert [c.type for c in schema_of(result).columns] == ["string", "string"]


def test_value_that_does_not_cast_fails_instead_of_null(spark) -> None:
    result = read.transform(rows(spark, ("1", "hello")), {"types": {"amount": "long"}})

    with pytest.raises(Exception, match="hello"):
        result.collect()


def test_types_for_missing_column_fail(spark) -> None:
    with pytest.raises(BlockError, match="price"):
        read.transform(rows(spark, ("1", "10")), {"types": {"price": "long"}})
