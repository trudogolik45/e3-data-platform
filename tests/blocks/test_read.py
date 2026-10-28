import pytest
from pyspark.sql import Row
from spark_io import recognized_errors, schema_of

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


@pytest.mark.parametrize("target_type", ["long", "double"])
def test_value_that_does_not_cast_fails_with_column_and_type_but_not_value(
    spark, target_type
) -> None:
    result = read.transform(rows(spark, ("1", "hello")), {"types": {"amount": target_type}})

    with pytest.raises(BlockError) as raised, recognized_errors():
        result.collect()

    error = raised.value.execution_error
    assert (error.code, error.column, error.target_type) == (
        "CAST_INVALID_INPUT",
        "amount",
        target_type,
    )
    assert error.message == f"колонка amount: значения не приводятся к {target_type}"
    assert "hello" not in error.model_dump_json()


def test_value_too_large_for_long_fails_with_column_and_type(spark) -> None:
    result = read.transform(
        rows(spark, ("1", "99999999999999999999")), {"types": {"amount": "long"}}
    )

    with pytest.raises(BlockError) as raised, recognized_errors():
        result.collect()

    assert (raised.value.execution_error.column, raised.value.execution_error.target_type) == (
        "amount",
        "long",
    )


def test_null_stays_null_when_cast(spark) -> None:
    result = read.transform(rows(spark, ("1", "5"), ("2", None)), {"types": {"amount": "long"}})

    assert [tuple(r) for r in result.collect()] == [("1", 5), ("2", None)]


def test_types_for_missing_column_fail(spark) -> None:
    with pytest.raises(BlockError, match="price"):
        read.transform(rows(spark, ("1", "10")), {"types": {"price": "long"}})
