import pytest
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from tests.blocks.conftest import block

filter_block = block("filter")
BlockError = filter_block.BlockError

SCHEMA = StructType(
    [
        StructField("city", StringType()),
        StructField("qty", LongType()),
        StructField("price", DoubleType()),
    ]
)


@pytest.fixture
def sales(spark):
    return spark.createDataFrame(
        [("msk", 5, 10.0), ("msk", 1, 99.5), ("spb", 7, 3.0), (None, 9, None)], SCHEMA
    )


def where(sales, *conditions):
    result = filter_block.transform(sales, {"conditions": list(conditions)})
    return sorted((tuple(r) for r in result.collect()), key=repr)


def cond(column, op, value):
    return {"column": column, "op": op, "value": value}


def test_conditions_are_combined_with_and(sales) -> None:
    assert where(sales, cond("city", "eq", "msk"), cond("qty", "gt", 2)) == [("msk", 5, 10.0)]


@pytest.mark.parametrize(
    ("op", "value", "expected"),
    [
        ("eq", 5, [5]),
        ("ne", 5, [1, 7, 9]),
        ("gt", 5, [7, 9]),
        ("gte", 5, [5, 7, 9]),
        ("lt", 5, [1]),
        ("lte", 5, [1, 5]),
    ],
)
def test_each_operator_compares_numbers(sales, op, value, expected) -> None:
    assert sorted(qty for _, qty, _ in where(sales, cond("qty", op, value))) == expected


def test_null_passes_no_condition(sales) -> None:
    assert len(where(sales, cond("city", "ne", "msk"))) == 1
    assert len(where(sales, cond("price", "lt", 1000))) == 3


def test_double_column_accepts_integer_value(sales) -> None:
    assert where(sales, cond("price", "gte", 10)) == [("msk", 1, 99.5), ("msk", 5, 10.0)]


def test_ordering_on_string_fails(sales) -> None:
    with pytest.raises(BlockError, match="gt"):
        where(sales, cond("city", "gt", "a"))


@pytest.mark.parametrize(
    ("column", "value"), [("qty", "5"), ("qty", 2.5), ("city", 1), ("qty", True)]
)
def test_value_of_another_type_fails(sales, column, value) -> None:
    with pytest.raises(BlockError, match=column):
        where(sales, cond(column, "eq", value))


def test_missing_column_fails(sales) -> None:
    with pytest.raises(BlockError, match="ghost"):
        where(sales, cond("ghost", "eq", 1))
