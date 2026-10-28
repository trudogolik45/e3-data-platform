import pytest
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from spark_io import recognized_errors, schema_of

from tests.blocks.conftest import block

aggregate = block("aggregate")
BlockError = aggregate.BlockError

SCHEMA = StructType(
    [
        StructField("city", StringType()),
        StructField("qty", LongType()),
        StructField("price", DoubleType()),
    ]
)
MAX_LONG = 2**63 - 1


@pytest.fixture
def sales(spark):
    return spark.createDataFrame([("msk", 5, 10.0), ("msk", 1, None), ("spb", 7, 3.0)], SCHEMA)


def agg(column, function, name=None):
    return {"column": column, "function": function, "as": name or f"{function}_{column}"}


def run(frame, aggregations, group_by=()):
    parameters = {"group_by": list(group_by), "aggregations": aggregations}
    return aggregate.transform(frame, parameters)


@pytest.mark.parametrize(
    ("aggregation", "expected"),
    [
        (agg("qty", "count"), "long"),
        (agg("qty", "sum"), "long"),
        (agg("price", "sum"), "double"),
        (agg("qty", "avg"), "double"),
        (agg("qty", "min"), "long"),
        (agg("price", "max"), "double"),
        (agg("city", "min"), "string"),
    ],
)
def test_result_type_follows_function_and_column(sales, aggregation, expected) -> None:
    result = run(sales, [aggregation])

    [column] = schema_of(result).columns
    assert column.type == expected


def test_groups_by_columns_and_counts_non_empty_values(sales) -> None:
    result = run(sales, [agg("price", "count"), agg("qty", "sum")], group_by=["city"])

    assert sorted(tuple(r) for r in result.collect()) == [("msk", 1, 6), ("spb", 1, 7)]
    assert result.columns == ["city", "count_price", "sum_qty"]


def test_empty_group_by_on_empty_input_gives_one_row(spark) -> None:
    empty = spark.createDataFrame([], SCHEMA)

    result = run(empty, [agg("qty", "count"), agg("qty", "sum")])

    assert [tuple(r) for r in result.collect()] == [(0, None)]


def test_sum_overflow_fails(spark) -> None:
    huge = spark.createDataFrame([("a", MAX_LONG, 0.0), ("a", 1, 0.0)], SCHEMA)

    with pytest.raises(BlockError) as raised, recognized_errors():
        run(huge, [agg("qty", "sum")]).collect()

    assert raised.value.execution_error.code == "ARITHMETIC_OVERFLOW"
    assert raised.value.execution_error.message == "переполнение: long overflow"


@pytest.mark.parametrize("function", ["sum", "avg"])
def test_numeric_function_on_string_fails(sales, function) -> None:
    with pytest.raises(BlockError, match=function):
        run(sales, [agg("city", function)])


def test_repeated_output_name_fails(sales) -> None:
    with pytest.raises(BlockError, match="total"):
        run(sales, [agg("qty", "sum", "total"), agg("price", "sum", "total")])


def test_output_name_equal_to_group_column_fails(sales) -> None:
    with pytest.raises(BlockError, match="city"):
        run(sales, [agg("qty", "count", "city")], group_by=["city"])


def test_missing_column_fails(sales) -> None:
    with pytest.raises(BlockError, match="ghost"):
        run(sales, [agg("ghost", "count")])
