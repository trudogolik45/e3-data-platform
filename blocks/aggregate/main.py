"""Блок `Aggregate`: группировка входа `source` по `group_by` и агрегаты → выход `result`.

`count` считает непустые значения и даёт `long`; `sum` — `long` для `long`
(переполнение роняет блок) и `double` для `double`; `avg` — `double`;
`min`/`max` — тип колонки. `sum`/`avg` по строкам запрещены. Пустой `group_by` —
одна строка на весь вход, даже пустой.
"""

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from spark_io import BlockError, Parameters, column_types, require_column, run

OUTPUT_PORT = "result"
NUMERIC = frozenset({"long", "double"})
NUMERIC_ONLY = frozenset({"sum", "avg"})
FUNCTIONS = {"count": F.count, "sum": F.sum, "min": F.min, "max": F.max, "avg": F.avg}


def transform(frame: DataFrame, parameters: Parameters) -> DataFrame:
    types = column_types(frame)
    group_by = parameters.get("group_by", [])
    for column in group_by:
        require_column(types, column)
    aggregations = parameters["aggregations"]
    _check_names(group_by, [aggregation["as"] for aggregation in aggregations])
    return frame.groupBy(*group_by).agg(*[_aggregate(a, types) for a in aggregations])


def _check_names(group_by: list[str], names: list[str]) -> None:
    seen = set(group_by)
    for name in names:
        if name in seen:
            raise BlockError(f"колонка {name} на выходе повторяется")
        seen.add(name)


def _aggregate(aggregation: dict[str, Any], types: dict[str, str]) -> Column:
    column, function = aggregation["column"], aggregation["function"]
    column_type = require_column(types, column)
    if function in NUMERIC_ONLY and column_type not in NUMERIC:
        raise BlockError(
            f"{function} по колонке {column}: нужен long или double, а не {column_type}"
        )
    return FUNCTIONS[function](F.col(column)).alias(aggregation["as"])


if __name__ == "__main__":
    run("aggregate", OUTPUT_PORT, transform)
