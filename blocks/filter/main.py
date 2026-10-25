"""Блок `Filter`: строки входа `source`, прошедшие все условия, → выход `result`.

Условия `{column, op, value}` объединяются через AND. `eq`/`ne` сравнивают
значения любого типа, `gt`/`gte`/`lt`/`lte` — только числа. Тип `value`
совпадает с типом колонки. `null` не проходит ни одно условие, как в SQL.
"""

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from spark_io import BlockError, Parameters, column_types, require_column, run

OUTPUT_PORT = "result"
NUMERIC = frozenset({"long", "double"})
ORDERING = frozenset({"gt", "gte", "lt", "lte"})


def transform(frame: DataFrame, parameters: Parameters) -> DataFrame:
    types = column_types(frame)
    predicate = F.lit(True)
    for condition in parameters["conditions"]:
        predicate = predicate & _predicate(condition, types)
    # Сравнение с `null` даёт `null`, и `where` такую строку отбрасывает.
    return frame.where(predicate)


def _predicate(condition: dict[str, Any], types: dict[str, str]) -> Column:
    column, op, value = condition["column"], condition["op"], condition["value"]
    column_type = require_column(types, column)
    if op in ORDERING and column_type not in NUMERIC:
        raise BlockError(f"{op} по колонке {column}: нужен long или double, а не {column_type}")
    if not _matches(value, column_type):
        raise BlockError(f"{column}: значение {value!r} не подходит к типу {column_type}")
    target, literal = F.col(column), F.lit(value)
    return {
        "eq": target == literal,
        "ne": target != literal,
        "gt": target > literal,
        "gte": target >= literal,
        "lt": target < literal,
        "lte": target <= literal,
    }[op]


def _matches(value: Any, column_type: str) -> bool:
    if isinstance(value, bool):
        return False
    if column_type == "string":
        return isinstance(value, str)
    if column_type == "long":
        return isinstance(value, int)
    return column_type == "double" and isinstance(value, (int, float))


if __name__ == "__main__":
    run("filter", OUTPUT_PORT, transform)
