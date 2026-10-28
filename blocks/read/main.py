"""Блок `Read`: CSV или Parquet со входа `source` → Parquet на выходе `data`.

Колонки CSV — строки. Параметр `types` приводит указанные колонки к `long` или
`double`; значение, которое не приводится, роняет блок с `CAST_INVALID_INPUT`.
В ошибке колонка и тип, но не значение: оно из данных.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from spark_io import BlockError, Parameters, fail_row, run

OUTPUT_PORT = "data"


def transform(frame: DataFrame, parameters: Parameters) -> DataFrame:
    types = parameters.get("types", {})
    missing = sorted(set(types) - set(frame.columns))
    if missing:
        raise BlockError(f"types: нет колонок {', '.join(missing)}")
    return frame.select(
        *[_cast(name, types[name]) if name in types else F.col(name) for name in frame.columns]
    )


def _cast(name: str, target_type: str) -> Column:
    """Значение, которое не приводится, роняет запись своей ошибкой, а не ошибкой Spark.

    Ошибка ANSI-приведения Spark называет значение, но не колонку.
    """
    column = F.col(name)
    quoted = "`" + name.replace("`", "``") + "`"
    malformed = column.isNotNull() & F.expr(f"try_cast({quoted} AS {target_type})").isNull()
    error = BlockError(
        f"колонка {name}: значения не приводятся к {target_type}",
        code="CAST_INVALID_INPUT",
        column=name,
        target_type=target_type,
    )
    return F.when(malformed, fail_row(error)).otherwise(column.cast(target_type)).alias(name)


if __name__ == "__main__":
    run("read", OUTPUT_PORT, transform)
