"""Блок `Read`: CSV или Parquet со входа `source` → Parquet на выходе `data`.

Колонки CSV — строки. Параметр `types` приводит указанные колонки к `long` или
`double`; значение, которое не приводится, роняет блок (ANSI-режим Spark).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from spark_io import BlockError, Parameters, run

OUTPUT_PORT = "data"


def transform(frame: DataFrame, parameters: Parameters) -> DataFrame:
    types = parameters.get("types", {})
    missing = sorted(set(types) - set(frame.columns))
    if missing:
        raise BlockError(f"types: нет колонок {', '.join(missing)}")
    return frame.select(
        *[F.col(name).cast(types[name]) if name in types else F.col(name) for name in frame.columns]
    )


if __name__ == "__main__":
    run("read", OUTPUT_PORT, transform)
