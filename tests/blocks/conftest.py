"""Тесты Spark-блоков: идут в образе `apache/spark`, где есть Java и pyspark.

Запуск — `scripts/block-tests.sh`. В обычном `uv run pytest` pyspark нет, и эти
тесты не собираются.
"""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

BLOCKS = Path(__file__).resolve().parents[2] / "blocks"

if importlib.util.find_spec("pyspark") is None:
    collect_ignore_glob = ["test_*.py"]
else:
    # Как в образе: `main.py` блока лежит рядом с `spark_io.py`.
    sys.path.insert(0, str(BLOCKS))


def block(name: str) -> ModuleType:
    """`main.py` блока как модуль `<name>_block`: у всех блоков файл называется одинаково."""
    spec = importlib.util.spec_from_file_location(f"{name}_block", BLOCKS / name / "main.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def spark():
    """Та же сессия, что у блоков в кластере, включая ANSI-режим; master — local."""
    from spark_io import session

    spark = session("e3-block-tests")
    yield spark
    spark.stop()
