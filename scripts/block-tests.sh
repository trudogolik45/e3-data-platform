#!/usr/bin/env bash
# Тесты Spark-блоков (tests/blocks) в образе блоков: там есть Java и pyspark.
#
# Ставит в образ те же версии контрактов, что и Dockerfile блоков, плюс pytest;
# pyspark и py4j берёт из самого Spark. Нужен только Docker.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
SPARK_IMAGE=apache/spark:3.5.9-java17-python3

docker run --rm -u root -v "$ROOT:/w:ro" -w /w \
  -e PYTHONPATH=/w/src:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  -e PYTHONDONTWRITEBYTECODE=1 \
  "$SPARK_IMAGE" bash -c '
    pip3 install -q --no-cache-dir pydantic==2.14.1 jsonschema==4.26.0 boto3==1.43.118 pytest==9.1.1 &&
    # Опции pytest-asyncio из pyproject.toml здесь не нужны.
    python3 -m pytest tests/blocks -p no:cacheprovider -q -W ignore::pytest.PytestConfigWarning "$@"
  ' -- "$@"
