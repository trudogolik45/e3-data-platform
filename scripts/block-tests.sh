#!/usr/bin/env bash
# Тесты Spark-блоков (tests/blocks) в образе блоков: там есть Java, pyspark и
# те же версии контрактов, что в кластере (см. blocks/Dockerfile). Код блоков и
# e3.shared берутся из рабочей копии, сверху ставится только pytest.
# Нужен только Docker.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
IMAGE=e3-block-tests

docker build -q -f "$ROOT/blocks/Dockerfile" --build-arg BLOCK=read -t "$IMAGE" "$ROOT" >/dev/null

docker run --rm -u root -v "$ROOT:/w:ro" -w /w -e PYTHONDONTWRITEBYTECODE=1 "$IMAGE" bash -c '
    export PYTHONPATH="/w/src:$(echo /opt/spark/python/lib/*.zip | tr " " :)"
    pip3 install -q --no-cache-dir pytest==9.1.1 &&
    # Опции pytest-asyncio из pyproject.toml здесь не нужны.
    python3 -m pytest tests/blocks -p no:cacheprovider -q -W ignore::pytest.PytestConfigWarning "$@"
  ' -- "$@"
