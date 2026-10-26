"""`n3-register-block <spec.yaml>` — регистрация BlockSpec через admin API.

Тонкий HTTP-клиент к `POST /api/v1/blocks`: спецификацию проверяет сервер.
Адрес API — `N3_API_URL`, токен команды платформы — `N3_PLATFORM_TOKEN`.
"""

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx
import yaml

DEFAULT_API_URL = "http://localhost:8000"
TIMEOUT_SECONDS = 30


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    token = os.environ.get("N3_PLATFORM_TOKEN")
    if not token:
        print("не задан N3_PLATFORM_TOKEN", file=sys.stderr)
        return 2
    api_url = os.environ.get("N3_API_URL", DEFAULT_API_URL).rstrip("/")
    try:
        response = _register(api_url, token, _load_spec(args.spec))
    except httpx.HTTPError as error:
        print(f"API недоступен: {error}", file=sys.stderr)
        return 1
    if response.is_error:
        print(f"{response.status_code}: {response.text}", file=sys.stderr)
        return 1
    spec = response.json()
    print(f"блок {spec['name']}:{spec['version']} зарегистрирован")
    return 0


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="n3-register-block", description=__doc__)
    parser.add_argument("spec", type=Path, help="YAML-файл с BlockSpec")
    return parser.parse_args(argv)


def _load_spec(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _register(api_url: str, token: str, spec: Any) -> httpx.Response:
    return httpx.post(
        f"{api_url}/api/v1/blocks",
        json=spec,
        headers={"Authorization": f"Bearer {token}"},
        timeout=TIMEOUT_SECONDS,
    )
