import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import uvicorn

from n3.api.app import create_app
from n3.cli.register_block import main
from n3.settings import Settings
from tests.integration.conftest import PLATFORM_TOKEN

pytestmark = pytest.mark.integration

SPEC_YAML = """\
name: {name}
version: "1.0.0"
runtime: spark
inputs: []
outputs:
  - name: data
    data_type: dataset
parameters_schema: {{}}
executable_ref: registry.local/n3/read@sha256:{digest}
"""


@pytest.fixture
def api_url(settings: Settings) -> Iterator[str]:
    port = _free_port()
    config = uvicorn.Config(create_app(settings), host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def spec_file(tmp_path: Path) -> Path:
    path = tmp_path / "spec.yaml"
    path.write_text(SPEC_YAML.format(name=f"read-{uuid4().hex}", digest="d" * 64))
    return path


def test_cli_registers_block_through_running_api(
    api_url: str, spec_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("N3_API_URL", api_url)
    monkeypatch.setenv("N3_PLATFORM_TOKEN", PLATFORM_TOKEN)

    exit_code = main([str(spec_file)])

    assert exit_code == 0
    names = [block["name"] for block in httpx.get(f"{api_url}/api/v1/blocks").json()]
    assert spec_file.read_text().splitlines()[0].removeprefix("name: ") in names


def test_cli_fails_when_api_refuses_token(
    api_url: str,
    spec_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("N3_API_URL", api_url)
    monkeypatch.setenv("N3_PLATFORM_TOKEN", "wrong-token")

    exit_code = main([str(spec_file)])

    assert exit_code != 0
    assert "403" in capsys.readouterr().err
