"""Компоненты e2e-стенда глазами теста: рестарт сервисов compose и процесса контроллера,
трассы запусков в Jaeger.

`scripts/e2e.sh` поднимает compose-проект `COMPOSE_PROJECT_NAME` и запускает
`e3-execution-controller` фоном; его pid лежит в `E3_E2E_CONTROLLER_PID_FILE`,
а лог — в `E3_E2E_CONTROLLER_LOG`. Перезапущенный тестом контроллер пишет туда
же, и в конце его останавливает `scripts/e2e.sh`.
"""

import asyncio
import os
import shutil
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx

ROOT = Path(__file__).resolve().parents[2]
CONTROLLER_STOP_SECONDS = 30
POLL_SECONDS = 0.5
# Jaeger compose-стенда: HTTP API его UI (внутренний, версия образа закреплена в compose).
JAEGER_URL = "http://localhost:16686"
TRACE_TIMEOUT_SECONDS = 60
NOT_FOUND = 404


@dataclass(frozen=True)
class JaegerSpan:
    operation: str
    service: str
    span_id: str
    # span_id родителя; у корня — None.
    parent: str | None


async def jaeger_trace(run_id: str, *operations: str) -> list[JaegerSpan]:
    """Спаны трассы запуска в Jaeger, как только в ней есть все `operations`.

    Сервисы отправляют спаны пачками раз в несколько секунд, поэтому ждём.
    """
    async with (
        httpx.AsyncClient(base_url=JAEGER_URL) as client,
        asyncio.timeout(TRACE_TIMEOUT_SECONDS),
    ):
        while True:
            found = await _trace_spans(client, UUID(run_id).hex)
            if set(operations) <= {span.operation for span in found}:
                return found
            await asyncio.sleep(1)


async def _trace_spans(client: httpx.AsyncClient, trace_id: str) -> list[JaegerSpan]:
    response = await client.get(f"/api/traces/{trace_id}")
    if response.status_code == NOT_FOUND:
        return []
    response.raise_for_status()
    return [span for trace in response.json()["data"] for span in _spans(trace)]


def _spans(trace: dict[str, Any]) -> list[JaegerSpan]:
    services = {key: process["serviceName"] for key, process in trace["processes"].items()}
    return [
        JaegerSpan(
            operation=span["operationName"],
            service=services[span["processID"]],
            span_id=span["spanID"],
            parent=next(
                (ref["spanID"] for ref in span["references"] if ref["refType"] == "CHILD_OF"),
                None,
            ),
        )
        for span in trace["spans"]
    ]


async def restart_service(service: str) -> None:
    """`docker compose restart`: контейнер с его данными остаётся, процесс — новый."""
    await _compose("restart", service)


async def _compose(*args: str) -> str:
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "-f",
        str(ROOT / "docker-compose.yml"),
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, errors = await process.communicate()
    assert process.returncode == 0, f"docker compose {' '.join(args)}: {errors.decode()}"
    return output.decode()


async def restart_controller() -> None:
    """Останавливает процесс Execution Controller и запускает новый с тем же окружением."""
    await _stop(int(_pid_file().read_text()))
    _pid_file().write_text(str(_spawn_controller()))


def _pid_file() -> Path:
    return Path(os.environ["E3_E2E_CONTROLLER_PID_FILE"])


async def _stop(pid: int) -> None:
    os.kill(pid, signal.SIGTERM)
    async with asyncio.timeout(CONTROLLER_STOP_SECONDS):
        while True:
            if not _alive(pid):
                return
            await asyncio.sleep(POLL_SECONDS)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _spawn_controller() -> int:
    executable = shutil.which("e3-execution-controller")
    assert executable is not None, "e3-execution-controller не установлен"
    with Path(os.environ["E3_E2E_CONTROLLER_LOG"]).open("ab") as log:
        # Отдельная сессия: процесс переживает pytest, пока его не остановит e2e.sh.
        process = subprocess.Popen(
            [executable], stdout=log, stderr=subprocess.STDOUT, start_new_session=True
        )
    return process.pid


async def pipeline_runs(project_id: UUID) -> int:
    """Сколько PipelineRun в Project: списка запусков в API нет, считаем в PostgreSQL стенда."""
    query = f"SELECT count(*) FROM runs.pipeline_runs WHERE project_id = '{project_id}'"
    return int(await _compose("exec", "-T", "postgres", "psql", "-U", "e3", "-tAc", query))
