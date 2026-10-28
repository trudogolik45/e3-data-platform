"""Компоненты e2e-стенда глазами теста: рестарт сервисов compose и процесса контроллера.

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
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parents[2]
CONTROLLER_STOP_SECONDS = 30
POLL_SECONDS = 0.5


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
