"""Пробные pod'ы в kind: проверяют, куда пускает сеть pod с меткой попытки и без неё.

Пробы — busybox: `nc -z` к адресу, `nslookup` к DNS кластера, `httpd` как слушатель.
Образ ноды kind тянут сами: сеть ноды NetworkPolicy не ограничивает.
"""

import asyncio
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import uuid4

from kubernetes_asyncio import config as kube_config
from kubernetes_asyncio.client import (
    ApiClient,
    CoreV1Api,
    V1Container,
    V1Namespace,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
)
from kubernetes_asyncio.client.exceptions import ApiException

from e3.execution.kubernetes import NOT_FOUND
from e3.execution.workload import ATTEMPT_LABEL

PROBE_IMAGE = os.environ.get("E3_E2E_PROBE_IMAGE", "busybox:1.37")
CONNECT_SECONDS = 5
# kindnet применяет NetworkPolicy к новому pod'у не сразу: первые ~секунду его
# соединения проходят. Код блока стартует позже — после JVM и SparkContext.
POLICY_SETTLE_SECONDS = 5
POD_TIMEOUT_SECONDS = 120
LISTENER_PORT = 8080
FINISHED = {"Succeeded", "Failed"}
CONFLICT = 409


def attempt_labels() -> dict[str, str]:
    """Метка, по которой NetworkPolicy узнаёт pod'ы попыток."""
    return {ATTEMPT_LABEL: str(uuid4())}


async def can_connect(address: str, *, namespace: str, labels: dict[str, str]) -> bool:
    """`address` — `host:port`; удалось ли открыть TCP-соединение."""
    host, port = address.rsplit(":", 1)
    command = ["nc", "-z", "-w", str(CONNECT_SECONDS), host, port]
    return await _succeeds(command, namespace=namespace, labels=labels)


async def resolves(name: str, *, namespace: str, labels: dict[str, str]) -> bool:
    return await _succeeds(["nslookup", name], namespace=namespace, labels=labels)


@asynccontextmanager
async def listener(namespace: str, labels: dict[str, str]) -> AsyncGenerator[str]:
    """Pod с HTTP-сервером на время блока; отдаёт его адрес `ip:port`."""
    name = f"e3-listener-{uuid4().hex[:8]}"
    command = ["httpd", "-f", "-p", str(LISTENER_PORT)]
    async with _core() as core:
        await core.create_namespaced_pod(namespace, _pod(name, command, labels))
        try:
            yield f"{await _running_ip(core, name, namespace)}:{LISTENER_PORT}"
        finally:
            await _delete(core, name, namespace)


async def ensure_namespace(name: str) -> None:
    async with _core() as core:
        try:
            await core.create_namespace(V1Namespace(metadata=V1ObjectMeta(name=name)))
        except ApiException as error:
            if error.status != CONFLICT:
                raise


async def _succeeds(command: list[str], *, namespace: str, labels: dict[str, str]) -> bool:
    """Проба ждёт `POLICY_SETTLE_SECONDS`: проверяется политика нового pod'а, а не окно до неё."""
    name = f"e3-probe-{uuid4().hex[:8]}"
    settled = ["sh", "-c", f'sleep {POLICY_SETTLE_SECONDS} && exec "$@"', "probe", *command]
    async with _core() as core:
        await core.create_namespaced_pod(namespace, _pod(name, settled, labels))
        try:
            return await _finished_phase(core, name, namespace) == "Succeeded"
        finally:
            await _delete(core, name, namespace)


async def _finished_phase(core: CoreV1Api, name: str, namespace: str) -> str:
    async with asyncio.timeout(POD_TIMEOUT_SECONDS):
        while True:
            phase = (await core.read_namespaced_pod(name, namespace)).status.phase
            if phase in FINISHED:
                return phase
            await asyncio.sleep(1)


async def _running_ip(core: CoreV1Api, name: str, namespace: str) -> str:
    async with asyncio.timeout(POD_TIMEOUT_SECONDS):
        while True:
            status = (await core.read_namespaced_pod(name, namespace)).status
            if status.phase == "Running" and status.pod_ip:
                return status.pod_ip
            await asyncio.sleep(1)


def _pod(name: str, command: list[str], labels: dict[str, str]) -> V1Pod:
    return V1Pod(
        metadata=V1ObjectMeta(name=name, labels=labels),
        spec=V1PodSpec(
            restart_policy="Never",
            termination_grace_period_seconds=0,
            containers=[
                V1Container(
                    name="probe",
                    image=PROBE_IMAGE,
                    image_pull_policy="IfNotPresent",
                    command=command,
                )
            ],
        ),
    )


async def _delete(core: CoreV1Api, name: str, namespace: str) -> None:
    try:
        await core.delete_namespaced_pod(name, namespace, grace_period_seconds=0)
    except ApiException as error:
        if error.status != NOT_FOUND:
            raise


@asynccontextmanager
async def _core() -> AsyncGenerator[CoreV1Api]:
    await kube_config.load_kube_config()
    api = ApiClient()
    try:
        yield CoreV1Api(api)
    finally:
        await api.close()
