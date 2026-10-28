"""E2E на kind: NetworkPolicy закрывает pod'ам попыток всю сеть, кроме DNS, S3,
Kubernetes API и связи между pod'ами попыток (PRD §9, ADR-0001).

`scripts/e2e.sh` подключает PostgreSQL, Kafka и API compose-стенда к сети kind и
передаёт их адреса: контрольный pod без метки попытки до них достаёт, поэтому
запрет проверяется, а не выполняется сам собой. kindnet соблюдает NetworkPolicy
через kube-network-policies; если egress или `ipBlock` не соблюдаются, тесты
красные. Что разрешённого хватает Spark, проверяет `Read → Filter → Aggregate`
в соседних тестах: политика применяется до них.
"""

import os

import pytest

from e3.execution.settings import ExecutionSettings
from tests.e2e.network import attempt_labels, can_connect, ensure_namespace, listener, resolves

API_URL = os.environ.get("E3_E2E_API_URL")

pytestmark = pytest.mark.skipif(API_URL is None, reason="E2E запускает scripts/e2e.sh")

OTHER_NAMESPACE = "e3-e2e-outsider"
KUBERNETES_API = "kubernetes.default.svc.cluster.local:443"
# Адрес в интернете; контрольный pod без метки до него достаёт.
INTERNET = "1.1.1.1:443"
# Адреса Control Plane в сети kind из scripts/e2e.sh.
CONTROL_PLANE = {
    "postgres": "E3_E2E_POSTGRES_ADDRESS",
    "kafka": "E3_E2E_KAFKA_ADDRESS",
    "api": "E3_E2E_CONTROL_PLANE_API_ADDRESS",
}
FORBIDDEN = [*CONTROL_PLANE, "internet"]


def namespace() -> str:
    return ExecutionSettings().spark_namespace  # pyright: ignore[reportCallIssue]


def address(target: str) -> str:
    return os.environ[CONTROL_PLANE[target]] if target in CONTROL_PLANE else INTERNET


@pytest.mark.parametrize("target", FORBIDDEN)
async def test_pod_without_attempt_label_reaches_control_plane_and_internet(target: str) -> None:
    assert await can_connect(address(target), namespace=namespace(), labels={})


@pytest.mark.parametrize("target", FORBIDDEN)
async def test_attempt_pod_cannot_reach_control_plane_or_internet(target: str) -> None:
    assert not await can_connect(address(target), namespace=namespace(), labels=attempt_labels())


async def test_attempt_pod_reaches_dns_s3_and_kubernetes_api() -> None:
    labels = attempt_labels()

    dns = await resolves(
        "kubernetes.default.svc.cluster.local", namespace=namespace(), labels=labels
    )
    s3 = await can_connect(os.environ["E3_E2E_S3_ADDRESS"], namespace=namespace(), labels=labels)
    kubernetes = await can_connect(KUBERNETES_API, namespace=namespace(), labels=labels)

    assert (dns, s3, kubernetes) == (True, True, True)


async def test_attempt_pods_reach_each_other() -> None:
    async with listener(namespace(), attempt_labels()) as driver:
        reached = await can_connect(driver, namespace=namespace(), labels=attempt_labels())

    assert reached


async def test_pod_outside_namespace_cannot_reach_attempt_pod() -> None:
    await ensure_namespace(OTHER_NAMESPACE)
    async with (
        listener(namespace(), {}) as plain,
        listener(namespace(), attempt_labels()) as attempt,
    ):
        to_plain = await can_connect(plain, namespace=OTHER_NAMESPACE, labels={})
        to_attempt = await can_connect(attempt, namespace=OTHER_NAMESPACE, labels={})

    assert (to_plain, to_attempt) == (True, False)
