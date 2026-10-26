"""Project Quota: потолок CPU и RAM, которые заявили работающие ExecutionAttempt Project (ADR-0005).

Все попытки одного размера, поэтому квота сводится к числу попыток, которые
влезают под оба потолка сразу. Занятое место — число попыток в LIVE.
"""

from dataclasses import dataclass

from e3.shared.messages import PodSize

# Driver и executor попытки, оба размера PodSize.
PODS_PER_ATTEMPT = 2
# Потолок по умолчанию — столько попыток, во сколько раз ни меняй их размер.
DEFAULT_ATTEMPTS = 10


@dataclass(frozen=True)
class Resources:
    cpu_millicores: int
    memory_mib: int

    def __mul__(self, count: int) -> "Resources":
        return Resources(self.cpu_millicores * count, self.memory_mib * count)


@dataclass(frozen=True)
class ProjectQuota:
    price: Resources
    ceiling: Resources

    def __post_init__(self) -> None:
        _fits_one("CPU", self.ceiling.cpu_millicores, self.price.cpu_millicores)
        _fits_one("RAM", self.ceiling.memory_mib, self.price.memory_mib)

    @classmethod
    def configured(
        cls, pod: PodSize, *, cpu_millicores: int | None, memory_mib: int | None
    ) -> "ProjectQuota":
        """Не заданный потолок — `DEFAULT_ATTEMPTS` попыток по своему ресурсу."""
        price = Resources(pod.cpu_millicores, pod.memory_mib) * PODS_PER_ATTEMPT
        default = price * DEFAULT_ATTEMPTS
        ceiling = Resources(
            cpu_millicores or default.cpu_millicores, memory_mib or default.memory_mib
        )
        return cls(price, ceiling)

    @property
    def capacity(self) -> int:
        """Сколько попыток Project работают одновременно: занятое + цена ≤ потолок по обоим."""
        return min(
            self.ceiling.cpu_millicores // self.price.cpu_millicores,
            self.ceiling.memory_mib // self.price.memory_mib,
        )

    def used(self, live: int) -> Resources:
        return self.price * live


def _fits_one(resource: str, ceiling: int, price: int) -> None:
    """Квота меньше одной попытки — ошибка конфигурации: ни один узел не стартовал бы."""
    if ceiling < price:
        raise ValueError(f"Project Quota по {resource} ({ceiling}) меньше одной попытки ({price})")
