from typing import Literal
from uuid import UUID

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from e3.shared.messages import PodSize


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="E3_", env_file=".env", extra="ignore")

    # Без значения по умолчанию: забытая переменная не должна включать dev-доступ.
    environment: Literal["dev", "test", "prod"]
    auth_mode: Literal["dev", "oidc"] = "dev"
    # OIDC-провайдер. JWKS — отдельным адресом: из контейнера провайдер виден не по `iss`.
    oidc_issuer: str | None = None
    oidc_audience: str | None = None
    oidc_jwks_url: str | None = None
    oidc_algorithms: list[str] = Field(default_factory=lambda: ["RS256"])

    database_url: str = "postgresql+asyncpg://e3:e3@localhost:5432/e3"

    s3_endpoint_url: str = "http://localhost:9000"
    s3_public_endpoint_url: str | None = None
    # Без значений по умолчанию: забытые ключи не должны подменяться общеизвестными.
    s3_access_key: SecretStr
    s3_secret_key: SecretStr
    s3_region: str = "us-east-1"
    s3_bucket: str = "e3-lake"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "e3-run-orchestrator"
    # Пауза фоновых шагов Run Orchestrator, когда работы нет.
    runs_idle_seconds: float = Field(default=0.5, gt=0)
    # Сколько попытка может молчать, прежде чем Run Orchestrator спросит о ней Controller.
    reconcile_after_seconds: int = Field(default=600, gt=0)
    # Сколько хранится staging попыток после завершения PipelineRun (PRD §8)
    # и как часто фоновая очистка его ищет.
    staging_retention_days: float = Field(default=7, gt=0)
    staging_purge_period_seconds: float = Field(default=3600, gt=0)

    # Размер pod'а попытки, driver и executor одинаковы: CPU-запрос в нотации Kubernetes
    # ("250m" на тесном стенде, "0.5", "1") и память Spark. Уходит в SubmitExecution.
    spark_core_request: str = "1"
    spark_memory_mib: int = Field(default=512, gt=0)
    # Project Quota (ADR-0005), общая для всех Project; не задана — 10 попыток по ресурсу.
    project_quota_cpu_millicores: int | None = Field(default=None, gt=0)
    project_quota_memory_mib: int | None = Field(default=None, gt=0)

    # Токен команды платформы для admin API. Не задан — регистрация блоков закрыта.
    platform_token: SecretStr | None = None

    upload_url_ttl_seconds: int = 3600
    upload_lease_seconds: float = Field(default=300, gt=0)

    dev_organization_id: UUID = UUID("00000000-0000-4000-8000-000000000001")
    dev_project_id: UUID = UUID("00000000-0000-4000-8000-000000000002")
    dev_user_id: UUID = UUID("00000000-0000-4000-8000-000000000003")

    def pod_size(self) -> PodSize:
        return PodSize(
            cpu_millicores=_millicores(self.spark_core_request), memory_mib=self.spark_memory_mib
        )


def _millicores(quantity: str) -> int:
    """CPU в нотации Kubernetes: "250m" или доля ядра "0.5"."""
    if quantity.endswith("m"):
        return int(quantity.removesuffix("m"))
    return round(float(quantity) * 1000)
