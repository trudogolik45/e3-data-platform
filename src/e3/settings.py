from typing import Literal
from uuid import UUID

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="E3_", env_file=".env", extra="ignore")

    # Без значения по умолчанию: забытая переменная не должна включать dev-доступ.
    environment: Literal["dev", "test", "prod"]
    auth_mode: Literal["dev"] = "dev"

    database_url: str = "postgresql+asyncpg://e3:e3@localhost:5432/e3"

    s3_endpoint_url: str = "http://localhost:9000"
    s3_public_endpoint_url: str | None = None
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_region: str = "us-east-1"
    s3_bucket: str = "e3-lake"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "e3-run-orchestrator"
    # Пауза фоновых шагов Run Orchestrator, когда работы нет.
    runs_idle_seconds: float = Field(default=0.5, gt=0)
    # Сколько попытка может молчать, прежде чем Run Orchestrator спросит о ней Controller.
    reconcile_after_seconds: int = Field(default=600, gt=0)

    # Токен команды платформы для admin API. Не задан — регистрация блоков закрыта.
    platform_token: SecretStr | None = None

    upload_url_ttl_seconds: int = 3600
    upload_lease_seconds: float = Field(default=300, gt=0)

    dev_organization_id: UUID = UUID("00000000-0000-4000-8000-000000000001")
    dev_project_id: UUID = UUID("00000000-0000-4000-8000-000000000002")
    dev_user_id: UUID = UUID("00000000-0000-4000-8000-000000000003")
