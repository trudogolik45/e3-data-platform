from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from e3.execution.workload import WorkloadConfig


class ExecutionSettings(BaseSettings):
    """Настройки Execution Controller: свои, без настроек Control Plane и PostgreSQL."""

    model_config = SettingsConfigDict(env_prefix="E3_", env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"
    execution_group_id: str = "e3-execution-controller"
    # Пауза опроса SparkApplication, когда команд нет.
    execution_poll_seconds: float = Field(default=2, gt=0)

    # Kubernetes: kubeconfig из KUBECONFIG, а внутри кластера — service account pod'а.
    spark_namespace: str = "default"
    spark_service_account: str = "spark-operator-spark"
    spark_version: str = "3.5.9"
    # S3 глазами workload'а в кластере.
    spark_s3_endpoint_url: str = "http://minio:9000"
    # Срок жизни Attempt Credentials, без продления (ADR-0006); границы — у MinIO STS.
    attempt_credentials_seconds: int = Field(default=12 * 3600, ge=900, le=365 * 24 * 3600)

    # S3 и STS глазами самого контроллера: читает ExecutionResult блока, пишет
    # Diagnostic Artifact и выдаёт Attempt Credentials. Ключи — его собственного
    # пользователя MinIO, не root.
    s3_endpoint_url: str = "http://localhost:9000"
    # Без значений по умолчанию: забытые ключи не должны подменяться общеизвестными.
    s3_access_key: SecretStr
    s3_secret_key: SecretStr
    s3_region: str = "us-east-1"
    s3_bucket: str = "e3-lake"

    def workload(self) -> WorkloadConfig:
        return WorkloadConfig(
            namespace=self.spark_namespace,
            service_account=self.spark_service_account,
            spark_version=self.spark_version,
            s3_endpoint_url=self.spark_s3_endpoint_url,
        )
