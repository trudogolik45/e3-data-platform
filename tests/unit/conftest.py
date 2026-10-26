import pytest

UNIT_S3_SECRET = "unit-s3-secret"


@pytest.fixture(autouse=True)
def s3_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """S3-ключи обязательны в настройках обоих сервисов: unit-тесты задают свои."""
    monkeypatch.setenv("E3_S3_ACCESS_KEY", "unit-s3-access")
    monkeypatch.setenv("E3_S3_SECRET_KEY", UNIT_S3_SECRET)
