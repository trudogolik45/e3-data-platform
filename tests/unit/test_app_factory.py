import pytest
from pydantic import ValidationError

from e3.api.app import create_app
from e3.settings import Settings


def test_app_refuses_to_start_with_dev_auth_outside_dev() -> None:
    settings = Settings(environment="prod", auth_mode="dev")

    with pytest.raises(RuntimeError, match="dev-авторизация"):
        create_app(settings)


def test_settings_require_explicit_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("E3_ENVIRONMENT", raising=False)

    with pytest.raises(ValidationError, match="environment"):
        Settings(_env_file=None)  # type: ignore[call-arg]
