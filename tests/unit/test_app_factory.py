import pytest
from pydantic import ValidationError

from e3.api.app import create_app, project_quota
from e3.runs import Resources
from e3.settings import Settings
from e3.shared.messages import PodSize
from tests.unit.conftest import UNIT_S3_SECRET


def test_app_refuses_to_start_with_dev_auth_outside_dev() -> None:
    settings = Settings(environment="prod", auth_mode="dev")  # pyright: ignore[reportCallIssue]

    with pytest.raises(RuntimeError, match="dev-авторизация"):
        create_app(settings)


def test_settings_require_explicit_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("E3_ENVIRONMENT", raising=False)

    with pytest.raises(ValidationError, match="environment"):
        Settings(_env_file=None)  # type: ignore[call-arg]


@pytest.mark.parametrize("missing", ["E3_S3_ACCESS_KEY", "E3_S3_SECRET_KEY"])
def test_settings_require_explicit_s3_keys(monkeypatch: pytest.MonkeyPatch, missing: str) -> None:
    monkeypatch.delenv(missing)

    with pytest.raises(ValidationError, match=missing.removeprefix("E3_").lower()):
        Settings(environment="prod", _env_file=None)  # pyright: ignore[reportCallIssue]


def test_settings_hide_s3_keys_from_repr() -> None:
    settings = Settings(environment="prod")  # pyright: ignore[reportCallIssue]

    assert UNIT_S3_SECRET not in repr(settings)


@pytest.mark.parametrize(
    ("quota", "resource"),
    [({"project_quota_cpu_millicores": 1999}, "CPU"), ({"project_quota_memory_mib": 1023}, "RAM")],
)
def test_app_refuses_quota_smaller_than_one_attempt(quota: dict[str, int], resource: str) -> None:
    settings = Settings(environment="dev", **quota)  # pyright: ignore[reportArgumentType]

    with pytest.raises(ValueError, match=f"Project Quota по {resource}"):
        create_app(settings)


def test_quota_of_exactly_one_attempt_is_allowed() -> None:
    settings = Settings(  # pyright: ignore[reportCallIssue]
        environment="test", project_quota_cpu_millicores=2000, project_quota_memory_mib=1024
    )

    assert project_quota(settings).capacity == 1


@pytest.mark.parametrize(
    ("core_request", "ceiling"),
    [
        (None, Resources(cpu_millicores=20_000, memory_mib=10_240)),
        ("250m", Resources(cpu_millicores=5_000, memory_mib=10_240)),
        ("0.5", Resources(cpu_millicores=10_000, memory_mib=10_240)),
    ],
)
def test_default_quota_is_ten_attempts_of_configured_size(
    monkeypatch: pytest.MonkeyPatch, core_request: str | None, ceiling: Resources
) -> None:
    monkeypatch.delenv("E3_SPARK_CORE_REQUEST", raising=False)
    if core_request is not None:
        monkeypatch.setenv("E3_SPARK_CORE_REQUEST", core_request)

    quota = project_quota(Settings(environment="test", _env_file=None))  # pyright: ignore[reportCallIssue]

    assert (quota.ceiling, quota.capacity) == (ceiling, 10)


def test_attempt_size_comes_from_control_plane_settings() -> None:
    settings = Settings(environment="test", spark_core_request="250m", spark_memory_mib=1024)  # pyright: ignore[reportCallIssue]

    assert settings.pod_size() == PodSize(cpu_millicores=250, memory_mib=1024)


def test_oidc_mode_needs_issuer_audience_and_jwks() -> None:
    settings = Settings(environment="prod", auth_mode="oidc", oidc_issuer="https://idp.test")  # pyright: ignore[reportCallIssue]

    with pytest.raises(RuntimeError, match="oidc_audience"):
        create_app(settings)


def test_oidc_mode_refuses_symmetric_algorithms() -> None:
    settings = Settings(  # pyright: ignore[reportCallIssue]
        environment="prod",
        auth_mode="oidc",
        oidc_issuer="https://idp.test",
        oidc_audience="e3-api",
        oidc_jwks_url="https://idp.test/jwks",
        oidc_algorithms=["HS256"],
    )

    with pytest.raises(ValueError, match="алгоритм"):
        create_app(settings)
