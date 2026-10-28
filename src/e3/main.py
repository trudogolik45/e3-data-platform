"""Точка входа ASGI: `uvicorn e3.main:app`."""

from e3.api.app import create_app
from e3.settings import Settings
from e3.shared.tracing import configure_logging

configure_logging()
app = create_app(Settings())  # pyright: ignore[reportCallIssue]  # environment и S3-ключи из E3_*
