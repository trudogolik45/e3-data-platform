"""Точка входа ASGI: `uvicorn n3.main:app`."""

from n3.api.app import create_app
from n3.settings import Settings

app = create_app(Settings())  # pyright: ignore[reportCallIssue]  # environment из N3_ENVIRONMENT
