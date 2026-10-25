from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING

import boto3
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from e3 import blocks, catalog, identity, pipelines, runs
from e3.settings import Settings
from e3.shared.storage import ObjectStorage

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def create_app(settings: Settings, *, storage: ObjectStorage | None = None) -> FastAPI:
    _ensure_auth_allowed(settings)
    app = FastAPI(title="E3 Control Plane", lifespan=_run_orchestrator_in_background(settings))
    app.state.principal = identity.dev_principal(
        user_id=settings.dev_user_id,
        organization_id=settings.dev_organization_id,
        project_id=settings.dev_project_id,
    )
    sessions = async_sessionmaker(
        create_async_engine(settings.database_url), expire_on_commit=False
    )
    storage = storage or create_storage(settings)
    app.state.catalog = catalog.Catalog(
        sessions,
        storage,
        upload_lease=timedelta(seconds=settings.upload_lease_seconds),
        upload_url_ttl_seconds=settings.upload_url_ttl_seconds,
    )
    app.state.blocks = blocks.BlockRegistry(sessions)
    app.state.pipelines = pipelines.PipelineService(sessions, app.state.blocks)
    app.state.runs = runs.RunOrchestrator(
        sessions,
        datasets=app.state.catalog,
        pipelines=app.state.pipelines,
        blocks=app.state.blocks,
        storage=storage,
        reconcile_after=timedelta(seconds=settings.reconcile_after_seconds),
    )
    app.state.platform_token = settings.platform_token
    app.include_router(catalog.router)
    app.include_router(blocks.router)
    app.include_router(pipelines.router)
    app.include_router(runs.router)
    return app


def _run_orchestrator_in_background(settings: Settings):
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        async with runs.background(
            app.state.runs,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            idle_seconds=settings.runs_idle_seconds,
        ):
            yield

    return lifespan


def create_storage(settings: Settings) -> ObjectStorage:
    public_endpoint = settings.s3_public_endpoint_url or settings.s3_endpoint_url
    return ObjectStorage(
        _s3_client(settings, settings.s3_endpoint_url),
        _s3_client(settings, public_endpoint),
        settings.s3_bucket,
    )


def _s3_client(settings: Settings, endpoint_url: str) -> "S3Client":
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _ensure_auth_allowed(settings: Settings) -> None:
    if settings.auth_mode == "dev" and settings.environment != "dev":
        raise RuntimeError("dev-авторизация разрешена только в environment=dev")
