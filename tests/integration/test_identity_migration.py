"""Миграция 0010: запуски без Initiator достаются `legacy_migration` (ADR-0004).

Отдельная база: откат и накат миграций не задевают общий стенд.
"""

import asyncio
from collections.abc import Iterator
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer

from n3.identity import LEGACY_MIGRATION_USER_ID

pytestmark = pytest.mark.integration


async def _execute(url: str, sql: str, **params: object) -> list[tuple[object, ...]]:
    engine = create_async_engine(url, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as connection:
            result = await connection.execute(text(sql), params)
            return [tuple(row) for row in result] if result.returns_rows else []
    finally:
        await engine.dispose()


def execute(url: str, sql: str, **params: object) -> list[tuple[object, ...]]:
    return asyncio.run(_execute(url, sql, **params))


@pytest.fixture
def fresh_database(postgres: PostgresContainer) -> Iterator[str]:
    server = postgres.get_connection_url()
    name = f"migration_{uuid4().hex}"
    execute(server, f"CREATE DATABASE {name}")
    yield make_url(server).set(database=name).render_as_string(hide_password=False)
    execute(server, f"DROP DATABASE {name} WITH (FORCE)")


def migrate(url: str, revision: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", url)
    command.upgrade(config, revision)


def test_runs_created_before_initiator_belong_to_legacy_migration(fresh_database: str) -> None:
    migrate(fresh_database, "0009")
    run_id = uuid4()
    execute(
        fresh_database,
        "INSERT INTO runs.pipeline_runs (id, project_id, pipeline_id, pipeline_version, state,"
        " inputs, outputs, idempotency_key, request_hash)"
        " VALUES (:id, :project, :pipeline, 1, 'SUCCEEDED', '{}', '{}', 'k', 'h')",
        id=run_id,
        project=uuid4(),
        pipeline=uuid4(),
    )

    migrate(fresh_database, "head")

    [(initiator,)] = execute(
        fresh_database,
        "SELECT initiated_by_user_id FROM runs.pipeline_runs WHERE id = :id",
        id=run_id,
    )
    assert initiator == LEGACY_MIGRATION_USER_ID
    assert execute(
        fresh_database,
        "SELECT count(*) FROM identity.memberships WHERE user_id = :user",
        user=LEGACY_MIGRATION_USER_ID,
    ) == [(0,)]
