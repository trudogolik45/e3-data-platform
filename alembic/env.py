import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from n3.blocks.models import Base as BlocksBase
from n3.catalog.models import Base as CatalogBase
from n3.identity.models import Base as IdentityBase
from n3.pipelines.models import Base as PipelinesBase
from n3.runs.models import Base as RunsBase

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = [
    CatalogBase.metadata,
    BlocksBase.metadata,
    PipelinesBase.metadata,
    RunsBase.metadata,
    IdentityBase.metadata,
]


def database_url() -> str:
    return config.get_main_option("sqlalchemy.url") or os.environ["N3_DATABASE_URL"]


def run_migrations_offline() -> None:
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        include_schemas=True,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata, include_schemas=True)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    engine = create_async_engine(database_url(), poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_async_migrations())
