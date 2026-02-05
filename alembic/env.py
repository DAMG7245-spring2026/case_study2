"""Alembic environment configuration for PE Org-AI-R Platform."""
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
import sys
import os

# Import snowflake-sqlalchemy to register the dialect
import snowflake.sqlalchemy

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Import all ORM models (this is CRITICAL for autogenerate to work)
from app.database.base import Base
from app.database.orm import (
    Industry,
    Company,
    Assessment,
    DimensionScore,
    Document,
    DocumentChunk,
    ExternalSignal,
    CompanySignalSummary,
)

# Import settings to get database URL
from app.config import get_settings

# Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set target metadata for 'autogenerate' support
target_metadata = Base.metadata


def get_url():
    """Build Snowflake connection URL from settings."""
    settings = get_settings()
    return (
        f"snowflake://{settings.snowflake_user}:{settings.snowflake_password}"
        f"@{settings.snowflake_account}/{settings.snowflake_database}/{settings.snowflake_schema}"
        f"?warehouse={settings.snowflake_warehouse}"
    )


def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    This configures the context with just a URL and not an Engine.
    By skipping the Engine creation we don't even need a DBAPI to be available.
    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.

    In this scenario we need to create an Engine and associate a connection with the context.
    """
    # Override sqlalchemy.url in alembic.ini
    configuration = config.get_section(config.config_ini_section)
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


# Determine which mode to run in
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
