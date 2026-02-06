"""Database connection and session management."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
from functools import lru_cache

from app.config import get_settings


@lru_cache
def get_engine():
    """Create and cache SQLAlchemy engine for Snowflake."""
    settings = get_settings()

    # Construct Snowflake connection URL
    snowflake_url = (
        f"snowflake://{settings.snowflake_user}:{settings.snowflake_password}"
        f"@{settings.snowflake_account}/{settings.snowflake_database}/{settings.snowflake_schema}"
        f"?warehouse={settings.snowflake_warehouse}"
    )

    engine = create_engine(
        snowflake_url,
        echo=settings.debug,  # Log SQL in debug mode
        pool_pre_ping=True,  # Verify connections before using
        pool_size=5,
        max_overflow=10
    )

    return engine


# Session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=get_engine()
)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency function for FastAPI to get database session.

    Usage in FastAPI:
        @app.get("/companies")
        def get_companies(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
