"""ExternalSignal ORM model."""
from sqlalchemy import String, Float, Date, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional
from datetime import datetime, date
import uuid

from app.database.base import Base

# Snowflake-specific type for JSON/semi-structured data
try:
    from snowflake.sqlalchemy import VARIANT
except ImportError:
    from sqlalchemy.types import JSON as VARIANT  # Fallback for non-Snowflake databases


class ExternalSignal(Base):
    """External signal table (LinkedIn hiring, GitHub activity, etc.)."""
    __tablename__ = "external_signals"

    # Primary key
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )

    # Fields
    company_id: Mapped[str] = mapped_column(ForeignKey("companies.id"))
    category: Mapped[str] = mapped_column(String(30))
    source: Mapped[str] = mapped_column(String(30))
    signal_date: Mapped[date] = mapped_column(Date)
    raw_value: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    normalized_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    signal_metadata: Mapped[Optional[dict]] = mapped_column("metadata", VARIANT, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow
    )

    # Relationships
    company: Mapped["Company"] = relationship(
        "Company",
        back_populates="external_signals"
    )

    # Note: Snowflake doesn't support CHECK constraints
    # Validation is handled at the application layer (Pydantic models)

    def __repr__(self):
        return f"<ExternalSignal(id={self.id}, company_id={self.company_id}, category={self.category})>"
