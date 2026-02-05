"""Company ORM model."""
from sqlalchemy import String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import List, Optional
from datetime import datetime
import uuid

from app.database.base import Base


class Company(Base):
    """Company information table."""
    __tablename__ = "companies"

    # Primary key
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )

    # Fields
    name: Mapped[str] = mapped_column(String(255))
    ticker: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    industry_id: Mapped[str] = mapped_column(ForeignKey("industries.id"))
    position_factor: Mapped[float] = mapped_column(Float, default=0.0)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    # Relationships
    industry: Mapped["Industry"] = relationship(
        "Industry",
        back_populates="companies"
    )
    assessments: Mapped[List["Assessment"]] = relationship(
        "Assessment",
        back_populates="company"
    )
    documents: Mapped[List["Document"]] = relationship(
        "Document",
        back_populates="company"
    )
    external_signals: Mapped[List["ExternalSignal"]] = relationship(
        "ExternalSignal",
        back_populates="company"
    )
    signal_summary: Mapped[Optional["CompanySignalSummary"]] = relationship(
        "CompanySignalSummary",
        back_populates="company",
        uselist=False
    )

    # Note: Snowflake doesn't support CHECK constraints
    # Validation is handled at the application layer (Pydantic models)

    def __repr__(self):
        return f"<Company(id={self.id}, name={self.name}, ticker={self.ticker})>"
