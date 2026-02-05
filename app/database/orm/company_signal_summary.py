"""CompanySignalSummary ORM model."""
from sqlalchemy import String, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional
from datetime import datetime

from app.database.base import Base


class CompanySignalSummary(Base):
    """Company signal summary table (aggregated external signals)."""
    __tablename__ = "company_signal_summaries"

    # Primary key (also a foreign key to companies)
    company_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("companies.id"),
        primary_key=True
    )

    # Fields
    ticker: Mapped[str] = mapped_column(String(10))
    technology_hiring_score: Mapped[float] = mapped_column(Float, default=0.0)
    innovation_activity_score: Mapped[float] = mapped_column(Float, default=0.0)
    digital_presence_score: Mapped[float] = mapped_column(Float, default=0.0)
    leadership_signals_score: Mapped[float] = mapped_column(Float, default=0.0)
    composite_score: Mapped[float] = mapped_column(Float, default=0.0)
    signal_count: Mapped[int] = mapped_column(Integer, default=0)
    last_updated: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True
    )

    # Relationships
    company: Mapped["Company"] = relationship(
        "Company",
        back_populates="signal_summary"
    )

    def __repr__(self):
        return f"<CompanySignalSummary(company_id={self.company_id}, ticker={self.ticker}, composite_score={self.composite_score})>"
