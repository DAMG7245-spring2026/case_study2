"""Assessment ORM model."""
from sqlalchemy import String, Float, Date, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import List, Optional
from datetime import datetime, date
import uuid

from app.database.base import Base


class Assessment(Base):
    """Assessment record table."""
    __tablename__ = "assessments"

    # Primary key
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )

    # Fields
    company_id: Mapped[str] = mapped_column(ForeignKey("companies.id"))
    assessment_type: Mapped[str] = mapped_column(String(20))
    assessment_date: Mapped[date] = mapped_column(Date)
    status: Mapped[str] = mapped_column(String(20), default="draft")
    primary_assessor: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True
    )
    secondary_assessor: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True
    )
    v_r_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    confidence_lower: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    confidence_upper: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow
    )

    # Relationships
    company: Mapped["Company"] = relationship(
        "Company",
        back_populates="assessments"
    )
    dimension_scores: Mapped[List["DimensionScore"]] = relationship(
        "DimensionScore",
        back_populates="assessment"
    )

    # Note: Snowflake doesn't support CHECK constraints
    # Validation is handled at the application layer (Pydantic models)

    def __repr__(self):
        return f"<Assessment(id={self.id}, company_id={self.company_id}, type={self.assessment_type})>"
