"""DimensionScore ORM model."""
from sqlalchemy import String, Integer, Float, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional
from datetime import datetime
import uuid

from app.database.base import Base


class DimensionScore(Base):
    """Dimension score table (7 AI readiness dimensions)."""
    __tablename__ = "dimension_scores"

    # Primary key
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )

    # Fields
    assessment_id: Mapped[str] = mapped_column(ForeignKey("assessments.id"))
    dimension: Mapped[str] = mapped_column(String(30))
    score: Mapped[float] = mapped_column(Float)
    weight: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.8)
    evidence_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow
    )

    # Relationships
    assessment: Mapped["Assessment"] = relationship(
        "Assessment",
        back_populates="dimension_scores"
    )

    # Note: Snowflake doesn't support CHECK constraints
    # Validation is handled at the application layer (Pydantic models)

    def __repr__(self):
        return f"<DimensionScore(id={self.id}, dimension={self.dimension}, score={self.score})>"
