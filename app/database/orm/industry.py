"""Industry ORM model."""
from sqlalchemy import String, Float, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import List, Optional
from datetime import datetime
import uuid

from app.database.base import Base


class Industry(Base):
    """Industry reference data table."""
    __tablename__ = "industries"

    # Primary key
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )

    # Fields
    name: Mapped[str] = mapped_column(String(255), unique=True)
    sector: Mapped[str] = mapped_column(String(100))
    h_r_base: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow
    )

    # Relationships
    companies: Mapped[List["Company"]] = relationship(
        "Company",
        back_populates="industry"
    )

    def __repr__(self):
        return f"<Industry(id={self.id}, name={self.name}, sector={self.sector})>"
