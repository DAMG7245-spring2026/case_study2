"""Document ORM model."""
from sqlalchemy import String, Integer, Date, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import List, Optional
from datetime import datetime, date
import uuid

from app.database.base import Base


class Document(Base):
    """Document table (SEC filings, 10-K, 10-Q, etc.)."""
    __tablename__ = "documents"

    # Primary key
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4())
    )

    # Fields
    company_id: Mapped[str] = mapped_column(ForeignKey("companies.id"))
    ticker: Mapped[str] = mapped_column(String(10))
    filing_type: Mapped[str] = mapped_column(String(20))
    filing_date: Mapped[date] = mapped_column(Date)
    source_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    local_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    s3_key: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    content_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    word_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    chunk_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    error_message: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime,
        nullable=True
    )

    # Relationships
    company: Mapped["Company"] = relationship(
        "Company",
        back_populates="documents"
    )
    chunks: Mapped[List["DocumentChunk"]] = relationship(
        "DocumentChunk",
        back_populates="document"
    )

    # Note: Snowflake doesn't support CHECK constraints
    # Validation is handled at the application layer (Pydantic models)

    def __repr__(self):
        return f"<Document(id={self.id}, ticker={self.ticker}, filing_type={self.filing_type})>"
