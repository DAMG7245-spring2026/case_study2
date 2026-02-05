"""Document models for evidence collection."""
from pydantic import BaseModel, Field
from uuid import UUID, uuid4
from datetime import datetime, timezone
from enum import Enum


class DocumentStatus(str, Enum):
    """Document processing status."""
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    PARSED = "parsed"
    CHUNKED = "chunked"
    INDEXED = "indexed"
    FAILED = "failed"


class DocumentRecord(BaseModel):
    """Metadata record for a document."""
    id: UUID = Field(default_factory=uuid4)
    company_id: UUID
    ticker: str
    filing_type: str
    filing_date: datetime
    source_url: str | None = None
    local_path: str | None = None
    s3_key: str | None = None
    content_hash: str | None = None
    word_count: int | None = None
    chunk_count: int | None = None
    status: DocumentStatus = DocumentStatus.PENDING
    error_message: str | None = None
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    processed_at: datetime | None = None

    class Config:
        from_attributes = True


class DocumentChunkRecord(BaseModel):
    """A chunk of a document."""
    id: UUID = Field(default_factory=uuid4)
    document_id: UUID
    chunk_index: int
    content: str
    section: str | None = None
    start_char: int
    end_char: int
    word_count: int
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    class Config:
        from_attributes = True
