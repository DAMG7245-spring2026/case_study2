"""Evidence models combining documents and signals."""

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from app.models.document import DocumentResponse
from app.models.signal import CompanySignalSummaryResponse, ExternalSignalResponse


class CompanyEvidence(BaseModel):
    """Complete evidence package for a company."""
    company_id: UUID
    ticker: str
    company_name: str
    
    # Document evidence
    document_count: int = 0
    chunk_count: int = 0
    documents: list[DocumentResponse] = Field(default_factory=list)
    
    # Signal evidence  
    signal_summary: Optional[CompanySignalSummaryResponse] = None
    signals: list[ExternalSignalResponse] = Field(default_factory=list)
    
    # Metadata
    last_updated: datetime


class EvidenceStats(BaseModel):
    """Statistics about evidence collection."""
    total_companies: int = 0
    total_documents: int = 0
    total_chunks: int = 0
    total_signals: int = 0
    
    documents_by_type: dict[str, int] = Field(default_factory=dict)
    documents_by_status: dict[str, int] = Field(default_factory=dict)
    signals_by_category: dict[str, int] = Field(default_factory=dict)
    
    companies_with_documents: int = 0
    companies_with_signals: int = 0
    
    last_collection_time: Optional[datetime] = None


class BackfillRequest(BaseModel):
    """Request to backfill evidence for companies."""
    tickers: Optional[list[str]] = None  # None means all companies
    include_documents: bool = True
    include_signals: bool = True
    years_back: int = Field(default=3, ge=1, le=10)


class BackfillResponse(BaseModel):
    """Response from backfill operation."""
    task_id: str
    status: str
    companies_queued: int
    message: str


# Target companies for this case study
TARGET_COMPANIES = {
    "CAT": {"name": "Caterpillar Inc.", "sector": "Manufacturing", "industry": "Manufacturing", "domain": "caterpillar.com"},
    "DE": {"name": "Deere & Company", "sector": "Manufacturing", "industry": "Manufacturing", "domain": "deere.com"},
    "UNH": {"name": "UnitedHealth Group", "sector": "Healthcare", "industry": "Healthcare Services", "domain": "unitedhealthgroup.com"},
    "HCA": {"name": "HCA Healthcare", "sector": "Healthcare", "industry": "Healthcare Services", "domain": "hcahealthcare.com"},
    "ADP": {"name": "Automatic Data Processing", "sector": "Services", "industry": "Business Services", "domain": "adp.com"},
    "PAYX": {"name": "Paychex Inc.", "sector": "Services", "industry": "Business Services", "domain": "paychex.com"},
    "WMT": {"name": "Walmart Inc.", "sector": "Consumer", "industry": "Retail", "domain": "walmart.com"},
    "TGT": {"name": "Target Corporation", "sector": "Consumer", "industry": "Retail", "domain": "target.com"},
    "JPM": {"name": "JPMorgan Chase", "sector": "Financial", "industry": "Financial Services", "domain": "jpmorganchase.com"},
    "GS": {"name": "Goldman Sachs", "sector": "Financial", "industry": "Financial Services", "domain": "goldmansachs.com"},
}