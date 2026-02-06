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


# Target companies for this case study (careers_url and leadership_url used for signal collection)
TARGET_COMPANIES = {
    "CAT": {
        "name": "Caterpillar Inc.",
        "sector": "Manufacturing",
        "industry": "Manufacturing",
        "domain": "caterpillar.com",
        "careers_url": "https://careers.caterpillar.com/en/jobs/?search=&country=United+States+of+America#results",
        "leadership_url": "https://www.caterpillar.com/en/company/governance/officers.html",
    },
    "DE": {
        "name": "Deere & Company",
        "sector": "Manufacturing",
        "industry": "Manufacturing",
        "domain": "deere.com",
        "careers_url": "https://careers.deere.com/careers?location=united%20states",
        "leadership_url": "https://about.deere.com/en-us/explore-john-deere/leadership",
    },
    "UNH": {
        "name": "UnitedHealth Group",
        "sector": "Healthcare",
        "industry": "Healthcare Services",
        "domain": "unitedhealthgroup.com",
        "careers_url": "https://careers.unitedhealthgroup.com/job-search-results/",
        "leadership_url": None,
    },
    "HCA": {
        "name": "HCA Healthcare",
        "sector": "Healthcare",
        "industry": "Healthcare Services",
        "domain": "hcahealthcare.com",
        "careers_url": "https://careers.hcahealthcare.com/search/jobs?q=AI&location=&ns_radius=40.2336&ns_from_search=1",
        "leadership_url": "https://careers.hcahealthcare.com/pages/executive",
    },
    "ADP": {
        "name": "Automatic Data Processing",
        "sector": "Services",
        "industry": "Business Services",
        "domain": "adp.com",
        "careers_url": "https://jobs.adp.com/en/jobs/?search=&mylocation=United+States&origin=global&lat=38.7945952&lng=-106.5348379&origin=global",
        "leadership_url": "https://www.adp.com/about-adp/leadership.aspx",
    },
    "PAYX": {
        "name": "Paychex Inc.",
        "sector": "Services",
        "industry": "Business Services",
        "domain": "paychex.com",
        "careers_url": "https://careers.paychex.com/careers/jobs?stretchUnit=MILES&stretch=10&location=United%20States&woe=12&regionCode=US&sortBy=relevance&page=1",
        "leadership_url": None,
    },
    "WMT": {
        "name": "Walmart Inc.",
        "sector": "Consumer",
        "industry": "Retail",
        "domain": "walmart.com",
        "careers_url": "https://careers.walmart.com/us/en/results?searchQuery=united+states",
        "leadership_url": "https://corporate.walmart.com/about/leadership",
    },
    "TGT": {
        "name": "Target Corporation",
        "sector": "Consumer",
        "industry": "Retail",
        "domain": "target.com",
        "careers_url": "https://corporate.target.com/careers/job-search?currentPage=1&country=United%20States",
        "leadership_url": "https://corporate.target.com/about/leadership-team",
    },
    "JPM": {
        "name": "JPMorgan Chase",
        "sector": "Financial",
        "industry": "Financial Services",
        "domain": "jpmorganchase.com",
        "careers_url": "https://jpmc.fa.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1001/jobs?location=United+States&locationId=300000000289738&locationLevel=country&mode=location",
        "leadership_url": "https://www.jpmorganchase.com/about/leadership",
    },
    "GS": {
        "name": "Goldman Sachs",
        "sector": "Financial",
        "industry": "Financial Services",
        "domain": "goldmansachs.com",
        "careers_url": "https://higher.gs.com/results?LOCATION=Albany|New%20York|Atlanta|Boston|Chicago|Dallas|Houston|Irving|Richardson|Detroit|Draper|Salt%20Lake%20City|Jersey%20City|Menlo%20Park|Newport%20Beach|San%20Francisco|Miami|West%20Palm%20Beach|Philadelphia|Pittsburgh|Seattle|Washington|Wilmington&page=1&sort=RELEVANCE",
        "leadership_url": "https://www.goldmansachs.com/our-firm/our-people-and-leadership/leadership",
    },
}