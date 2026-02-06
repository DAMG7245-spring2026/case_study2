"""Signal models for external data sources."""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


class SignalCategory(str, Enum):
    """Categories of external signals."""
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"


class SignalSource(str, Enum):
    """Sources of external signals."""
    LINKEDIN = "linkedin"
    INDEED = "indeed"
    GLASSDOOR = "glassdoor"
    USPTO = "uspto"
    LENS = "lens"
    BUILTWITH = "builtwith"
    WAPPALYZER = "wappalyzer"
    SIMILARTECH = "similartech"
    PRESS_RELEASE = "press_release"
    COMPANY_WEBSITE = "company_website"
    CAREERS = "careers"
    CAREERS_AND_SERP = "careers_and_serp"
    COMPANY_NEWS = "company_news"


# --- Base Models ---

class ExternalSignalBase(BaseModel):
    """Base external signal model."""
    company_id: UUID
    category: SignalCategory
    source: SignalSource
    signal_date: datetime
    raw_value: str = Field(..., max_length=500)
    normalized_score: float = Field(..., ge=0, le=100)
    confidence: float = Field(default=0.8, ge=0, le=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CompanySignalSummaryBase(BaseModel):
    """Base signal summary model."""
    company_id: UUID
    ticker: str = Field(..., max_length=10)
    technology_hiring_score: float = Field(default=0, ge=0, le=100)
    innovation_activity_score: float = Field(default=0, ge=0, le=100)
    digital_presence_score: float = Field(default=0, ge=0, le=100)
    leadership_signals_score: float = Field(default=0, ge=0, le=100)
    signal_count: int = Field(default=0, ge=0)


# --- Create Models ---

class ExternalSignalCreate(ExternalSignalBase):
    """Model for creating an external signal."""
    pass


class CompanySignalSummaryCreate(CompanySignalSummaryBase):
    """Model for creating a signal summary."""
    pass


# --- Response Models ---

class ExternalSignalResponse(ExternalSignalBase):
    """Response model for external signal."""
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True


class CompanySignalSummaryResponse(CompanySignalSummaryBase):
    """Response model for signal summary."""
    composite_score: float = Field(default=0, ge=0, le=100)
    last_updated: datetime

    @model_validator(mode='after')
    def calculate_composite(self) -> 'CompanySignalSummaryResponse':
        """Calculate weighted composite score."""
        self.composite_score = round(
            0.30 * self.technology_hiring_score +
            0.25 * self.innovation_activity_score +
            0.25 * self.digital_presence_score +
            0.20 * self.leadership_signals_score,
            2
        )
        return self

    class Config:
        from_attributes = True


# --- Request Models ---

class SignalCollectionRequest(BaseModel):
    """Request to trigger signal collection."""
    company_id: UUID
    categories: list[SignalCategory] = Field(
        default=[
            SignalCategory.TECHNOLOGY_HIRING,
            SignalCategory.INNOVATION_ACTIVITY,
            SignalCategory.DIGITAL_PRESENCE,
            SignalCategory.LEADERSHIP_SIGNALS,
        ]
    )


class SignalCollectionResponse(BaseModel):
    """Response from signal collection trigger."""
    task_id: str
    status: str
    message: str


# --- Helper Data Classes ---

class JobPosting(BaseModel):
    """Represents a job posting."""
    title: str
    company: str
    location: str
    description: str
    posted_date: Optional[str] = None
    source: str
    url: str
    is_ai_related: bool = False
    ai_skills: list[str] = Field(default_factory=list)


class TechnologyDetection(BaseModel):
    """A detected technology."""
    name: str
    category: str
    is_ai_related: bool = False
    confidence: float = Field(default=0.8, ge=0, le=1)


class Patent(BaseModel):
    """A patent record."""
    patent_number: str
    title: str
    abstract: str
    filing_date: datetime
    grant_date: Optional[datetime] = None
    inventors: list[str] = Field(default_factory=list)
    assignee: str
    is_ai_related: bool = False
    ai_categories: list[str] = Field(default_factory=list)