"""External signal models."""
from pydantic import BaseModel, Field, model_validator
from uuid import UUID, uuid4
from datetime import datetime, timezone
from enum import Enum


class SignalCategory(str, Enum):
    """Signal category types."""
    TECHNOLOGY_HIRING = "technology_hiring"
    INNOVATION_ACTIVITY = "innovation_activity"
    DIGITAL_PRESENCE = "digital_presence"
    LEADERSHIP_SIGNALS = "leadership_signals"


class SignalSource(str, Enum):
    """Signal data sources."""
    LINKEDIN = "linkedin"
    INDEED = "indeed"
    GLASSDOOR = "glassdoor"
    USPTO = "uspto"
    BUILTWITH = "builtwith"
    PRESS_RELEASE = "press_release"
    COMPANY_WEBSITE = "company_website"


class ExternalSignal(BaseModel):
    """A single external signal observation."""
    id: UUID = Field(default_factory=uuid4)
    company_id: UUID
    category: SignalCategory
    source: SignalSource
    signal_date: datetime
    raw_value: str
    normalized_score: float = Field(ge=0, le=100)
    confidence: float = Field(default=0.8, ge=0, le=1)
    metadata: dict = Field(default_factory=dict)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    class Config:
        from_attributes = True


class CompanySignalSummary(BaseModel):
    """Aggregated signals for a company."""
    company_id: UUID
    ticker: str
    technology_hiring_score: float = Field(ge=0, le=100)
    innovation_activity_score: float = Field(ge=0, le=100)
    digital_presence_score: float = Field(ge=0, le=100)
    leadership_signals_score: float = Field(ge=0, le=100)
    composite_score: float = Field(ge=0, le=100)
    signal_count: int
    last_updated: datetime

    @model_validator(mode='after')
    def calculate_composite(self) -> 'CompanySignalSummary':
        """Calculate weighted composite score."""
        self.composite_score = (
            0.30 * self.technology_hiring_score +
            0.25 * self.innovation_activity_score +
            0.25 * self.digital_presence_score +
            0.20 * self.leadership_signals_score
        )
        return self

    class Config:
        from_attributes = True
