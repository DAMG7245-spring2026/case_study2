"""Report API: external signals report with all four scores and composite per rubric."""
from datetime import datetime, timezone
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.services.snowflake import get_snowflake_service, SnowflakeService

router = APIRouter(prefix="/api/v1/report", tags=["report"])

# Weights per rubric: tech 0.30, innovation 0.25, digital 0.25, leadership 0.20
W_TECH, W_INNOVATION, W_DIGITAL, W_LEADERSHIP = 0.30, 0.25, 0.25, 0.20


class CompanyReportRow(BaseModel):
    """One company's row in the external signals report."""
    company_id: str
    ticker: str
    company_name: str
    technology_hiring_score: float = Field(ge=0, le=100)
    innovation_activity_score: float = Field(ge=0, le=100)
    digital_presence_score: float = Field(ge=0, le=100)
    leadership_signals_score: float = Field(ge=0, le=100)
    composite_score: float = Field(ge=0, le=100)
    signal_count: int
    last_updated: datetime | None


class ReportResponse(BaseModel):
    """Full report: companies and metadata. Composite uses all four scores per rubric (30/25/25/20)."""
    generated_at: datetime
    note: str = "Composite uses all four scores per rubric: Tech Hiring (30%), Innovation (25%), Digital (25%), Leadership (20%)."
    companies: list[CompanyReportRow]


@router.get("", response_model=ReportResponse)
async def get_external_signals_report(
    db: SnowflakeService = Depends(get_snowflake_service),
):
    """Get external signals report for all companies. Composite uses all four scores per rubric (30/25/25/20)."""
    query = """
        SELECT s.company_id, s.ticker, s.technology_hiring_score, s.innovation_activity_score,
               s.digital_presence_score, s.leadership_signals_score, s.signal_count, s.last_updated,
               c.name AS company_name
        FROM company_signal_summaries s
        LEFT JOIN companies c ON c.id = s.company_id
        ORDER BY s.ticker
    """
    rows = db.execute_query(query)
    companies = []
    for r in rows:
        th = float(r.get("technology_hiring_score") or 0)
        ia = float(r.get("innovation_activity_score") or 0)
        dp = float(r.get("digital_presence_score") or 0)
        lead = float(r.get("leadership_signals_score") or 0)
        composite_score = round(
            W_TECH * th + W_INNOVATION * ia + W_DIGITAL * dp + W_LEADERSHIP * lead, 1
        )
        companies.append(
            CompanyReportRow(
                company_id=str(r["company_id"]),
                ticker=r.get("ticker") or "",
                company_name=r.get("company_name") or r.get("ticker") or "—",
                technology_hiring_score=th,
                innovation_activity_score=ia,
                digital_presence_score=dp,
                leadership_signals_score=lead,
                composite_score=composite_score,
                signal_count=int(r.get("signal_count") or 0),
                last_updated=r.get("last_updated"),
            )
        )
    return ReportResponse(
        generated_at=datetime.now(timezone.utc),
        companies=companies,
    )
