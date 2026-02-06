"""Signal API endpoints for external data sources."""

import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from pydantic import BaseModel

from app.models.signal import (
    SignalCategory,
    SignalCollectionRequest,
    SignalCollectionResponse,
    ExternalSignalResponse,
    CompanySignalSummaryResponse,
    SignalSource,
)
from app.services.snowflake import SnowflakeService, get_snowflake_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["signals"])


# --- Response Models ---

class PaginatedSignals(BaseModel):
    items: list[ExternalSignalResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


# --- Signal Collection Endpoints ---

@router.post("/signals/collect", response_model=SignalCollectionResponse)
async def collect_signals(
    request: SignalCollectionRequest,
    background_tasks: BackgroundTasks,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Trigger signal collection for a company."""
    task_id = str(uuid4())
    
    background_tasks.add_task(
        _run_signal_collection,
        task_id=task_id,
        company_id=request.company_id,
        categories=request.categories,
        db=db
    )
    
    return SignalCollectionResponse(
        task_id=task_id,
        status="queued",
        message=f"Signal collection started for company {request.company_id}"
    )

@router.get("/signals", response_model=PaginatedSignals)
async def list_signals(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    company_id: Optional[UUID] = None,
    category: Optional[SignalCategory] = None,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """List signals with optional filtering."""
    offset = (page - 1) * page_size
    
    signals = db.get_signals(
        company_id=company_id,
        category=category.value if category else None,
        limit=page_size,
        offset=offset
    )
    
    total = db.count_signals(
        company_id=company_id,
        category=category.value if category else None
    )
    
    total_pages = (total + page_size - 1) // page_size
    
    return PaginatedSignals(
        items=[ExternalSignalResponse(**s) for s in signals],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages
    )


# --- Company Signal Endpoints ---

@router.get("/companies/{company_id}/signals", response_model=CompanySignalSummaryResponse)
async def get_company_signal_summary(
    company_id: UUID,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Get signal summary for a company."""
    summary = db.get_signal_summary(company_id)
    
    if not summary:
        # Return empty summary
        return CompanySignalSummaryResponse(
            company_id=company_id,
            ticker="UNKNOWN",
            technology_hiring_score=0,
            innovation_activity_score=0,
            digital_presence_score=0,
            leadership_signals_score=0,
            signal_count=0,
            last_updated=datetime.now(timezone.utc)
        )
    
    return CompanySignalSummaryResponse(**summary)


@router.get("/companies/{company_id}/signals/{category}", response_model=list[ExternalSignalResponse])
async def get_company_signals_by_category(
    company_id: UUID,
    category: SignalCategory,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Get signals for a company by category."""
    signals = db.get_signals(
        company_id=company_id,
        category=category.value
    )
    
    return [ExternalSignalResponse(**s) for s in signals]


# --- Background Tasks ---

def _run_signal_collection(
    task_id: str,
    company_id: UUID,
    categories: list[SignalCategory],
    db: SnowflakeService
):
    """Background task for signal collection using real API fetches."""
    logger.info(f"Starting signal collection task {task_id} for company {company_id}")

    try:
        from app.pipelines import (
            JobSignalCollector,
            DigitalPresenceCollector,
            PatentSignalCollector,
            LeadershipSignalCollector,
        )
        from app.models.evidence import TARGET_COMPANIES
        from app.config import get_settings

        company = db.get_company_by_id(company_id)
        if not company:
            logger.error(f"Task {task_id}: Company {company_id} not found")
            return

        ticker = (company.get("ticker") or "UNKNOWN").upper()
        name = company.get("name") or "Unknown"
        domain = TARGET_COMPANIES.get(ticker, {}).get("domain", "")
        settings = get_settings()

        job_collector = JobSignalCollector()
        digital_presence_collector = DigitalPresenceCollector()
        patent_collector = PatentSignalCollector()

        hiring_score = 0.0
        digital_score = 0.0
        innovation_score = 0.0
        signals_collected = 0

        if SignalCategory.TECHNOLOGY_HIRING in categories:
            company_info = TARGET_COMPANIES.get(ticker, {})
            careers_url = company_info.get("careers_url") if isinstance(company_info.get("careers_url"), str) else None
            postings = []
            if careers_url:
                postings.extend(job_collector.fetch_postings_from_careers_page(careers_url, name))
            serp_postings = job_collector.fetch_postings(name, api_key=settings.serpapi_key or None)
            if serp_postings:
                postings.extend(serp_postings)
            jobspy_postings = job_collector.fetch_postings_from_jobspy(name, location="United States", results_wanted=20)
            if jobspy_postings:
                postings.extend(jobspy_postings)
            postings = job_collector._dedupe_postings_by_title(postings) if postings else []
            used_careers = bool(careers_url)
            used_serp = bool(serp_postings)
            used_jobspy = bool(jobspy_postings)
            if postings:
                signal = job_collector.analyze_job_postings(name, postings, company_id)
                sources_used = []
                if used_careers:
                    sources_used.append("careers")
                if used_serp:
                    sources_used.append("serp")
                if used_jobspy:
                    sources_used.append("jobspy")
                if used_jobspy and not used_careers and not used_serp:
                    signal = signal.model_copy(update={"source": SignalSource.JOBSPY})
                elif used_careers and used_serp:
                    signal = signal.model_copy(update={"source": SignalSource.CAREERS_AND_SERP})
                elif used_careers:
                    signal = signal.model_copy(update={"source": SignalSource.CAREERS})
                if sources_used:
                    meta = dict(signal.metadata)
                    meta["sources_used"] = sources_used
                    signal = signal.model_copy(update={"metadata": meta})
                db.insert_signal(
                    company_id=company_id,
                    category=signal.category.value,
                    source=signal.source.value,
                    signal_date=signal.signal_date,
                    raw_value=signal.raw_value,
                    normalized_score=signal.normalized_score,
                    confidence=signal.confidence,
                    metadata=signal.metadata
                )
                hiring_score = signal.normalized_score
                signals_collected += 1

        if SignalCategory.DIGITAL_PRESENCE in categories:
            company_info = TARGET_COMPANIES.get(ticker, {})
            news_url = company_info.get("news_url") if isinstance(company_info.get("news_url"), str) else None
            dp_signals, digital_score = digital_presence_collector.collect(
                company_id=company_id,
                ticker=ticker,
                domain=domain,
                news_url=news_url,
                builtwith_api_key=settings.builtwith_api_key or None,
            )
            for sig in dp_signals:
                db.insert_signal(
                    company_id=company_id,
                    category=sig.category.value,
                    source=sig.source.value,
                    signal_date=sig.signal_date,
                    raw_value=sig.raw_value,
                    normalized_score=sig.normalized_score,
                    confidence=sig.confidence,
                    metadata=sig.metadata
                )
                signals_collected += 1

        if SignalCategory.INNOVATION_ACTIVITY in categories:
            patents = patent_collector.fetch_patents(name, api_key=settings.lens_api_key or None)
            if patents:
                signal = patent_collector.analyze_patents(company_id, patents)
                db.insert_signal(
                    company_id=company_id,
                    category=signal.category.value,
                    source=signal.source.value,
                    signal_date=signal.signal_date,
                    raw_value=signal.raw_value,
                    normalized_score=signal.normalized_score,
                    confidence=signal.confidence,
                    metadata=signal.metadata
                )
                innovation_score = signal.normalized_score
                signals_collected += 1

        leadership_score = 0.0
        if SignalCategory.LEADERSHIP_SIGNALS in categories:
            leadership_collector = LeadershipSignalCollector()
            company_info = TARGET_COMPANIES.get(ticker, {})
            leadership_url = company_info.get("leadership_url") if isinstance(company_info.get("leadership_url"), str) else None
            if leadership_url:
                website_data = leadership_collector.fetch_leadership_page(leadership_url)
            else:
                website_data = None
            if not website_data:
                website_data = leadership_collector.fetch_from_company_website(domain)
            leadership_signals = leadership_collector.analyze_leadership(
                company_id, website_data=website_data
            )
            for sig in leadership_signals:
                db.insert_signal(
                    company_id=company_id,
                    category=sig.category.value,
                    source=sig.source.value,
                    signal_date=sig.signal_date,
                    raw_value=sig.raw_value,
                    normalized_score=sig.normalized_score,
                    confidence=sig.confidence,
                    metadata=sig.metadata
                )
                leadership_score = max(leadership_score, sig.normalized_score)
                signals_collected += 1

        db.upsert_signal_summary(
            company_id=company_id,
            ticker=ticker,
            technology_hiring_score=hiring_score,
            innovation_activity_score=innovation_score,
            digital_presence_score=digital_score,
            leadership_signals_score=leadership_score,
            signal_count=signals_collected
        )
        logger.info(f"Task {task_id} completed: {signals_collected} signals collected")

    except Exception as e:
        logger.error(f"Task {task_id} failed: {e}")
