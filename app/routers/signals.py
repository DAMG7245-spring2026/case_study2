from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends, Query
from pydantic import BaseModel
from uuid import UUID, uuid4
from typing import List, Optional
from datetime import datetime, timezone

from app.models.signal import ExternalSignal, SignalCategory, CompanySignalSummary
from app.services.snowflake import get_snowflake_service, SnowflakeService
from app.pipelines.job_signals import JobSignalCollector
from app.pipelines.tech_signals import TechStackCollector
from app.pipelines.patent_signals import PatentSignalCollector

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])

class SignalCollectionRequest(BaseModel):
    company_id: UUID

class SignalCollectionResponse(BaseModel):
    task_id: str
    status: str
    message: str

async def run_signal_collection(
    task_id: str,
    company_id: UUID
):
    """Background task for signal collection."""
    db = get_snowflake_service()
    
    # Get company info
    company_query = "SELECT ticker, name FROM companies WHERE id = %s"
    company = db.execute_one(company_query, (str(company_id),))
    if not company:
        return

    ticker = company["ticker"]
    company_name = company["name"]
    
    # Collectors
    job_collector = JobSignalCollector()
    tech_collector = TechStackCollector()
    patent_collector = PatentSignalCollector()
    
    try:
        # 1. Job signals
        # In real app, we'd fetch actual postings here
        job_signal = job_collector.analyze_job_postings(
            company=company_name,
            postings=[] # Placeholder
        )
        job_signal.company_id = company_id
        await db.insert_signal(job_signal)
        
        # 2. Tech signals
        tech_signal = tech_collector.analyze_tech_stack(
            company_id=company_id,
            technologies=[] # Placeholder
        )
        await db.insert_signal(tech_signal)
        
        # 3. Patent signals
        patent_signal = patent_collector.analyze_patents(
            company_id=company_id,
            patents=[] # Placeholder
        )
        await db.insert_signal(patent_signal)
        
        # Update summary
        await db.update_signal_summary(company_id)
        
    except Exception as e:
        # Log error
        pass

@router.post("/collect", response_model=SignalCollectionResponse)
async def collect_signals(
    request: SignalCollectionRequest,
    background_tasks: BackgroundTasks
):
    """Trigger signal collection for a company."""
    task_id = str(uuid4())
    background_tasks.add_task(
        run_signal_collection,
        task_id=task_id,
        company_id=request.company_id
    )
    
    return SignalCollectionResponse(
        task_id=task_id,
        status="queued",
        message=f"Signal collection started for company {request.company_id}"
    )

@router.get("", response_model=List[ExternalSignal])
async def list_signals(
    company_id: Optional[UUID] = None,
    category: Optional[SignalCategory] = None,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """List signals with optional filtering."""
    query = "SELECT * FROM external_signals WHERE 1=1"
    params = []
    
    if company_id:
        query += " AND company_id = %s"
        params.append(str(company_id))
    
    if category:
        query += " AND category = %s"
        params.append(category.value)
        
    results = db.execute_query(query, tuple(params) if params else None)
    return [ExternalSignal(**r) for r in results]

# These are actually companies/{id}/signals but grouped here for logic
@router.get("/summary/{company_id}", response_model=CompanySignalSummary)
async def get_company_signal_summary(
    company_id: UUID,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Get aggregated signal summary for a company."""
    query = "SELECT * FROM company_signal_summaries WHERE company_id = %s"
    result = db.execute_one(query, (str(company_id),))
    
    if not result:
        raise HTTPException(status_code=404, detail="Signal summary not found")
        
    return CompanySignalSummary(**result)
