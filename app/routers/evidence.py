from fastapi import APIRouter, BackgroundTasks, Depends
from pydantic import BaseModel
from uuid import UUID
from typing import List, Dict, Any

from app.services.snowflake import get_snowflake_service, SnowflakeService

router = APIRouter(prefix="/api/v1/evidence", tags=["evidence"])

@router.get("/stats")
async def get_evidence_stats(
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Get overall evidence collection statistics."""
    doc_stats = db.execute_one("SELECT count(*) as count FROM documents")
    signal_stats = db.execute_one("SELECT count(*) as count FROM external_signals")
    company_stats = db.execute_one("SELECT count(distinct company_id) as count FROM documents")
    
    return {
        "total_documents": doc_stats["count"] if doc_stats else 0,
        "total_signals": signal_stats["count"] if signal_stats else 0,
        "companies_with_evidence": company_stats["count"] if company_stats else 0
    }

@router.get("/{company_id}")
async def get_company_evidence(
    company_id: UUID,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Get overall evidence collection statistics."""
    doc_stats = db.execute_one("SELECT count(*) as count FROM documents")
    signal_stats = db.execute_one("SELECT count(*) as count FROM external_signals")
    company_stats = db.execute_one("SELECT count(distinct company_id) as count FROM documents")
    
    return {
        "total_documents": doc_stats["count"] if doc_stats else 0,
        "total_signals": signal_stats["count"] if signal_stats else 0,
        "companies_with_evidence": company_stats["count"] if company_stats else 0
    }
