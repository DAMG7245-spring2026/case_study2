from fastapi import APIRouter, BackgroundTasks, HTTPException, Depends, Query
from pydantic import BaseModel
from uuid import UUID, uuid4
from typing import List, Optional
from datetime import datetime, timezone
from pathlib import Path

from app.models.document import DocumentRecord, DocumentStatus, DocumentChunkRecord
from app.services.snowflake import get_snowflake_service, SnowflakeService
from app.pipelines.sec_edgar import SECEdgarPipeline
from app.pipelines.document_parser import DocumentParser, SemanticChunker

router = APIRouter(prefix="/api/v1/documents", tags=["documents"])

class CollectionRequest(BaseModel):
    company_id: UUID
    filing_types: List[str] = ["10-K", "10-Q", "8-K"]
    years_back: int = 3

class CollectionResponse(BaseModel):
    task_id: str
    status: str
    message: str

async def run_document_collection(
    task_id: str,
    company_id: UUID,
    filing_types: List[str],
    years_back: int
):
    """Background task for document collection."""
    db = get_snowflake_service()
    
    # Get company info
    company_query = "SELECT ticker, name FROM companies WHERE id = %s"
    company = db.execute_one(company_query, (str(company_id),))
    if not company:
        return

    ticker = company["ticker"]
    
    pipeline = SECEdgarPipeline(
        company_name="PE-OrgAIR-Platform",
        email="your-email@university.edu"
    )
    parser = DocumentParser()
    chunker = SemanticChunker()
    
    after_date = f"{datetime.now().year - years_back}-01-01"
    
    try:
        filings = pipeline.download_filings(
            ticker=ticker,
            filing_types=filing_types,
            limit=10,
            after=after_date
        )
        
        for filing_path in filings:
            try:
                doc = parser.parse_filing(filing_path, ticker)
                doc_record = DocumentRecord(
                    company_id=company_id,
                    ticker=ticker,
                    filing_type=doc.filing_type,
                    filing_date=doc.filing_date,
                    source_url=str(filing_path),
                    local_path=str(filing_path),
                    content_hash=doc.content_hash,
                    word_count=doc.word_count,
                    status=DocumentStatus.PARSED
                )
                
                chunks = chunker.chunk_document(doc)
                doc_record.chunk_count = len(chunks)
                
                await db.insert_document(doc_record)
                await db.insert_chunks(chunks)
                
            except Exception as e:
                # Log error and continue
                pass
                
    except Exception as e:
        # Log error
        pass

@router.post("/collect", response_model=CollectionResponse)
async def collect_documents(
    request: CollectionRequest,
    background_tasks: BackgroundTasks
):
    """Trigger document collection for a company."""
    task_id = str(uuid4())
    background_tasks.add_task(
        run_document_collection,
        task_id=task_id,
        company_id=request.company_id,
        filing_types=request.filing_types,
        years_back=request.years_back
    )
    
    return CollectionResponse(
        task_id=task_id,
        status="queued",
        message=f"Document collection started for company {request.company_id}"
    )

@router.get("", response_model=List[DocumentRecord])
async def list_documents(
    company_id: Optional[UUID] = None,
    filing_type: Optional[str] = None,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """List documents, optionally filtered by company or type."""
    query = "SELECT * FROM documents WHERE 1=1"
    params = []
    
    if company_id:
        query += " AND company_id = %s"
        params.append(str(company_id))
    
    if filing_type:
        query += " AND filing_type = %s"
        params.append(filing_type)
        
    results = db.execute_query(query, tuple(params) if params else None)
    return [DocumentRecord(**r) for r in results]

@router.get("/{id}", response_model=DocumentRecord)
async def get_document(
    id: UUID,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Get document metadata by ID."""
    query = "SELECT * FROM documents WHERE id = %s"
    result = db.execute_one(query, (str(id),))
    
    if not result:
        raise HTTPException(status_code=404, detail="Document not found")
        
    return DocumentRecord(**result)

@router.get("/{id}/chunks", response_model=List[DocumentChunkRecord])
async def get_document_chunks(
    id: UUID,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Get all chunks for a specific document."""
    query = "SELECT * FROM document_chunks WHERE document_id = %s ORDER BY chunk_index"
    results = db.execute_query(query, (str(id),))
    return [DocumentChunkRecord(**r) for r in results]
