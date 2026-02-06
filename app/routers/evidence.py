"""Evidence API endpoints combining documents and signals."""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from app.models.evidence import (
    CompanyEvidence,
    EvidenceStats,
    BackfillRequest,
    BackfillResponse,
    TARGET_COMPANIES,
)
from app.models.document import DocumentResponse
from app.models.signal import CompanySignalSummaryResponse, ExternalSignalResponse
from app.services.snowflake import SnowflakeService, get_snowflake_service
from app.services.s3_storage import S3Storage, get_s3_storage

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["evidence"])


@router.get("/companies/{company_id}/evidence", response_model=CompanyEvidence)
async def get_company_evidence(
    company_id: UUID,
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Get all evidence (documents + signals) for a company."""
    # Get documents
    docs = db.get_documents(company_id=company_id, limit=100)
    
    # Count chunks
    chunk_count = sum(db.count_chunks(d["id"]) for d in docs)
    
    # Get signals
    signals = db.get_signals(company_id=company_id, limit=100)
    
    # Get signal summary
    summary_data = db.get_signal_summary(company_id)
    signal_summary = CompanySignalSummaryResponse(**summary_data) if summary_data else None
    
    # Get company info
    ticker = docs[0]["ticker"] if docs else "UNKNOWN"
    company_name = "Unknown"
    
    for t, info in TARGET_COMPANIES.items():
        if t == ticker:
            company_name = info["name"]
            break
    
    return CompanyEvidence(
        company_id=company_id,
        ticker=ticker,
        company_name=company_name,
        document_count=len(docs),
        chunk_count=chunk_count,
        documents=[DocumentResponse(**d) for d in docs],
        signal_summary=signal_summary,
        signals=[ExternalSignalResponse(**s) for s in signals],
        last_updated=datetime.now(timezone.utc)
    )


@router.post("/evidence/backfill", response_model=BackfillResponse)
async def backfill_evidence(
    request: BackfillRequest,
    background_tasks: BackgroundTasks,
    db: SnowflakeService = Depends(get_snowflake_service),
    s3: S3Storage = Depends(get_s3_storage)
):
    """Backfill evidence for multiple companies."""
    task_id = str(uuid4())
    
    # Determine which companies to process
    if request.tickers:
        tickers = [t.upper() for t in request.tickers if t.upper() in TARGET_COMPANIES]
    else:
        tickers = list(TARGET_COMPANIES.keys())
    
    if not tickers:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid tickers provided"
        )
    
    background_tasks.add_task(
        _run_backfill,
        task_id=task_id,
        tickers=tickers,
        include_documents=request.include_documents,
        include_signals=request.include_signals,
        years_back=request.years_back,
        db=db,
        s3=s3
    )
    
    return BackfillResponse(
        task_id=task_id,
        status="queued",
        companies_queued=len(tickers),
        message=f"Backfill started for {len(tickers)} companies: {', '.join(tickers)}"
    )


@router.get("/evidence/stats", response_model=EvidenceStats)
async def get_evidence_stats(
    db: SnowflakeService = Depends(get_snowflake_service)
):
    """Get statistics about evidence collection."""
    stats = db.get_evidence_stats()
    
    return EvidenceStats(
        total_companies=stats.get("total_companies", 0),
        total_documents=stats.get("total_documents", 0),
        total_chunks=stats.get("total_chunks", 0),
        total_signals=stats.get("total_signals", 0),
        documents_by_type=stats.get("documents_by_type", {}),
        documents_by_status=stats.get("documents_by_status", {}),
        signals_by_category=stats.get("signals_by_category", {}),
        companies_with_documents=stats.get("companies_with_documents", 0),
        companies_with_signals=stats.get("companies_with_signals", 0),
        last_collection_time=datetime.now(timezone.utc)
    )


@router.get("/target-companies")
async def get_target_companies():
    """Get list of target companies for this case study."""
    return {
        "companies": [
            {"ticker": ticker, **info}
            for ticker, info in TARGET_COMPANIES.items()
        ],
        "count": len(TARGET_COMPANIES)
    }


# --- Background Tasks ---

def _run_backfill(
    task_id: str,
    tickers: list[str],
    include_documents: bool,
    include_signals: bool,
    years_back: int,
    db: SnowflakeService,
    s3: S3Storage
):
    """Background task for evidence backfill with S3 support."""
    logger.info(f"Starting backfill task {task_id} for {len(tickers)} companies")
    
    from app.pipelines import (
        SECEdgarPipeline, DocumentParser, SemanticChunker,
        JobSignalCollector, DigitalPresenceCollector, PatentSignalCollector,
        LeadershipSignalCollector,
    )
    from app.config import get_settings

    settings = get_settings()
    stats = {"companies": 0, "documents": 0, "chunks": 0, "signals": 0, "s3_uploads": 0, "errors": 0}
    
    for ticker in tickers:
        try:
            company_info = TARGET_COMPANIES[ticker]
            logger.info(f"Task {task_id}: Processing {ticker} - {company_info['name']}")
            
            # Get or create company
            industry_result = db.execute_one(
                "SELECT id FROM industries WHERE name = %s",
                (company_info["industry"],)
            )
            
            if not industry_result:
                logger.warning(f"Task {task_id}: Industry {company_info['industry']} not found, skipping {ticker}")
                stats["errors"] += 1
                continue
            
            industry_id = UUID(industry_result["id"])
            
            company = db.get_or_create_company(
                ticker=ticker,
                name=company_info["name"],
                industry_id=industry_id
            )
            company_id = UUID(company["id"])
            
            # ========== DOCUMENTS ==========
            if include_documents:
                try:
                    email = getattr(settings, 'sec_edgar_email', 'student@university.edu')
                    pipeline = SECEdgarPipeline(
                        company_name="PE-OrgAIR-Platform",
                        email=email
                    )
                    parser = DocumentParser()
                    chunker = SemanticChunker()
                    
                    filings = pipeline.download_filings(
                        ticker=ticker,
                        filing_types=["10-K", "10-Q", "8-K"],
                        limit=5,
                        after=f"{datetime.now().year - years_back}-01-01"
                    )
                    
                    logger.info(f"Task {task_id}: Downloaded {len(filings)} filings for {ticker}")
                    
                    for filing_path in filings:
                        try:
                            filing_path = Path(filing_path)
                            parsed = parser.parse_filing(filing_path, ticker)
                            
                            # Check duplicate
                            existing = db.execute_one(
                                "SELECT id FROM documents WHERE content_hash = %s",
                                (parsed.content_hash,)
                            )
                            if existing:
                                logger.info(f"Task {task_id}: Skipping duplicate {parsed.filing_type}")
                                continue
                            
                            # Upload to S3
                            filing_date_str = parsed.filing_date.strftime("%Y-%m-%d")
                            s3_key = s3.upload_sec_filing(
                                ticker=ticker,
                                filing_type=parsed.filing_type,
                                filing_date=filing_date_str,
                                local_path=filing_path,
                                content_hash=parsed.content_hash
                            )
                            
                            if s3_key:
                                stats["s3_uploads"] += 1
                            
                            # Insert document
                            doc_id = db.insert_document(
                                company_id=company_id,
                                ticker=ticker,
                                filing_type=parsed.filing_type,
                                filing_date=parsed.filing_date,
                                content_hash=parsed.content_hash,
                                word_count=parsed.word_count,
                                local_path=str(filing_path),
                                s3_key=s3_key,
                                status="parsed"
                            )
                            
                            # Chunk and insert
                            chunks = chunker.chunk_document(parsed)
                            chunk_dicts = [
                                {
                                    "chunk_index": c.chunk_index,
                                    "content": c.content,
                                    "section": c.section,
                                    "start_char": c.start_char,
                                    "end_char": c.end_char,
                                    "word_count": c.word_count
                                }
                                for c in chunks
                            ]
                            
                            db.insert_chunks(doc_id, chunk_dicts)
                            db.update_document_status(doc_id, "chunked", chunk_count=len(chunks))
                            
                            stats["documents"] += 1
                            stats["chunks"] += len(chunks)
                            logger.info(f"Task {task_id}: Processed {parsed.filing_type}: {len(chunks)} chunks")
                            
                        except Exception as e:
                            logger.error(f"Task {task_id}: Error processing {filing_path}: {e}")
                            stats["errors"] += 1
                            
                except Exception as e:
                    logger.error(f"Task {task_id}: Error downloading documents for {ticker}: {e}")
                    stats["errors"] += 1
            
            # ========== SIGNALS ==========
            if include_signals:
                try:
                    job_collector = JobSignalCollector()
                    digital_presence_collector = DigitalPresenceCollector()
                    patent_collector = PatentSignalCollector()
                    domain = company_info.get("domain") or ""

                    hiring_score = 0.0
                    digital_score = 0.0
                    innovation_score = 0.0
                    signals_collected = 0

                    # Job signals (SerpAPI)
                    postings = job_collector.fetch_postings(company_info["name"], api_key=settings.serpapi_key or None)
                    if postings:
                        job_signal = job_collector.analyze_job_postings(company_info["name"], postings, company_id)
                        db.insert_signal(
                            company_id=company_id,
                            category=job_signal.category.value,
                            source=job_signal.source.value,
                            signal_date=job_signal.signal_date,
                            raw_value=job_signal.raw_value,
                            normalized_score=job_signal.normalized_score,
                            confidence=job_signal.confidence,
                            metadata=job_signal.metadata
                        )
                        hiring_score = job_signal.normalized_score
                        signals_collected += 1

                    # Digital presence (BuiltWith + company news)
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

                    # Patent signals (Lens)
                    patents = patent_collector.fetch_patents(company_info["name"], api_key=settings.lens_api_key or None)
                    if patents:
                        patent_signal = patent_collector.analyze_patents(company_id, patents)
                        db.insert_signal(
                            company_id=company_id,
                            category=patent_signal.category.value,
                            source=patent_signal.source.value,
                            signal_date=patent_signal.signal_date,
                            raw_value=patent_signal.raw_value,
                            normalized_score=patent_signal.normalized_score,
                            confidence=patent_signal.confidence,
                            metadata=patent_signal.metadata
                        )
                        innovation_score = patent_signal.normalized_score
                        signals_collected += 1

                    # Leadership signals (leadership_url first, then company website fallback)
                    leadership_collector = LeadershipSignalCollector()
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
                    if not leadership_signals:
                        logger.info(
                            f"Task {task_id}: No leadership data for {ticker} (domain={domain!r}); "
                            "check logs for leadership_fetch_no_page if website fetch failed."
                        )
                    leadership_score = 0.0
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
                    stats["signals"] += signals_collected
                    logger.info(f"Task {task_id}: Collected {signals_collected} signals for {ticker}")

                except Exception as e:
                    logger.error(f"Task {task_id}: Error collecting signals for {ticker}: {e}")
                    stats["errors"] += 1
            
            stats["companies"] += 1
            
        except Exception as e:
            logger.error(f"Task {task_id}: Error processing {ticker}: {e}")
            stats["errors"] += 1
    
    logger.info(f"Task {task_id} completed: {stats}")
