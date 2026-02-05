#!/usr/bin/env python
"""
Collect evidence for all target companies.

Usage:
    python scripts/collect_evidence.py --companies all
    python scripts/collect_evidence.py --companies CAT,DE,UNH
"""

import argparse
import asyncio
from datetime import datetime, timezone
from typing import Any
import structlog
from pathlib import Path
import sys

# Add parent directory to path
_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

# Load .env from project root so API keys are available regardless of cwd
from dotenv import load_dotenv
load_dotenv(_project_root / ".env")

from app.pipelines.sec_edgar import SECEdgarPipeline
from app.pipelines.document_parser import DocumentParser, SemanticChunker
from app.pipelines.job_signals import JobSignalCollector
from app.pipelines.tech_signals import TechStackCollector
from app.pipelines.patent_signals import PatentSignalCollector
from app.config import get_settings
from app.services.snowflake import SnowflakeService
from app.models.document import DocumentRecord, DocumentStatus

logger = structlog.get_logger()

TARGET_COMPANIES = {
    "CAT": {"name": "Caterpillar Inc.", "sector": "Manufacturing", "domain": "caterpillar.com"},
    "DE": {"name": "Deere & Company", "sector": "Manufacturing", "domain": "deere.com"},
    "UNH": {"name": "UnitedHealth Group", "sector": "Healthcare", "domain": "unitedhealthgroup.com"},
    "HCA": {"name": "HCA Healthcare", "sector": "Healthcare", "domain": "hcahealthcare.com"},
    "ADP": {"name": "Automatic Data Processing", "sector": "Services", "domain": "adp.com"},
    "PAYX": {"name": "Paychex Inc.", "sector": "Services", "domain": "paychex.com"},
    "WMT": {"name": "Walmart Inc.", "sector": "Retail", "domain": "walmart.com"},
    "TGT": {"name": "Target Corporation", "sector": "Retail", "domain": "target.com"},
    "JPM": {"name": "JPMorgan Chase", "sector": "Financial", "domain": "jpmorganchase.com"},
    "GS": {"name": "Goldman Sachs", "sector": "Financial", "domain": "goldmansachs.com"},
}


async def collect_documents(ticker: str, company_id: Any, db: SnowflakeService, pipeline: SECEdgarPipeline):
    """Collect SEC documents for a company with rate limiting."""
    logger.info("collecting_documents", ticker=ticker)

    parser = DocumentParser()
    chunker = SemanticChunker()

    try:
        # Download filings with rate limiting and retry
        filings = pipeline.download_filings(
            ticker=ticker,
            filing_types=["10-K", "10-Q", "8-K"],
            limit=10,
            after="2021-01-01",
            max_retries=3  # Retry up to 3 times on failure
        )

        logger.info(
            "downloaded_filings",
            ticker=ticker,
            count=len(filings)
        )

        # Parse and chunk each filing
        total_chunks = 0
        processed_docs = 0

        for filing_path in filings:
            try:
                doc = parser.parse_filing(filing_path, ticker)
                chunks = chunker.chunk_document(doc)

                # Store in database
                doc_record = DocumentRecord(
                    company_id=company_id,
                    ticker=ticker,
                    filing_type=doc.filing_type,
                    filing_date=doc.filing_date,
                    source_url=str(filing_path),
                    local_path=str(filing_path),
                    content_hash=doc.content_hash,
                    word_count=doc.word_count,
                    chunk_count=len(chunks),
                    status=DocumentStatus.PARSED
                )
                
                doc_id = await db.insert_document(doc_record)
                await db.insert_chunks(doc_id, chunks)

                total_chunks += len(chunks)
                processed_docs += 1

                logger.info(
                    "processed_document",
                    ticker=ticker,
                    filing_type=doc.filing_type,
                    chunks=len(chunks)
                )
            except Exception as e:
                logger.error(
                    "failed_to_process_document",
                    ticker=ticker,
                    path=str(filing_path),
                    error=str(e)
                )
                continue

        return len(filings), total_chunks, processed_docs

    except Exception as e:
        logger.error(
            "failed_to_collect_documents",
            ticker=ticker,
            error=str(e)
        )
        return 0, 0, 0


async def collect_signals(ticker: str, company_id: Any, db: SnowflakeService):
    """Collect external signals for a company. Skips a source when no API key or no data."""
    logger.info("Collecting signals", ticker=ticker)
    settings = get_settings()
    signals = []
    company_name = TARGET_COMPANIES[ticker]["name"]
    domain = TARGET_COMPANIES[ticker].get("domain") or ""

    # Job postings: fetch only when API key is set; skip if no data
    job_collector = JobSignalCollector()
    postings = job_collector.fetch_postings(company_name, api_key=settings.serpapi_key or None)
    if postings:
        job_signal = job_collector.analyze_job_postings(company=company_name, company_id=company_id, postings=postings)
        signals.append(job_signal)
    else:
        logger.debug("job_signals_skipped", ticker=ticker, reason="no_data_or_no_key")

    # Technology stack: fetch only when API key and domain are set; skip if no data
    tech_collector = TechStackCollector()
    technologies = tech_collector.fetch_tech_stack(domain, api_key=settings.builtwith_api_key or None) if domain else []
    if technologies:
        tech_signal = tech_collector.analyze_tech_stack(company_id=company_id, technologies=technologies)
        signals.append(tech_signal)
    else:
        logger.debug("tech_signals_skipped", ticker=ticker, reason="no_data_or_no_key")

    # Patents: fetch from Lens.org when API key is set; skip if no data
    patent_collector = PatentSignalCollector()
    patents = patent_collector.fetch_patents(company_name, api_key=settings.lens_api_key or None)
    if patents:
        patent_signal = patent_collector.analyze_patents(company_id=company_id, patents=patents)
        signals.append(patent_signal)
    else:
        logger.debug("patent_signals_skipped", ticker=ticker, reason="no_data_or_no_key")

    # Store only signals we have; skip when we have none
    for signal in signals:
        await db.insert_signal(signal)

    if signals:
        await db.update_signal_summary(company_id)

    logger.info("Signal collection complete", ticker=ticker, count=len(signals))
    return len(signals)


async def main(companies: list[str], use_batch: bool = True, signals_only: bool = False):
    """Main collection routine with rate-limited batch processing."""
    stats = {
        "companies": 0,
        "documents": 0,
        "chunks": 0,
        "processed_docs": 0,
        "signals": 0,
        "errors": 0
    }

    db = SnowflakeService()
    pipeline = SECEdgarPipeline(
        company_name="Northeastern University",
        email="tu.wei@northeastern.edu",
        download_dir=Path("data/raw/sec"),
        rate_limit_buffer=0.1,
    )
    logger.info(
        "pipeline_initialized",
        rate_limit_buffer=pipeline.rate_limit_buffer,
        max_requests_per_second=pipeline.MAX_REQUESTS_PER_SECOND,
    )

    # Signals-only mode: collect external signals for all companies, skip SEC documents
    if signals_only:
        logger.info("signals_only_mode", company_count=len(companies))
        valid_tickers = [t for t in companies if t in TARGET_COMPANIES]
        for ticker in valid_tickers:
            try:
                company = await db.get_or_create_company(
                    ticker=ticker,
                    name=TARGET_COMPANIES[ticker]["name"],
                    sector=TARGET_COMPANIES[ticker]["sector"],
                )
                signal_count = await collect_signals(ticker, company.id, db)
                stats["companies"] += 1
                stats["signals"] += signal_count
            except Exception as e:
                logger.error("failed_to_process_company", ticker=ticker, error=str(e))
                stats["errors"] += 1
        pipeline_stats = pipeline.get_stats()
        logger.info("collection_complete", **stats, total_api_requests=pipeline_stats["total_requests"], rate_limit_hits=pipeline_stats["rate_limit_hits"])
        return stats, pipeline_stats

    # Option 1: Batch download (faster, uses download_batch method)
    if use_batch and len(companies) > 1:
        logger.info("using_batch_download_mode", company_count=len(companies))

        # Download all documents in one batch
        valid_tickers = [t for t in companies if t in TARGET_COMPANIES]

        try:
            results = pipeline.download_batch(
                tickers=valid_tickers,
                filing_types=["10-K", "10-Q", "8-K"],
                limit=10,
                after="2021-01-01",
                delay_between_tickers=1.0  # 1 second between companies
            )

            # Process each company's documents
            for ticker in valid_tickers:
                try:
                    logger.info(
                        "processing_company",
                        ticker=ticker,
                        name=TARGET_COMPANIES[ticker]["name"],
                        sector=TARGET_COMPANIES[ticker]["sector"]
                    )

                    # Get or create company in database
                    company = await db.get_or_create_company(
                        ticker=ticker,
                        name=TARGET_COMPANIES[ticker]["name"],
                        sector=TARGET_COMPANIES[ticker]["sector"]
                    )

                    filings = results.get(ticker, [])
                    if not filings:
                        logger.warning("no_filings_downloaded", ticker=ticker)
                    
                    # Parse and chunk documents
                    parser = DocumentParser()
                    chunker = SemanticChunker()
                    total_chunks = 0
                    processed_docs = 0

                    for filing_path in filings:
                        try:
                            doc = parser.parse_filing(filing_path, ticker)
                            chunks = chunker.chunk_document(doc)

                            # Store in database
                            doc_record = DocumentRecord(
                                company_id=company.id,
                                ticker=ticker,
                                filing_type=doc.filing_type,
                                filing_date=doc.filing_date,
                                source_url=str(filing_path),
                                local_path=str(filing_path),
                                content_hash=doc.content_hash,
                                word_count=doc.word_count,
                                chunk_count=len(chunks),
                                status=DocumentStatus.PARSED
                            )
                            
                            doc_id = await db.insert_document(doc_record)
                            await db.insert_chunks(doc_id, chunks)

                            total_chunks += len(chunks)
                            processed_docs += 1

                        except Exception as e:
                            logger.error(
                                "failed_to_process_document",
                                ticker=ticker,
                                path=str(filing_path),
                                error=str(e)
                            )
                            continue

                    # Collect signals
                    signal_count = await collect_signals(ticker, company.id, db)

                    # Update stats
                    stats["companies"] += 1
                    stats["documents"] += len(filings)
                    stats["chunks"] += total_chunks
                    stats["processed_docs"] += processed_docs
                    stats["signals"] += signal_count

                except Exception as e:
                    logger.error("failed_to_process_company", ticker=ticker, error=str(e))
                    stats["errors"] += 1

        except Exception as e:
            logger.error("batch_download_failed", error=str(e))
            stats["errors"] += len(valid_tickers)

    # Option 2: Sequential download (one at a time)
    else:
        logger.info("using_sequential_download_mode", company_count=len(companies))

        for ticker in companies:
            if ticker not in TARGET_COMPANIES:
                logger.warning("unknown_ticker", ticker=ticker)
                continue

            try:
                logger.info(
                    "processing_company",
                    ticker=ticker,
                    name=TARGET_COMPANIES[ticker]["name"],
                    sector=TARGET_COMPANIES[ticker]["sector"]
                )

                # Get or create company in database
                company = await db.get_or_create_company(
                    ticker=ticker,
                    name=TARGET_COMPANIES[ticker]["name"],
                    sector=TARGET_COMPANIES[ticker]["sector"]
                )

                # Collect documents
                doc_count, chunk_count, processed_docs = await collect_documents(ticker, company.id, db, pipeline)
                stats["documents"] += doc_count
                stats["chunks"] += chunk_count
                stats["processed_docs"] += processed_docs

                # Collect signals
                signal_count = await collect_signals(ticker, company.id, db)
                stats["signals"] += signal_count

                stats["companies"] += 1

            except Exception as e:
                logger.error("failed_to_process_company", ticker=ticker, error=str(e))
                stats["errors"] += 1

    # Get pipeline statistics
    pipeline_stats = pipeline.get_stats()
    logger.info(
        "collection_complete",
        **stats,
        total_api_requests=pipeline_stats["total_requests"],
        rate_limit_hits=pipeline_stats["rate_limit_hits"]
    )

    return stats, pipeline_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect evidence for target companies with rate-limited downloads"
    )
    parser.add_argument(
        "--companies",
        default="all",
        help="Comma-separated tickers or 'all'"
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        default=True,
        help="Use batch download mode (default: True, faster for multiple companies)"
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Force sequential download mode (slower but more stable)"
    )
    parser.add_argument(
        "--signals-only",
        action="store_true",
        help="Collect only external signals (jobs, tech stack, patents) for all companies; skip SEC filings"
    )
    args = parser.parse_args()

    use_batch = args.batch and not args.sequential
    signals_only = getattr(args, "signals_only", False)

    if args.companies == "all":
        companies = list(TARGET_COMPANIES.keys())
    else:
        companies = [t.strip().upper() for t in args.companies.split(",")]

    print(f"\n{'='*60}")
    print("PE Org-AI-R Evidence Collection (Rate-Limited)")
    print(f"{'='*60}")
    print(f"Companies to process: {', '.join(companies)}")
    if signals_only:
        print("Mode: Signals only (no SEC documents)")
    else:
        print(f"Download mode: {'Batch' if use_batch else 'Sequential'}")
        print(f"Rate limit: 10 requests/second (with safety buffer)")
    # Show which external signal API keys are set (values hidden)
    _s = get_settings()
    serp_ok = bool((_s.serpapi_key or "").strip())
    builtwith_ok = bool((_s.builtwith_api_key or "").strip())
    lens_ok = bool((_s.lens_api_key or "").strip())
    print("External APIs: SerpAPI={} BuiltWith={} Lens={}".format(
        "set" if serp_ok else "not set",
        "set" if builtwith_ok else "not set",
        "set" if lens_ok else "not set",
    ))
    if not (serp_ok or builtwith_ok or lens_ok):
        print("Hint: add SERPAPI_KEY, BUILTWITH_API_KEY, LENS_API_KEY to .env to fetch signals.")
    print(f"{'='*60}\n")

    stats, pipeline_stats = asyncio.run(main(companies, use_batch=use_batch, signals_only=signals_only))

    print(f"\n{'='*60}")
    print("Collection Complete!")
    print(f"{'='*60}")
    print(f"Companies processed: {stats['companies']}")
    print(f"Documents downloaded: {stats['documents']}")
    print(f"Documents processed: {stats['processed_docs']}")
    print(f"Total chunks: {stats['chunks']}")
    print(f"Signals collected: {stats['signals']}")
    print(f"Errors: {stats['errors']}")
    print(f"\nAPI Statistics:")
    print(f"Total API requests: {pipeline_stats['total_requests']}")
    print(f"Rate limit hits: {pipeline_stats['rate_limit_hits']}")
    if pipeline_stats['rate_limit_hits'] > 0:
        hit_rate = (pipeline_stats['rate_limit_hits'] / pipeline_stats['total_requests'] * 100)
        print(f"Rate limit hit rate: {hit_rate:.1f}%")
    else:
        print("✓ No rate limit issues!")
    print(f"{'='*60}\n")
