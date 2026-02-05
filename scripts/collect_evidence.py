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
import structlog
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.pipelines.sec_edgar import SECEdgarPipeline
from app.pipelines.document_parser import DocumentParser, SemanticChunker
from app.pipelines.job_signals import JobSignalCollector
from app.pipelines.tech_signals import TechStackCollector
from app.pipelines.patent_signals import PatentSignalCollector

logger = structlog.get_logger()

TARGET_COMPANIES = {
    "CAT": {"name": "Caterpillar Inc.", "sector": "Manufacturing"},
    "DE": {"name": "Deere & Company", "sector": "Manufacturing"},
    "UNH": {"name": "UnitedHealth Group", "sector": "Healthcare"},
    "HCA": {"name": "HCA Healthcare", "sector": "Healthcare"},
    "ADP": {"name": "Automatic Data Processing", "sector": "Services"},
    "PAYX": {"name": "Paychex Inc.", "sector": "Services"},
    "WMT": {"name": "Walmart Inc.", "sector": "Retail"},
    "TGT": {"name": "Target Corporation", "sector": "Retail"},
    "JPM": {"name": "JPMorgan Chase", "sector": "Financial"},
    "GS": {"name": "Goldman Sachs", "sector": "Financial"},
}


async def collect_documents(ticker: str, pipeline: SECEdgarPipeline):
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

                # TODO: Store in database
                # await db.insert_document(doc)
                # await db.insert_chunks(chunks)

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


async def collect_signals(ticker: str):
    """Collect external signals for a company."""
    logger.info("Collecting signals", ticker=ticker)

    signals = []

    # Job postings (simplified - in practice, use API)
    job_collector = JobSignalCollector()
    # In real implementation, fetch actual job postings from API
    # For now, create a placeholder signal
    logger.info("Job signal collector initialized", ticker=ticker)

    # Technology stack
    tech_collector = TechStackCollector()
    # In real implementation, fetch from BuiltWith API
    logger.info("Tech stack collector initialized", ticker=ticker)

    # Patents
    patent_collector = PatentSignalCollector()
    # In real implementation, fetch from USPTO API
    logger.info("Patent collector initialized", ticker=ticker)

    # TODO: Store signals in database
    # for signal in signals:
    #     await db.insert_signal(signal)
    # await db.update_signal_summary(company_id)

    logger.info("Signal collection setup complete", ticker=ticker)

    return 3  # Placeholder for 3 signal types


async def main(companies: list[str], use_batch: bool = True):
    """Main collection routine with rate-limited batch processing."""
    stats = {
        "companies": 0,
        "documents": 0,
        "chunks": 0,
        "processed_docs": 0,
        "signals": 0,
        "errors": 0
    }

    # Initialize SEC EDGAR pipeline once (shared across all downloads)
    pipeline = SECEdgarPipeline(
        company_name="Northeastern University",  # Replace with your institution
        email="tu.wei@northeastern.edu",  # Replace with your email
        download_dir=Path("data/raw/sec"),
        rate_limit_buffer=0.1  # Conservative 100ms buffer for safety
    )

    logger.info(
        "pipeline_initialized",
        rate_limit_buffer=pipeline.rate_limit_buffer,
        max_requests_per_second=pipeline.MAX_REQUESTS_PER_SECOND
    )

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

                    filings = results.get(ticker, [])
                    if not filings:
                        logger.warning("no_filings_downloaded", ticker=ticker)
                        continue

                    # Parse and chunk documents
                    parser = DocumentParser()
                    chunker = SemanticChunker()
                    total_chunks = 0
                    processed_docs = 0

                    for filing_path in filings:
                        try:
                            doc = parser.parse_filing(filing_path, ticker)
                            chunks = chunker.chunk_document(doc)

                            # TODO: Store in database
                            # await db.insert_document(doc)
                            # await db.insert_chunks(chunks)

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
                    signal_count = await collect_signals(ticker)

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

                # Collect documents
                doc_count, chunk_count, processed_docs = await collect_documents(ticker, pipeline)
                stats["documents"] += doc_count
                stats["chunks"] += chunk_count
                stats["processed_docs"] += processed_docs

                # Collect signals
                signal_count = await collect_signals(ticker)
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
    args = parser.parse_args()

    # Determine download mode
    use_batch = args.batch and not args.sequential

    if args.companies == "all":
        companies = list(TARGET_COMPANIES.keys())
    else:
        companies = [t.strip().upper() for t in args.companies.split(",")]

    print(f"\n{'='*60}")
    print("PE Org-AI-R Evidence Collection (Rate-Limited)")
    print(f"{'='*60}")
    print(f"Companies to process: {', '.join(companies)}")
    print(f"Download mode: {'Batch' if use_batch else 'Sequential'}")
    print(f"Rate limit: 10 requests/second (with safety buffer)")
    print(f"{'='*60}\n")

    stats, pipeline_stats = asyncio.run(main(companies, use_batch=use_batch))

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
