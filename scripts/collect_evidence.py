#!/usr/bin/env python
"""
Collect evidence for all target companies.

Usage:
    poetry run python scripts/collect_evidence.py --companies all
    poetry run python scripts/collect_evidence.py --companies CAT,DE,UNH
    poetry run python scripts/collect_evidence.py --companies JPM --documents-only
    poetry run python scripts/collect_evidence.py --companies all --signals-only
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.models.evidence import TARGET_COMPANIES
from app.pipelines import (
    SECEdgarPipeline,
    DocumentParser,
    SemanticChunker,
    JobSignalCollector,
    TechStackCollector,
    PatentSignalCollector,
    LeadershipSignalCollector,
)
from app.config import get_settings
from app.services.snowflake import get_snowflake_service
from app.services.s3_storage import get_s3_storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class EvidenceCollector:
    """Main evidence collection orchestrator."""

    def __init__(
        self,
        email: str = "student@university.edu",
        download_dir: Path = Path("data/raw/sec")
    ):
        self.email = email
        self.download_dir = download_dir
        self.download_dir.mkdir(parents=True, exist_ok=True)
        
        # Services
        self.db = get_snowflake_service()
        self.s3 = get_s3_storage()
        
        # Pipelines
        self.sec_pipeline = SECEdgarPipeline(
            company_name="PE-OrgAIR-Platform",
            email=email,
            download_dir=download_dir
        )
        self.parser = DocumentParser()
        self.chunker = SemanticChunker()
        
        # Signal collectors
        self.job_collector = JobSignalCollector()
        self.tech_collector = TechStackCollector()
        self.patent_collector = PatentSignalCollector()
        
        # Statistics
        self.stats = {
            "companies": 0,
            "documents": 0,
            "chunks": 0,
            "signals": 0,
            "s3_uploads": 0,
            "errors": 0
        }

    def get_or_create_company(self, ticker: str) -> UUID:
        """Get or create company in database."""
        company_info = TARGET_COMPANIES[ticker]
        
        # Get industry_id
        industry_result = self.db.execute_one(
            "SELECT id FROM industries WHERE name = %s",
            (company_info["industry"],)
        )
        
        if not industry_result:
            raise ValueError(f"Industry '{company_info['industry']}' not found in database")
        
        industry_id = UUID(industry_result["id"])
        
        company = self.db.get_or_create_company(
            ticker=ticker,
            name=company_info["name"],
            industry_id=industry_id
        )
        
        return UUID(company["id"])

    def collect_documents(self, ticker: str, company_id: UUID, years_back: int = 3) -> int:
        """Collect SEC documents for a company."""
        logger.info(f"📄 Collecting documents for {ticker}")
        
        docs_processed = 0
        
        try:
            # Download filings
            after_date = f"{datetime.now().year - years_back}-01-01"
            filings = self.sec_pipeline.download_filings(
                ticker=ticker,
                filing_types=["10-K", "10-Q", "8-K"],
                limit=10,
                after=after_date
            )
            
            logger.info(f"   Downloaded {len(filings)} filings")
            
            # Process each filing
            for filing_path in filings:
                try:
                    filing_path = Path(filing_path)
                    
                    # Parse document
                    parsed = self.parser.parse_filing(filing_path, ticker)
                    
                    # Check for duplicate
                    existing = self.db.execute_one(
                        "SELECT id FROM documents WHERE content_hash = %s",
                        (parsed.content_hash,)
                    )
                    if existing:
                        logger.info(f"   ⏭️  Skipping duplicate {parsed.filing_type}")
                        continue
                    
                    # Upload to S3
                    filing_date_str = parsed.filing_date.strftime("%Y-%m-%d")
                    s3_key = self.s3.upload_sec_filing(
                        ticker=ticker,
                        filing_type=parsed.filing_type,
                        filing_date=filing_date_str,
                        local_path=filing_path,
                        content_hash=parsed.content_hash
                    )
                    
                    if s3_key:
                        self.stats["s3_uploads"] += 1
                        logger.info(f"   ☁️  Uploaded to S3: {s3_key}")
                    
                    # Insert document record
                    doc_id = self.db.insert_document(
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
                    
                    # Chunk document
                    chunks = self.chunker.chunk_document(parsed)
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
                    
                    # Insert chunks
                    self.db.insert_chunks(doc_id, chunk_dicts)
                    
                    # Update status
                    self.db.update_document_status(doc_id, "chunked", chunk_count=len(chunks))
                    
                    docs_processed += 1
                    self.stats["documents"] += 1
                    self.stats["chunks"] += len(chunks)
                    
                    logger.info(f"   ✅ {parsed.filing_type}: {len(chunks)} chunks")
                    
                except Exception as e:
                    logger.error(f"   ❌ Error processing {filing_path}: {e}")
                    self.stats["errors"] += 1
                    
        except Exception as e:
            logger.error(f"   ❌ Error downloading filings: {e}")
            self.stats["errors"] += 1
        
        return docs_processed

    def collect_signals(self, ticker: str, company_id: UUID) -> int:
        """Collect external signals for a company using real API fetches."""
        logger.info(f"📊 Collecting signals for {ticker}")

        company_info = TARGET_COMPANIES[ticker]
        domain = company_info.get("domain", "")
        settings = get_settings()

        hiring_score = 0.0
        digital_score = 0.0
        innovation_score = 0.0
        signals_collected = 0

        try:
            # Job posting signal (SerpAPI)
            postings = self.job_collector.fetch_postings(
                company_info["name"], api_key=settings.serpapi_key or None
            )
            if postings:
                job_signal = self.job_collector.analyze_job_postings(
                    company_info["name"], postings, company_id
                )
                self.db.insert_signal(
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
                logger.info(f"   ✅ Hiring signal: {job_signal.normalized_score:.1f}")

            # Technology stack signal (BuiltWith)
            techs = self.tech_collector.fetch_tech_stack(
                domain, api_key=settings.builtwith_api_key or None
            )
            if techs:
                tech_signal = self.tech_collector.analyze_tech_stack(company_id, techs)
                self.db.insert_signal(
                    company_id=company_id,
                    category=tech_signal.category.value,
                    source=tech_signal.source.value,
                    signal_date=tech_signal.signal_date,
                    raw_value=tech_signal.raw_value,
                    normalized_score=tech_signal.normalized_score,
                    confidence=tech_signal.confidence,
                    metadata=tech_signal.metadata
                )
                digital_score = tech_signal.normalized_score
                signals_collected += 1
                logger.info(f"   ✅ Tech stack signal: {tech_signal.normalized_score:.1f}")

            # Patent signal (Lens)
            patents = self.patent_collector.fetch_patents(
                company_info["name"], api_key=settings.lens_api_key or None
            )
            if patents:
                patent_signal = self.patent_collector.analyze_patents(company_id, patents)
                self.db.insert_signal(
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
                logger.info(f"   ✅ Patent signal: {patent_signal.normalized_score:.1f}")

            # Leadership signals (company website + optional LinkedIn)
            leadership_collector = LeadershipSignalCollector()
            website_data = leadership_collector.fetch_from_company_website(domain)
            linkedin_data = leadership_collector.fetch_from_linkedin(
                company_info["name"], api_key=settings.linkedin_api_key or None
            )
            leadership_signals = leadership_collector.analyze_leadership(
                company_id, website_data=website_data, linkedin_data=linkedin_data
            )
            if not leadership_signals:
                logger.info(
                    f"   No leadership data for {ticker} (domain={domain!r}); "
                    "check logs for leadership_fetch_no_page if website fetch failed."
                )
            leadership_score = 0.0
            for sig in leadership_signals:
                self.db.insert_signal(
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
                logger.info(f"   ✅ Leadership signal ({sig.source.value}): {sig.normalized_score:.1f}")

            self.db.upsert_signal_summary(
                company_id=company_id,
                ticker=ticker,
                technology_hiring_score=hiring_score,
                innovation_activity_score=innovation_score,
                digital_presence_score=digital_score,
                leadership_signals_score=leadership_score,
                signal_count=signals_collected
            )
            self.stats["signals"] += signals_collected

        except Exception as e:
            logger.error(f"   ❌ Error collecting signals: {e}")
            self.stats["errors"] += 1

        return signals_collected

    def collect_for_company(
        self,
        ticker: str,
        include_documents: bool = True,
        include_signals: bool = True,
        years_back: int = 3
    ) -> dict:
        """Collect all evidence for a single company."""
        if ticker not in TARGET_COMPANIES:
            logger.warning(f"Unknown ticker: {ticker}")
            return {}
        
        company_info = TARGET_COMPANIES[ticker]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🏢 Processing {ticker} - {company_info['name']}")
        logger.info(f"   Sector: {company_info['sector']} | Industry: {company_info['industry']}")
        logger.info(f"{'='*60}")
        
        result = {
            "ticker": ticker,
            "name": company_info["name"],
            "documents_collected": 0,
            "signals_collected": 0,
        }
        
        try:
            # Get or create company
            company_id = self.get_or_create_company(ticker)
            logger.info(f"   Company ID: {company_id}")
            
            if include_documents:
                result["documents_collected"] = self.collect_documents(ticker, company_id, years_back)
            
            if include_signals:
                result["signals_collected"] = self.collect_signals(ticker, company_id)
            
            self.stats["companies"] += 1
            
        except Exception as e:
            logger.error(f"   ❌ Error processing {ticker}: {e}")
            self.stats["errors"] += 1
        
        return result

    def collect_all(
        self,
        tickers: list[str],
        include_documents: bool = True,
        include_signals: bool = True,
        years_back: int = 3
    ) -> dict:
        """Collect evidence for multiple companies."""
        results = {}
        
        for ticker in tickers:
            result = self.collect_for_company(
                ticker,
                include_documents=include_documents,
                include_signals=include_signals,
                years_back=years_back
            )
            if result:
                results[ticker] = result
        
        return results

    def print_summary(self):
        """Print collection summary."""
        logger.info(f"\n{'='*60}")
        logger.info("📈 COLLECTION SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"   Companies processed:  {self.stats['companies']}")
        logger.info(f"   Documents collected:  {self.stats['documents']}")
        logger.info(f"   Chunks created:       {self.stats['chunks']}")
        logger.info(f"   S3 uploads:           {self.stats['s3_uploads']}")
        logger.info(f"   Signals collected:    {self.stats['signals']}")
        logger.info(f"   Errors:               {self.stats['errors']}")
        logger.info(f"{'='*60}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Collect evidence for PE Org-AI-R Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --companies all                    # All 10 companies
  %(prog)s --companies JPM,WMT,GS             # Specific companies
  %(prog)s --companies all --signals-only     # Only signals (fast)
  %(prog)s --companies JPM --documents-only   # Only documents
        """
    )
    parser.add_argument(
        "--companies",
        default="all",
        help="Comma-separated tickers or 'all' (default: all)"
    )
    parser.add_argument(
        "--documents-only",
        action="store_true",
        help="Only collect SEC documents"
    )
    parser.add_argument(
        "--signals-only",
        action="store_true",
        help="Only collect external signals"
    )
    parser.add_argument(
        "--years-back",
        type=int,
        default=3,
        help="Years of SEC filings to collect (default: 3)"
    )
    parser.add_argument(
        "--email",
        default="student@university.edu",
        help="Email for SEC EDGAR (required by SEC)"
    )
    parser.add_argument(
        "--output-dir",
        default="data/raw/sec",
        help="Directory for downloaded files"
    )
    
    args = parser.parse_args()
    
    # Determine companies to process
    if args.companies.lower() == "all":
        tickers = list(TARGET_COMPANIES.keys())
    else:
        tickers = [t.strip().upper() for t in args.companies.split(",")]
        invalid = [t for t in tickers if t not in TARGET_COMPANIES]
        if invalid:
            logger.warning(f"Unknown tickers (skipping): {', '.join(invalid)}")
            tickers = [t for t in tickers if t in TARGET_COMPANIES]
    
    if not tickers:
        logger.error("No valid tickers to process")
        sys.exit(1)
    
    # Determine what to collect
    include_documents = not args.signals_only
    include_signals = not args.documents_only
    
    logger.info(f"\n🚀 Starting evidence collection")
    logger.info(f"   Companies: {', '.join(tickers)}")
    logger.info(f"   Documents: {'Yes' if include_documents else 'No'}")
    logger.info(f"   Signals:   {'Yes' if include_signals else 'No'}")
    logger.info(f"   Years:     {args.years_back}")
    
    # Run collection
    collector = EvidenceCollector(
        email=args.email,
        download_dir=Path(args.output_dir)
    )
    
    results = collector.collect_all(
        tickers=tickers,
        include_documents=include_documents,
        include_signals=include_signals,
        years_back=args.years_back
    )
    
    # Print summary
    collector.print_summary()
    
    # Save results summary
    import json
    output_file = Path("data/evidence_summary.json")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w") as f:
        json.dump({
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "stats": collector.stats,
            "results": results
        }, f, indent=2, default=str)
    
    logger.info(f"📁 Results saved to {output_file}")


if __name__ == "__main__":
    main()
