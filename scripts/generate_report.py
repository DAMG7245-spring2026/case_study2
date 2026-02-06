#!/usr/bin/env python
"""
Generate external signals report to Markdown and CSV.

Includes all four scores (tech hiring, innovation, digital presence, leadership) and the
single composite per rubric (30% + 25% + 25% + 20%). Writes to docs/evidence_report.md
and reports/external_signals_report.csv.

Usage:
    poetry run python scripts/generate_report.py
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

_project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_project_root))

from dotenv import load_dotenv
load_dotenv(_project_root / ".env")

from app.services.snowflake import SnowflakeService

# Composite weights per rubric: tech 0.30, innovation 0.25, digital 0.25, leadership 0.20
W_TECH, W_INNOVATION, W_DIGITAL, W_LEADERSHIP = 0.30, 0.25, 0.25, 0.20


def main():
    db = SnowflakeService()
    query = """
        SELECT s.company_id, s.ticker, s.technology_hiring_score, s.innovation_activity_score,
               s.digital_presence_score, s.leadership_signals_score, s.signal_count, s.last_updated,
               c.name AS company_name
        FROM company_signal_summaries s
        LEFT JOIN companies c ON c.id = s.company_id
        ORDER BY s.ticker
    """
    try:
        rows = db.execute_query(query)
    except Exception as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print("No company signal summaries found. Run collect_evidence.py --signals-only first.")
        sys.exit(0)

    docs_dir = _project_root / "docs"
    docs_dir.mkdir(exist_ok=True)
    reports_dir = _project_root / "reports"
    reports_dir.mkdir(exist_ok=True)

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Aggregate metrics (Summary Statistics)
    try:
        stats = db.get_evidence_stats()
        companies_processed = len(rows)
        total_documents = stats.get("total_documents", 0)
        total_chunks = stats.get("total_chunks", 0)
        total_signals = stats.get("total_signals", 0)
    except Exception:
        companies_processed = len(rows)
        total_documents = total_chunks = total_signals = 0

    # Documents by company (10-K, 10-Q, 8-K, Total, Chunks)
    doc_by_company = {}
    try:
        doc_query = """
            SELECT ticker,
                   SUM(CASE WHEN filing_type = '10-K' THEN 1 ELSE 0 END) AS count_10k,
                   SUM(CASE WHEN filing_type = '10-Q' THEN 1 ELSE 0 END) AS count_10q,
                   SUM(CASE WHEN filing_type = '8-K' THEN 1 ELSE 0 END) AS count_8k,
                   COUNT(*) AS total,
                   COALESCE(SUM(chunk_count), 0) AS chunks
            FROM documents
            GROUP BY ticker
            ORDER BY ticker
        """
        for r in db.execute_query(doc_query):
            doc_by_company[r["ticker"]] = {
                "10-K": int(r.get("count_10k") or 0),
                "10-Q": int(r.get("count_10q") or 0),
                "8-K": int(r.get("count_8k") or 0),
                "total": int(r.get("total") or 0),
                "chunks": int(r.get("chunks") or 0),
            }
    except Exception:
        doc_by_company = {}

    # Build signal score rows and ensure we have doc stats for each ticker
    signal_rows = []
    for r in rows:
        th = float(r.get("technology_hiring_score") or 0)
        ia = float(r.get("innovation_activity_score") or 0)
        dp = float(r.get("digital_presence_score") or 0)
        lead = float(r.get("leadership_signals_score") or 0)
        composite_score = round(
            W_TECH * th + W_INNOVATION * ia + W_DIGITAL * dp + W_LEADERSHIP * lead, 1
        )
        ticker = r.get("ticker") or ""
        signal_rows.append({
            "ticker": ticker,
            "company_name": r.get("company_name") or "—",
            "technology_hiring_score": th,
            "innovation_activity_score": ia,
            "digital_presence_score": dp,
            "leadership_signals_score": lead,
            "composite_score": composite_score,
            "signal_count": int(r.get("signal_count") or 0),
        })
        if ticker and ticker not in doc_by_company:
            doc_by_company[ticker] = {"10-K": 0, "10-Q": 0, "8-K": 0, "total": 0, "chunks": 0}

    # Markdown report: three sections (committed to repo in docs/)
    md_path = docs_dir / "evidence_report.md"
    with open(md_path, "w") as f:
        f.write("# External Signals Report\n\n")
        f.write(f"Generated: {generated}\n\n")

        # 1. Summary Statistics
        f.write("### Summary Statistics\n\n")
        f.write("| Metric | Value |\n")
        f.write("|--------|-------|\n")
        f.write(f"| Companies processed | {companies_processed} |\n")
        f.write(f"| Total documents | {total_documents} |\n")
        f.write(f"| Total chunks | {total_chunks} |\n")
        f.write(f"| Total signals | {total_signals} |\n\n")

        # 2. Documents by Company
        f.write("### Documents by Company\n\n")
        f.write("| Ticker | 10-K | 10-Q | 8-K | Total | Chunks |\n")
        f.write("|--------|------|------|-----|-------|--------|\n")
        for row in signal_rows:
            ticker = row["ticker"]
            doc = doc_by_company.get(ticker, {"10-K": 0, "10-Q": 0, "8-K": 0, "total": 0, "chunks": 0})
            f.write(
                f"| {ticker} | {doc['10-K']} | {doc['10-Q']} | {doc['8-K']} | "
                f"{doc['total']} | {doc['chunks']} |\n"
            )

        # 3. Signal Scores by Company
        f.write("\n### Signal Scores by Company\n\n")
        f.write("| Ticker | Hiring | Innovation | Tech | Leadership | Composite |\n")
        f.write("|--------|--------|------------|------|------------|----------|\n")
        for row in signal_rows:
            f.write(
                f"| {row['ticker']} | {row['technology_hiring_score']:.1f} | "
                f"{row['innovation_activity_score']:.1f} | {row['digital_presence_score']:.1f} | "
                f"{row['leadership_signals_score']:.1f} | {row['composite_score']:.1f} |\n"
            )
    print(f"Wrote {md_path}")

    # CSV report (signal scores; gitignored, stays in reports/)
    csv_path = reports_dir / "external_signals_report.csv"
    with open(csv_path, "w") as f:
        f.write("ticker,company_name,technology_hiring_score,innovation_activity_score,digital_presence_score,leadership_signals_score,composite_score,signal_count\n")
        for row in signal_rows:
            name_esc = row["company_name"].replace('"', '""')
            f.write(
                f'{row["ticker"]},"{name_esc}",{row["technology_hiring_score"]:.1f},'
                f'{row["innovation_activity_score"]:.1f},{row["digital_presence_score"]:.1f},'
                f'{row["leadership_signals_score"]:.1f},{row["composite_score"]:.1f},{row["signal_count"]}\n'
            )
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    main()
