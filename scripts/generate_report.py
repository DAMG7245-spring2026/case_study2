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
    stats = {}
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

    # --- Key Findings (data-driven) ---
    findings_say_do = []
    if signal_rows:
        avg_composite = sum(r["composite_score"] for r in signal_rows) / len(signal_rows)
        avg_docs = sum(doc_by_company.get(r["ticker"], {}).get("total", 0) for r in signal_rows) / len(signal_rows)
        high_disc_low_action = [
            r["ticker"] for r in signal_rows
            if doc_by_company.get(r["ticker"], {}).get("total", 0) >= avg_docs and r["composite_score"] < avg_composite
        ]
        if high_disc_low_action:
            findings_say_do.append(
                f"{len(high_disc_low_action)} companies have above-average document count but below-average composite score (say-do gap): {', '.join(high_disc_low_action)}."
            )
        docs_no_signals = [
            r["ticker"] for r in signal_rows
            if doc_by_company.get(r["ticker"], {}).get("total", 0) > 0 and r["signal_count"] == 0
        ]
        if docs_no_signals:
            findings_say_do.append(
                f"{len(docs_no_signals)} companies have SEC documents but no external signals collected: {', '.join(docs_no_signals)}."
            )
        score_spread = []
        for r in signal_rows:
            scores = [r["technology_hiring_score"], r["innovation_activity_score"], r["digital_presence_score"], r["leadership_signals_score"]]
            spread = max(scores) - min(scores)
            if spread >= 30:
                score_spread.append(f"{r['ticker']} (spread {spread:.0f})")
        if score_spread:
            findings_say_do.append(
                f"Companies with large score imbalance (strongest vs weakest dimension ≥ 30 pts): {', '.join(score_spread)}."
            )
    if not findings_say_do:
        findings_say_do = ["No strong say-do gaps detected from current disclosure vs external signal alignment."]

    sector_patterns = []
    try:
        sector_query = """
            SELECT i.sector,
                   AVG(s.technology_hiring_score) AS avg_hiring,
                   AVG(s.innovation_activity_score) AS avg_innovation,
                   AVG(s.digital_presence_score) AS avg_digital,
                   AVG(s.leadership_signals_score) AS avg_leadership
            FROM company_signal_summaries s
            JOIN companies c ON c.id = s.company_id
            JOIN industries i ON i.id = c.industry_id
            GROUP BY i.sector
            ORDER BY i.sector
        """
        sector_rows = db.execute_query(sector_query)
        for r in sector_rows:
            sector = r.get("sector") or "Unknown"
            th = float(r.get("avg_hiring") or 0)
            ia = float(r.get("avg_innovation") or 0)
            dp = float(r.get("avg_digital") or 0)
            lead = float(r.get("avg_leadership") or 0)
            comp = round(W_TECH * th + W_INNOVATION * ia + W_DIGITAL * dp + W_LEADERSHIP * lead, 1)
            sector_patterns.append((sector, {"hiring": th, "innovation": ia, "digital": dp, "leadership": lead, "composite": comp}))
    except Exception:
        sector_rows = []
    sector_sentences = []
    if sector_patterns:
        by_composite = sorted(sector_patterns, key=lambda x: x[1]["composite"], reverse=True)
        top = by_composite[0]
        sector_sentences.append(f"Sector **{top[0]}** has the highest average composite score ({top[1]['composite']:.1f}).")
        if len(by_composite) > 1:
            bottom = by_composite[-1]
            sector_sentences.append(f"**{bottom[0]}** has the lowest ({bottom[1]['composite']:.1f}).")
        by_hiring = max(sector_patterns, key=lambda x: x[1]["hiring"])
        sector_sentences.append(f"**{by_hiring[0]}** leads on average tech hiring score ({by_hiring[1]['hiring']:.1f}).")
    if not sector_sentences:
        sector_sentences = ["Insufficient sector-level data to summarize patterns."]

    data_quality_lines = []
    try:
        total_companies = stats.get("total_companies", 0)
        companies_with_docs = stats.get("companies_with_documents", 0)
        companies_with_sigs = stats.get("companies_with_signals", 0)
        no_docs = total_companies - companies_with_docs if total_companies else 0
        no_sigs = total_companies - companies_with_sigs if total_companies else 0
        if no_docs > 0 or no_sigs > 0:
            data_quality_lines.append(f"{no_docs} companies have no SEC documents; {no_sigs} have no external signals.")
        err_result = db.execute_one("SELECT COUNT(*) AS cnt FROM documents WHERE error_message IS NOT NULL AND error_message != ''")
        err_count = (err_result or {}).get("cnt") or 0
        if err_count > 0:
            data_quality_lines.append(f"{err_count} documents have a non-empty error_message (parsing or processing issues).")
        doc_status = stats.get("documents_by_status", {})
        if doc_status:
            pending = doc_status.get("pending", 0)
            if pending > 0:
                data_quality_lines.append(f"Document status: {pending} pending; {sum(v for k, v in doc_status.items() if k != 'pending')} in other states.")
    except Exception:
        data_quality_lines = ["Could not compute data quality metrics."]
    if not data_quality_lines:
        data_quality_lines = ["No major data quality issues identified from current stats."]

    # Markdown report: three sections + Key Findings (committed to repo in docs/)
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
        f.write("| Ticker | Hiring | Innovation | Digital Presence | Leadership | Composite | Signals |\n")
        f.write("|--------|--------|------------|------------------|------------|----------|--------|\n")
        for row in signal_rows:
            f.write(
                f"| {row['ticker']} | {row['technology_hiring_score']:.1f} | "
                f"{row['innovation_activity_score']:.1f} | {row['digital_presence_score']:.1f} | "
                f"{row['leadership_signals_score']:.1f} | {row['composite_score']:.1f} | {row['signal_count']} |\n"
            )

        # 4. Key Findings (data-driven)
        f.write("\n## Key Findings\n\n")
        f.write("1. **Say–do gaps:** ")
        f.write(" ".join(findings_say_do) + "\n\n")
        f.write("2. **Patterns across sectors:** ")
        f.write(" ".join(sector_sentences) + "\n\n")
        f.write("3. **Data quality issues encountered:** ")
        f.write(" ".join(data_quality_lines) + "\n")
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
