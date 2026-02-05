#!/usr/bin/env python
"""
Generate external signals report (excluding leadership) to Markdown and CSV.

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

# Composite weights when excluding leadership (30+25+25 = 80; renormalize to 1.0)
W_TECH = 0.30 / 0.80
W_INNOVATION = 0.25 / 0.80
W_DIGITAL = 0.25 / 0.80


def main():
    db = SnowflakeService()
    query = """
        SELECT s.company_id, s.ticker, s.technology_hiring_score, s.innovation_activity_score,
               s.digital_presence_score, s.signal_count, s.last_updated,
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

    report_dir = _project_root / "reports"
    report_dir.mkdir(exist_ok=True)

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Build table rows
    table_rows = []
    for r in rows:
        th = float(r.get("technology_hiring_score") or 0)
        ia = float(r.get("innovation_activity_score") or 0)
        dp = float(r.get("digital_presence_score") or 0)
        composite = round(W_TECH * th + W_INNOVATION * ia + W_DIGITAL * dp, 1)
        table_rows.append({
            "ticker": r.get("ticker") or "",
            "company_name": r.get("company_name") or "—",
            "technology_hiring_score": th,
            "innovation_activity_score": ia,
            "digital_presence_score": dp,
            "composite_without_leadership": composite,
            "signal_count": int(r.get("signal_count") or 0),
        })

    # Markdown report
    md_path = report_dir / "external_signals_report.md"
    with open(md_path, "w") as f:
        f.write("# External Signals Report (Leadership Excluded)\n\n")
        f.write(f"Generated: {generated}\n\n")
        f.write("Composite score uses only **Technology Hiring**, **Innovation Activity**, and **Digital Presence**. Leadership is excluded.\n\n")
        f.write("| Ticker | Company | Tech Hiring | Innovation | Digital | Composite | Signals |\n")
        f.write("|--------|---------|-------------|------------|---------|-----------|--------|\n")
        for row in table_rows:
            f.write(
                f"| {row['ticker']} | {row['company_name']} | "
                f"{row['technology_hiring_score']:.1f} | {row['innovation_activity_score']:.1f} | "
                f"{row['digital_presence_score']:.1f} | {row['composite_without_leadership']:.1f} | "
                f"{row['signal_count']} |\n"
            )
    print(f"Wrote {md_path}")

    # CSV report
    csv_path = report_dir / "external_signals_report.csv"
    with open(csv_path, "w") as f:
        f.write("ticker,company_name,technology_hiring_score,innovation_activity_score,digital_presence_score,composite_without_leadership,signal_count\n")
        for row in table_rows:
            name_esc = row["company_name"].replace('"', '""')
            f.write(f'{row["ticker"]},"{name_esc}",{row["technology_hiring_score"]:.1f},{row["innovation_activity_score"]:.1f},{row["digital_presence_score"]:.1f},{row["composite_without_leadership"]:.1f},{row["signal_count"]}\n')
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    main()
