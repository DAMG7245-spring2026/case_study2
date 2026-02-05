#!/usr/bin/env python
"""
Backfill evidence for all 10 target companies.

This script ensures all target companies exist in the database
and have evidence collection triggered.
"""

import asyncio
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.collect_evidence import TARGET_COMPANIES, main

if __name__ == "__main__":
    print("\n" + "="*60)
    print("Backfilling Evidence for All Target Companies")
    print("="*60 + "\n")

    companies = list(TARGET_COMPANIES.keys())

    print(f"Processing {len(companies)} companies:")
    for ticker in companies:
        info = TARGET_COMPANIES[ticker]
        print(f"  - {ticker}: {info['name']} ({info['sector']})")

    print("\n" + "="*60 + "\n")

    asyncio.run(main(companies))
