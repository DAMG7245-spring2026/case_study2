"""Patent signal collector (Lens.org API)."""
import json
import re
import httpx
import structlog
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from uuid import UUID
from app.models.signal import ExternalSignal, SignalCategory, SignalSource

logger = structlog.get_logger()


@dataclass
class Patent:
    """A patent record."""
    patent_number: str
    title: str
    abstract: str
    filing_date: datetime
    grant_date: datetime | None
    inventors: list[str]
    assignee: str
    is_ai_related: bool
    ai_categories: list[str]


class PatentSignalCollector:
    """Collect patent signals for AI innovation."""

    AI_PATENT_KEYWORDS = [
        "machine learning", "neural network", "deep learning",
        "artificial intelligence", "natural language processing",
        "computer vision", "reinforcement learning",
        "predictive model", "classification algorithm"
    ]

    AI_PATENT_CLASSES = [
        "706",  # Data processing: AI
        "382",  # Image analysis
        "704",  # Speech processing
    ]

    def __init__(self):
        self.client = httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0 (compatible; research)"})

    def fetch_patents(self, company_name: str, api_key: str | None = None) -> list[Patent]:
        """Fetch patents from Lens.org API by applicant/owner name. Returns [] if no key or on failure."""
        if not api_key or not api_key.strip():
            logger.debug("patent_fetch_skipped", reason="no_api_key", company=company_name)
            return []
        try:
            base = "https://api.lens.org/patent/search"
            params = {"token": api_key.strip()}
            # Search by applicant name; also match owner in case assignee differs
            payload = {
                "query": {
                    "bool": {
                        "should": [
                            {"match": {"applicant.name": company_name}},
                            {"match": {"owner_all.name": company_name}},
                        ]
                    }
                },
                "size": 100,  # Lens API max is 100
                "include": [
                    "lens_id",
                    "doc_number",
                    "date_published",
                    "biblio",
                    "abstract",
                    "legal_status",
                ],
            }
            r = self.client.post(base, params=params, json=payload)
            r.raise_for_status()
            data = r.json()
            patents_data = data.get("data") or []
            out = []
            for p in patents_data:
                pid = p.get("doc_number") or p.get("lens_id") or ""
                # Title from biblio.invention_title
                biblio = p.get("biblio") or {}
                titles = biblio.get("invention_title") or []
                title = (titles[0].get("text") or "") if titles else ""
                # Abstract
                abstr_list = p.get("abstract") or []
                abstract = (abstr_list[0].get("text") or "") if abstr_list else ""
                # Filing date: application_reference.date or earliest_claim or date_published
                app_ref = biblio.get("application_reference") or {}
                fd = app_ref.get("date")
                if not fd and biblio.get("priority_claims", {}).get("earliest_claim"):
                    fd = biblio["priority_claims"]["earliest_claim"].get("date")
                if not fd:
                    fd = p.get("date_published")
                gd = (p.get("legal_status") or {}).get("grant_date")
                # Assignee: first applicant or first owner
                parties = biblio.get("parties") or {}
                applicants = parties.get("applicants") or []
                owners = parties.get("owners_all") or []
                assignee = ""
                if applicants and applicants[0].get("extracted_name"):
                    assignee = applicants[0]["extracted_name"].get("value") or ""
                if not assignee and owners and owners[0].get("extracted_name"):
                    assignee = owners[0]["extracted_name"].get("value") or ""
                inventors = []
                for inv in parties.get("inventors") or []:
                    if inv.get("extracted_name", {}).get("value"):
                        inventors.append(inv["extracted_name"]["value"])
                def _parse_date(s: str | None) -> datetime | None:
                    if not s:
                        return None
                    try:
                        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        return dt
                    except Exception:
                        return None

                filing_dt = _parse_date(fd) or datetime.now(timezone.utc)
                grant_dt = _parse_date(gd)
                patent = Patent(
                    patent_number=str(pid),
                    title=title,
                    abstract=abstract,
                    filing_date=filing_dt,
                    grant_date=grant_dt,
                    inventors=inventors,
                    assignee=assignee,
                    is_ai_related=False,
                    ai_categories=[],
                )
                patent = self.classify_patent(patent)
                out.append(patent)
            logger.info("patent_fetch_ok", company=company_name, count=len(out), source="lens")
            return out
        except Exception as e:
            err_msg = str(e)
            if "token=" in err_msg:
                err_msg = re.sub(r"token=[^\s&'\"]+", "token=***", err_msg)
            logger.warning("patent_fetch_failed", company=company_name, error=err_msg, source="lens")
            return []

    def analyze_patents(
        self,
        company_id: UUID,
        patents: list[Patent],
        years: int = 5
    ) -> ExternalSignal:
        """Analyze patent portfolio for AI innovation."""

        cutoff = datetime.now(timezone.utc) - timedelta(days=years * 365)
        recent_patents = [p for p in patents if p.filing_date > cutoff]
        ai_patents = [p for p in recent_patents if p.is_ai_related]

        # Scoring:
        # - AI patent count: 5 points each (max 50)
        # - Recency bonus: +2 per patent filed in last year (max 20)
        # - Category diversity: 10 points per category (max 30)

        last_year = datetime.now(timezone.utc) - timedelta(days=365)
        recent_ai = [p for p in ai_patents if p.filing_date > last_year]

        categories = set()
        for p in ai_patents:
            categories.update(p.ai_categories)

        score = (
            min(len(ai_patents) * 5, 50) +
            min(len(recent_ai) * 2, 20) +
            min(len(categories) * 10, 30)
        )

        return ExternalSignal(
            company_id=company_id,
            category=SignalCategory.INNOVATION_ACTIVITY,
            source=SignalSource.LENS,
            signal_date=datetime.now(timezone.utc),
            raw_value=f"{len(ai_patents)} AI patents in {years} years",
            normalized_score=round(score, 1),
            confidence=0.90,
            metadata={
                "total_patents": len(patents),
                "ai_patents": len(ai_patents),
                "recent_ai_patents": len(recent_ai),
                "ai_categories": list(categories)
            }
        )

    def classify_patent(self, patent: Patent) -> Patent:
        """Classify a patent as AI-related."""
        text = f"{patent.title} {patent.abstract}".lower()

        is_ai = any(kw in text for kw in self.AI_PATENT_KEYWORDS)

        categories = []
        if "neural network" in text or "deep learning" in text:
            categories.append("deep_learning")
        if "natural language" in text:
            categories.append("nlp")
        if "computer vision" in text or "image" in text:
            categories.append("computer_vision")
        if "predictive" in text:
            categories.append("predictive_analytics")

        patent.is_ai_related = is_ai or len(categories) > 0
        patent.ai_categories = categories

        return patent
