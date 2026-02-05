"""Patent signal collector."""
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from uuid import UUID
from app.models.signal import ExternalSignal, SignalCategory, SignalSource


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
            source=SignalSource.USPTO,
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
