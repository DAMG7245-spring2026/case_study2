"""Technology stack signal collector."""
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID
from app.models.signal import ExternalSignal, SignalCategory, SignalSource


@dataclass
class TechnologyDetection:
    """A detected technology."""
    name: str
    category: str
    is_ai_related: bool
    confidence: float


class TechStackCollector:
    """Analyze company technology stacks."""

    AI_TECHNOLOGIES = {
        # Cloud AI Services
        "aws sagemaker": "cloud_ml",
        "azure ml": "cloud_ml",
        "google vertex": "cloud_ml",
        "databricks": "cloud_ml",

        # ML Frameworks
        "tensorflow": "ml_framework",
        "pytorch": "ml_framework",
        "scikit-learn": "ml_framework",

        # Data Infrastructure
        "snowflake": "data_platform",
        "databricks": "data_platform",
        "spark": "data_platform",

        # AI APIs
        "openai": "ai_api",
        "anthropic": "ai_api",
        "huggingface": "ai_api",
    }

    def analyze_tech_stack(
        self,
        company_id: UUID,
        technologies: list[TechnologyDetection]
    ) -> ExternalSignal:
        """Analyze technology stack for AI capabilities."""

        ai_techs = [t for t in technologies if t.is_ai_related]

        # Score by category
        categories_found = set(t.category for t in ai_techs)

        # Scoring:
        # - Each AI technology: 10 points (max 50)
        # - Each category covered: 12.5 points (max 50)
        tech_score = min(len(ai_techs) * 10, 50)
        category_score = min(len(categories_found) * 12.5, 50)

        score = tech_score + category_score

        return ExternalSignal(
            company_id=company_id,
            category=SignalCategory.DIGITAL_PRESENCE,
            source=SignalSource.BUILTWITH,
            signal_date=datetime.now(timezone.utc),
            raw_value=f"{len(ai_techs)} AI technologies detected",
            normalized_score=round(score, 1),
            confidence=0.85,
            metadata={
                "ai_technologies": [t.name for t in ai_techs],
                "categories": list(categories_found),
                "total_technologies": len(technologies)
            }
        )
