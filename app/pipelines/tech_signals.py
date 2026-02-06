"""Technology stack signal collector (BuiltWith Free API)."""

import logging
import time
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import httpx

from app.models.signal import (
    ExternalSignalCreate,
    SignalCategory,
    SignalSource,
    TechnologyDetection,
)

logger = logging.getLogger(__name__)


class TechStackCollector:
    """Analyze company technology stacks for AI capabilities."""

    # Known AI-related technologies and their categories
    AI_TECHNOLOGIES = {
        # Cloud AI Services
        "aws sagemaker": "cloud_ml",
        "azure ml": "cloud_ml",
        "azure machine learning": "cloud_ml",
        "google vertex": "cloud_ml",
        "google cloud ai": "cloud_ml",
        "databricks": "cloud_ml",
        
        # ML Frameworks
        "tensorflow": "ml_framework",
        "pytorch": "ml_framework",
        "scikit-learn": "ml_framework",
        "keras": "ml_framework",
        "jax": "ml_framework",
        
        # Data Infrastructure
        "snowflake": "data_platform",
        "spark": "data_platform",
        "apache spark": "data_platform",
        "kafka": "data_platform",
        "airflow": "data_platform",
        
        # AI APIs
        "openai": "ai_api",
        "anthropic": "ai_api",
        "huggingface": "ai_api",
        "cohere": "ai_api",
        
        # ML Infrastructure
        "mlflow": "ml_infrastructure",
        "kubeflow": "ml_infrastructure",
        "ray": "ml_infrastructure",
        "weights and biases": "ml_infrastructure",
        "wandb": "ml_infrastructure",
    }

    def __init__(self):
        self.client = httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0 (compatible; research)"})

    def fetch_tech_stack(self, domain: str, api_key: str | None = None) -> list[TechnologyDetection]:
        """Fetch technology stack from BuiltWith Free API.

        - Endpoint: https://api.builtwith.com/free1/api.json
        - Docs: https://api.builtwith.com/free-api#usage
        - Key: must be the key from the Free API product. Limit: 1 request per second.

        Returns [] if no key or on failure.
        """
        if not api_key or not api_key.strip():
            logger.debug("tech_fetch_skipped reason=no_api_key domain=%s", domain)
            return []
        if not domain or not domain.strip():
            logger.debug("tech_fetch_skipped reason=no_domain")
            return []
        try:
            lookup = domain.strip().lower().replace("https://", "").replace("http://", "").split("/")[0]
            url = "https://api.builtwith.com/free1/api.json"
            params = {"KEY": api_key.strip(), "LOOKUP": lookup}
            r = self.client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            errors = data.get("Errors") or []
            if errors:
                msg = errors[0].get("Message") or errors[0].get("Code") or str(errors[0])
                logger.warning("tech_fetch_failed domain=%s error=%s", lookup, msg)
                return []
            results = data.get("free1") or data.get("Results") or data
            if isinstance(results, list) and results:
                results = results[0]
            groups = results.get("Groups") or results.get("groups") or []
            techs = []
            seen = set()
            for group in groups:
                group_name = (group.get("Name") or group.get("name") or "").lower()
                if not group_name or group_name in seen:
                    continue
                seen.add(group_name)
                is_ai = False
                category = "other"
                for kw, cat_label in self.AI_TECHNOLOGIES.items():
                    if kw in group_name:
                        is_ai = True
                        category = cat_label
                        break
                techs.append(TechnologyDetection(name=group_name, category=category, is_ai_related=is_ai, confidence=0.8))
                for cat in group.get("Categories") or group.get("categories") or []:
                    cat_name = (cat.get("Name") or cat.get("name") or "").lower()
                    if not cat_name or cat_name in seen:
                        continue
                    seen.add(cat_name)
                    is_ai_cat = any(kw in cat_name for kw in self.AI_TECHNOLOGIES)
                    techs.append(TechnologyDetection(name=cat_name, category="other", is_ai_related=is_ai_cat, confidence=0.7))
            time.sleep(1)
            logger.info("tech_fetch_ok domain=%s count=%s", lookup, len(techs))
            return techs
        except Exception as e:
            logger.warning("tech_fetch_failed domain=%s error=%s", domain, str(e))
            return []

    def analyze_tech_stack(
        self,
        company_id: UUID,
        technologies: list[TechnologyDetection]
    ) -> ExternalSignalCreate:
        """
        Analyze technology stack for AI capabilities.
        
        Scoring (0-100):
        - Each AI technology: 10 points (max 50)
        - Each category covered: 12.5 points (max 50)
        """
        # Filter AI-related technologies
        ai_techs = [t for t in technologies if t.is_ai_related]
        
        # Score by category coverage
        categories_found = set(t.category for t in ai_techs)
        
        # Calculate scores
        tech_score = min(len(ai_techs) * 10, 50)
        category_score = min(len(categories_found) * 12.5, 50)
        
        score = tech_score + category_score

        return ExternalSignalCreate(
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
                "total_technologies": len(technologies),
                "category_count": len(categories_found)
            }
        )

    def classify_technology(self, tech_name: str) -> TechnologyDetection:
        """Classify a technology as AI-related or not."""
        tech_lower = tech_name.lower()
        
        # Check if it's a known AI technology
        for ai_tech, category in self.AI_TECHNOLOGIES.items():
            if ai_tech in tech_lower or tech_lower in ai_tech:
                return TechnologyDetection(
                    name=tech_name,
                    category=category,
                    is_ai_related=True,
                    confidence=0.9
                )
        
        # Not a known AI technology
        return TechnologyDetection(
            name=tech_name,
            category="other",
            is_ai_related=False,
            confidence=0.7
        )

    def create_sample_technologies(
        self, 
        ai_maturity: str = "medium"
    ) -> list[TechnologyDetection]:
        """
        Create sample technology detections for testing/demo.
        
        Args:
            ai_maturity: "low", "medium", or "high"
        """
        # Base technologies (non-AI)
        base_techs = [
            TechnologyDetection(name="React", category="frontend", is_ai_related=False),
            TechnologyDetection(name="Node.js", category="backend", is_ai_related=False),
            TechnologyDetection(name="PostgreSQL", category="database", is_ai_related=False),
            TechnologyDetection(name="AWS", category="cloud", is_ai_related=False),
            TechnologyDetection(name="Docker", category="devops", is_ai_related=False),
        ]
        
        # AI technologies by maturity level
        low_ai_techs = [
            TechnologyDetection(name="Python", category="language", is_ai_related=False),
        ]
        
        medium_ai_techs = [
            TechnologyDetection(name="Snowflake", category="data_platform", is_ai_related=True),
            TechnologyDetection(name="Apache Spark", category="data_platform", is_ai_related=True),
        ]
        
        high_ai_techs = [
            TechnologyDetection(name="AWS SageMaker", category="cloud_ml", is_ai_related=True),
            TechnologyDetection(name="TensorFlow", category="ml_framework", is_ai_related=True),
            TechnologyDetection(name="OpenAI", category="ai_api", is_ai_related=True),
            TechnologyDetection(name="MLflow", category="ml_infrastructure", is_ai_related=True),
        ]
        
        if ai_maturity == "low":
            return base_techs + low_ai_techs
        elif ai_maturity == "medium":
            return base_techs + low_ai_techs + medium_ai_techs
        else:  # high
            return base_techs + low_ai_techs + medium_ai_techs + high_ai_techs