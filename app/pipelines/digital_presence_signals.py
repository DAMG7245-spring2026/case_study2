"""Digital presence signal collection: BuiltWith (tech stack) + company news page."""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import httpx
from bs4 import BeautifulSoup

from app.models.signal import (
    ExternalSignalCreate,
    SignalCategory,
    SignalSource,
    TechnologyDetection,
)

logger = logging.getLogger(__name__)


# --- BuiltWith (tech stack) ---

class TechStackCollector:
    """Analyze company technology stacks for AI capabilities (BuiltWith Free API)."""

    AI_TECHNOLOGIES = {
        "aws sagemaker": "cloud_ml",
        "azure ml": "cloud_ml",
        "azure machine learning": "cloud_ml",
        "google vertex": "cloud_ml",
        "google cloud ai": "cloud_ml",
        "databricks": "cloud_ml",
        "tensorflow": "ml_framework",
        "pytorch": "ml_framework",
        "scikit-learn": "ml_framework",
        "keras": "ml_framework",
        "jax": "ml_framework",
        "snowflake": "data_platform",
        "spark": "data_platform",
        "apache spark": "data_platform",
        "kafka": "data_platform",
        "airflow": "data_platform",
        "openai": "ai_api",
        "anthropic": "ai_api",
        "huggingface": "ai_api",
        "cohere": "ai_api",
        "mlflow": "ml_infrastructure",
        "kubeflow": "ml_infrastructure",
        "ray": "ml_infrastructure",
        "weights and biases": "ml_infrastructure",
        "wandb": "ml_infrastructure",
    }

    def __init__(self):
        self.client = httpx.Client(timeout=30.0, headers={"User-Agent": "Mozilla/5.0 (compatible; research)"})

    def fetch_tech_stack(self, domain: str, api_key: str | None = None) -> list[TechnologyDetection]:
        """Fetch technology stack from BuiltWith Free API. Returns [] if no key or on failure."""
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
        """Analyze technology stack for AI capabilities. Scoring: AI techs (max 50) + category coverage (max 50)."""
        ai_techs = [t for t in technologies if t.is_ai_related]
        categories_found = set(t.category for t in ai_techs)
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
        for ai_tech, category in self.AI_TECHNOLOGIES.items():
            if ai_tech in tech_lower or tech_lower in ai_tech:
                return TechnologyDetection(name=tech_name, category=category, is_ai_related=True, confidence=0.9)
        return TechnologyDetection(name=tech_name, category="other", is_ai_related=False, confidence=0.7)

    def create_sample_technologies(self, ai_maturity: str = "medium") -> list[TechnologyDetection]:
        """Create sample technology detections for testing/demo."""
        base_techs = [
            TechnologyDetection(name="React", category="frontend", is_ai_related=False),
            TechnologyDetection(name="Node.js", category="backend", is_ai_related=False),
            TechnologyDetection(name="PostgreSQL", category="database", is_ai_related=False),
            TechnologyDetection(name="AWS", category="cloud", is_ai_related=False),
            TechnologyDetection(name="Docker", category="devops", is_ai_related=False),
        ]
        low_ai_techs = [TechnologyDetection(name="Python", category="language", is_ai_related=False)]
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
        if ai_maturity == "medium":
            return base_techs + low_ai_techs + medium_ai_techs
        return base_techs + low_ai_techs + medium_ai_techs + high_ai_techs


# --- Company news page ---

ARTICLE_LINK_SELECTORS = [
    "a[href*='/news/']", "a[href*='/press/']", "a[href*='/releases/']",
    "a[href*='/newsroom/']", "a[href*='/media/']", "article a",
    ".news-item a", ".press-release a", "[class*='news'] a", "[class*='release'] a",
]
LIST_ITEM_SELECTORS = [
    "ul.news-list li", "ol.news-list li", "[class*='news'] li",
    "[class*='release'] li", "article",
]
KEYWORD_BONUS_TERMS = ["ai", "digital", "technology", "innovation", "data"]
KEYWORD_BONUS_MAX = 25.0
MIN_PAGE_LENGTH = 50


class NewsSignalCollector:
    """Collect and score digital presence signals from company news pages."""

    def __init__(self):
        self.client = httpx.Client(
            timeout=20.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )

    def fetch_news_page(self, url: str) -> Optional[str]:
        """Fetch a news page by URL. Returns HTML if status 200, else None."""
        url = (url or "").strip()
        if not url:
            return None
        if not url.startswith("http"):
            url = f"https://{url}"
        try:
            r = self.client.get(url)
            if r.status_code != 200:
                logger.debug("news_fetch_failed url=%s status=%s", url, r.status_code)
                return None
            logger.info("news_fetch_ok url=%s length=%s", url, len(r.text))
            return r.text
        except Exception as e:
            logger.warning("news_fetch_failed url=%s error=%s", url, str(e))
            return None

    def _extract_text(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return re.sub(r"\s+", " ", text)

    def _count_article_like_elements(self, soup: BeautifulSoup) -> int:
        seen_hrefs: set[str] = set()
        count = 0
        for selector in ARTICLE_LINK_SELECTORS:
            for el in soup.select(selector):
                href = el.get("href") or ""
                if href and href not in seen_hrefs and len(href) > 5:
                    seen_hrefs.add(href)
                    count += 1
        if count > 0:
            return min(count, 30)
        for selector in LIST_ITEM_SELECTORS:
            n = len(soup.select(selector))
            if n > 0:
                return min(n, 30)
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if len(href) > 10 and not href.startswith("#") and "javascript:" not in href.lower() and href not in seen_hrefs:
                seen_hrefs.add(href)
                count += 1
        return min(count, 30)

    def analyze_news(
        self,
        company_id: UUID,
        ticker: str,
        html: Optional[str],
    ) -> Optional[ExternalSignalCreate]:
        """Analyze news page HTML; return one ExternalSignalCreate or None."""
        if not html or len(html.strip()) < 100:
            return None
        soup = BeautifulSoup(html, "html.parser")
        page_text = self._extract_text(html)
        if len(page_text) < MIN_PAGE_LENGTH:
            logger.debug("news_analyze_skipped reason=page_too_short length=%s", len(page_text))
            return None
        article_count = self._count_article_like_elements(soup)
        page_length = len(page_text)
        base_score = min(article_count, 20) * 5.0
        lower = page_text.lower()
        keyword_hits = sum(1 for k in KEYWORD_BONUS_TERMS if k in lower)
        keyword_bonus = min(keyword_hits * 5.0, KEYWORD_BONUS_MAX)
        normalized_score = min(base_score + keyword_bonus, 100.0)
        raw_value = f"article_count={article_count}, page_length={page_length}, keyword_hits={keyword_hits}"
        metadata = {"article_count": article_count, "page_length": page_length, "keyword_hits": keyword_hits}
        return ExternalSignalCreate(
            company_id=company_id,
            category=SignalCategory.DIGITAL_PRESENCE,
            source=SignalSource.COMPANY_NEWS,
            signal_date=datetime.now(timezone.utc),
            raw_value=raw_value[:500],
            normalized_score=round(normalized_score, 1),
            confidence=0.75,
            metadata=metadata,
        )


# --- Unified collector ---

class DigitalPresenceCollector:
    """Runs both BuiltWith and company news; returns all signals and combined score (max of both)."""

    def __init__(self):
        self.tech_collector = TechStackCollector()
        self.news_collector = NewsSignalCollector()

    def collect(
        self,
        company_id: UUID,
        ticker: str,
        domain: str,
        news_url: Optional[str] = None,
        builtwith_api_key: Optional[str] = None,
    ) -> tuple[list[ExternalSignalCreate], float]:
        """
        Run BuiltWith (if domain + API key) and news (if news_url). Insert order: BuiltWith first, then news.
        Returns (list of signals to insert, combined_score = max of both scores or 0).
        """
        signals: list[ExternalSignalCreate] = []
        combined_score = 0.0

        # BuiltWith
        if domain and builtwith_api_key and builtwith_api_key.strip():
            techs = self.tech_collector.fetch_tech_stack(domain, api_key=builtwith_api_key)
            if techs:
                tech_signal = self.tech_collector.analyze_tech_stack(company_id, techs)
                signals.append(tech_signal)
                combined_score = max(combined_score, tech_signal.normalized_score)

        # News
        if news_url and isinstance(news_url, str) and news_url.strip():
            html = self.news_collector.fetch_news_page(news_url)
            news_signal = self.news_collector.analyze_news(company_id, ticker, html)
            if news_signal and news_signal.normalized_score > 0:
                signals.append(news_signal)
                combined_score = max(combined_score, news_signal.normalized_score)

        return signals, combined_score
