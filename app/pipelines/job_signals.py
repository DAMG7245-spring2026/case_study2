"""Job posting signal collector for AI hiring analysis."""

import logging
import re
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import httpx

from app.models.signal import (
    ExternalSignalCreate,
    JobPosting,
    SignalCategory,
    SignalSource,
)

logger = logging.getLogger(__name__)


class JobSignalCollector:
    """Collect and analyze job posting signals for AI hiring."""

    # Keywords indicating AI/ML related positions
    AI_KEYWORDS = [
        "machine learning", "ml engineer", "data scientist",
        "artificial intelligence", "deep learning", "nlp",
        "computer vision", "mlops", "ai engineer",
        "pytorch", "tensorflow", "llm", "large language model",
        "generative ai", "neural network", "data engineer"
    ]

    # Skills that indicate AI/ML capabilities
    AI_SKILLS = [
        "python", "pytorch", "tensorflow", "scikit-learn",
        "spark", "hadoop", "kubernetes", "docker",
        "aws sagemaker", "azure ml", "gcp vertex",
        "huggingface", "langchain", "openai",
        "pandas", "numpy", "sql", "databricks"
    ]

    # Only keep jobs posted within this many days (SerpApi often returns "X days ago")
    RECENT_DAYS = 7

    def __init__(self):
        self.client = httpx.Client(
            timeout=30.0,
            headers={"User-Agent": "Mozilla/5.0 (compatible; research)"}
        )

    def _posted_within_days(self, posted_str: Optional[str], days: float = 7) -> bool:
        """True if posted_str indicates the job was posted within the last `days` days, or if unparseable (keep)."""
        if not posted_str or not isinstance(posted_str, str):
            return True
        s = posted_str.strip().lower()
        if s in ("just posted", "today"):
            return True
        if s == "yesterday":
            return days >= 1
        # "N hours ago" or "N hour ago"
        m = re.match(r"(\d+)\s*hours?\s*ago", s)
        if m:
            return int(m.group(1)) <= (days * 24)
        # "N days ago" or "N day ago"
        m = re.match(r"(\d+)\s*days?\s*ago", s)
        if m:
            return int(m.group(1)) <= days
        # "1 week ago", "2 weeks ago"
        m = re.match(r"(\d+)\s*weeks?\s*ago", s)
        if m:
            return int(m.group(1)) * 7 <= days
        # "N+ days ago", "30+ days ago", "1 month ago" etc. -> exclude
        if re.search(r"\d+\s*\+\s*days?\s*ago", s) or "month" in s or "year" in s:
            return False
        return True  # unparseable: keep

    def fetch_postings(self, company_name: str, api_key: str | None = None) -> list["JobPosting"]:
        """Fetch job postings from SerpApi (Google Jobs). Returns [] if no key or on failure."""
        if not api_key or not api_key.strip():
            logger.debug("job_fetch_skipped", reason="no_api_key", company=company_name)
            return []
        try:
            url = "https://serpapi.com/search.json"
            params = {
                "engine": "google_jobs",
                "q": f"{company_name} jobs",
                "api_key": api_key,
            }
            r = self.client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            jobs_data = data.get("jobs_results") or data.get("organic_results") or []
            postings = []
            for j in jobs_data[:50]:
                title = j.get("title") or ""
                desc = j.get("description") or j.get("snippet") or ""
                company = j.get("company_name") or company_name
                loc = (j.get("location") or [""])[0] if isinstance(j.get("location"), list) else (j.get("location") or "")
                link = j.get("link") or j.get("apply_link") or ""
                # Use posted_at if it's a string, else date if available, else None
                posted_at_val = j.get("posted_at")
                date_val = j.get("date")
                if isinstance(posted_at_val, str):
                    posted = posted_at_val
                elif isinstance(date_val, str):
                    posted = date_val
                else:
                    posted = None
                if not title and not desc:
                    continue
                if not self._posted_within_days(posted, self.RECENT_DAYS):
                    continue
                p = JobPosting(
                    title=title,
                    company=company,
                    location=loc,
                    description=desc,
                    posted_date=posted,
                    source="serpapi_google_jobs",
                    url=link,
                    is_ai_related=False,
                    ai_skills=[],
                )
                p = self.classify_posting(p)
                postings.append(p)
            logger.info("job_fetch_ok", company=company_name, count=len(postings))
            return postings
        except Exception as e:
            logger.warning("job_fetch_failed", company=company_name, error=str(e))
            return []

    def analyze_job_postings(
        self,
        company: str,
        postings: list[JobPosting],
        company_id: Optional[UUID] = None
    ) -> ExternalSignalCreate:
        """
        Analyze job postings to calculate hiring signal score.
        Scoring (0-100): AI job ratio, skill diversity, volume bonus.
        """
        classified = [self.classify_posting(p) for p in postings]
        total_tech_jobs = len([p for p in classified if self._is_tech_job(p)])
        ai_jobs = len([p for p in classified if p.is_ai_related])

        ai_ratio = ai_jobs / total_tech_jobs if total_tech_jobs > 0 else 0
        all_skills = set()
        for posting in classified:
            all_skills.update(posting.ai_skills)

        score = (
            min(ai_ratio * 60, 60) +
            min(len(all_skills) / 10, 1) * 20 +
            min(ai_jobs / 5, 1) * 20
        )
        confidence = min(0.5 + total_tech_jobs / 100, 0.95)

        return ExternalSignalCreate(
            company_id=company_id or UUID("00000000-0000-0000-0000-000000000000"),
            category=SignalCategory.TECHNOLOGY_HIRING,
            source=SignalSource.INDEED,
            signal_date=datetime.now(timezone.utc),
            raw_value=f"{ai_jobs}/{total_tech_jobs} AI jobs",
            normalized_score=round(score, 1),
            confidence=round(confidence, 3),
            metadata={
                "company": company,
                "total_tech_jobs": total_tech_jobs,
                "ai_jobs": ai_jobs,
                "ai_ratio": round(ai_ratio, 3),
                "skills_found": list(all_skills),
                "total_postings_analyzed": len(postings)
            }
        )

    def classify_posting(self, posting: JobPosting) -> JobPosting:
        """Classify a job posting as AI-related or not."""
        text = f"{posting.title} {posting.description}".lower()

        # Check for AI keywords
        is_ai = any(kw in text for kw in self.AI_KEYWORDS)

        # Extract AI skills
        skills = [skill for skill in self.AI_SKILLS if skill in text]

        posting.is_ai_related = is_ai
        posting.ai_skills = skills

        return posting

    def _is_tech_job(self, posting: JobPosting) -> bool:
        """Check if posting is a technology job."""
        tech_keywords = [
            "engineer", "developer", "programmer", "software",
            "data", "analyst", "scientist", "technical",
            "architect", "devops", "sre", "platform"
        ]
        title_lower = posting.title.lower()
        return any(kw in title_lower for kw in tech_keywords)

    def create_sample_postings(self, company: str, ai_focus: str = "medium") -> list[JobPosting]:
        """
        Create sample job postings for testing/demo purposes.
        
        Args:
            company: Company name
            ai_focus: "low", "medium", or "high" AI hiring focus
        """
        base_postings = [
            JobPosting(
                title="Software Engineer",
                company=company,
                location="Remote",
                description="Build scalable web applications using Python and JavaScript.",
                source="sample",
                url="https://example.com/job/1"
            ),
            JobPosting(
                title="Data Analyst",
                company=company,
                location="New York, NY",
                description="Analyze business data using SQL and Python. Create dashboards.",
                source="sample",
                url="https://example.com/job/2"
            ),
            JobPosting(
                title="Product Manager",
                company=company,
                location="San Francisco, CA",
                description="Lead product strategy and roadmap for enterprise software.",
                source="sample",
                url="https://example.com/job/3"
            ),
        ]

        ai_postings = [
            JobPosting(
                title="Machine Learning Engineer",
                company=company,
                location="Remote",
                description="Build ML models using PyTorch and TensorFlow. Deploy on AWS SageMaker.",
                source="sample",
                url="https://example.com/job/4"
            ),
            JobPosting(
                title="Data Scientist - NLP",
                company=company,
                location="Seattle, WA",
                description="Develop NLP models using HuggingFace transformers and LangChain.",
                source="sample",
                url="https://example.com/job/5"
            ),
            JobPosting(
                title="AI Engineer",
                company=company,
                location="Austin, TX",
                description="Build generative AI applications using OpenAI and LLM technologies.",
                source="sample",
                url="https://example.com/job/6"
            ),
            JobPosting(
                title="MLOps Engineer",
                company=company,
                location="Remote",
                description="Deploy and monitor ML models. Kubernetes, Docker, MLflow expertise.",
                source="sample",
                url="https://example.com/job/7"
            ),
        ]

        if ai_focus == "low":
            return base_postings
        elif ai_focus == "medium":
            return base_postings + ai_postings[:2]
        else:  # high
            return base_postings + ai_postings

    def __del__(self):
        """Cleanup HTTP client."""
        if hasattr(self, 'client'):
            self.client.close()