"""Job posting signal collector."""
import httpx
import structlog
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID
from app.models.signal import ExternalSignal, SignalCategory, SignalSource

logger = structlog.get_logger()


@dataclass
class JobPosting:
    """Represents a job posting."""
    title: str
    company: str
    location: str
    description: str
    posted_date: str | None
    source: str
    url: str
    is_ai_related: bool
    ai_skills: list[str]


class JobSignalCollector:
    """Collect job posting signals for AI hiring."""

    AI_KEYWORDS = [
        "machine learning", "ml engineer", "data scientist",
        "artificial intelligence", "deep learning", "nlp",
        "computer vision", "mlops", "ai engineer",
        "pytorch", "tensorflow", "llm", "large language model"
    ]

    AI_SKILLS = [
        "python", "pytorch", "tensorflow", "scikit-learn",
        "spark", "hadoop", "kubernetes", "docker",
        "aws sagemaker", "azure ml", "gcp vertex",
        "huggingface", "langchain", "openai"
    ]

    def __init__(self):
        self.client = httpx.Client(
            timeout=30.0,
            headers={"User-Agent": "Mozilla/5.0 (compatible; research)"}
        )

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
                posted = j.get("posted_at") or j.get("date") if isinstance(j.get("posted_at"), str) else None
                if not title and not desc:
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
        company_id: UUID,
        postings: list[JobPosting]
    ) -> ExternalSignal:
        """Analyze job postings to calculate hiring signal."""

        total_tech_jobs = len([p for p in postings if self._is_tech_job(p)])
        ai_jobs = len([p for p in postings if p.is_ai_related])

        # Calculate metrics
        if total_tech_jobs > 0:
            ai_ratio = ai_jobs / total_tech_jobs
        else:
            ai_ratio = 0

        # Collect all AI skills mentioned
        all_skills = set()
        for posting in postings:
            all_skills.update(posting.ai_skills)

        # Score calculation (0-100)
        # - Base: AI ratio * 60 (max 60 points)
        # - Skill diversity: len(skills) / 10 * 20 (max 20 points)
        # - Volume bonus: min(ai_jobs / 5, 1) * 20 (max 20 points)
        score = (
            min(ai_ratio * 60, 60) +
            min(len(all_skills) / 10, 1) * 20 +
            min(ai_jobs / 5, 1) * 20
        )

        return ExternalSignal(
            company_id=company_id,
            category=SignalCategory.TECHNOLOGY_HIRING,
            source=SignalSource.INDEED,
            signal_date=datetime.now(timezone.utc),
            raw_value=f"{ai_jobs}/{total_tech_jobs} AI jobs",
            normalized_score=round(score, 1),
            confidence=min(0.5 + total_tech_jobs / 100, 0.95),
            metadata={
                "total_tech_jobs": total_tech_jobs,
                "ai_jobs": ai_jobs,
                "ai_ratio": round(ai_ratio, 3),
                "skills_found": list(all_skills)
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
            "data", "analyst", "scientist", "technical"
        ]
        title_lower = posting.title.lower()
        return any(kw in title_lower for kw in tech_keywords)
