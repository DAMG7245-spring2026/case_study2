"""Leadership (executive commitment) signal collector from company website."""

import logging
import re
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import httpx
from bs4 import BeautifulSoup

from app.models.signal import (
    ExternalSignalCreate,
    SignalCategory,
    SignalSource,
)

logger = logging.getLogger(__name__)


# Paths to try for leadership/about content (in order). Fallback to "/" if none work.
LEADERSHIP_PATHS = [
    "/about", "/about-us", "/about_us", "/about/corporate",
    "/leadership", "/leadership-team", "/our-team", "/executive-team",
    "/company", "/our-company", "/who-we-are", "/about/leadership",
]
MIN_TEXT_LENGTH = 80  # minimum chars to accept a page (reject empty/JS-only)


# Keywords indicating leadership/executive content
LEADERSHIP_KEYWORDS = [
    "executive", "ceo", "chief", "cfo", "cto", "board", "leadership",
    "management", "officer", "president", "director", "governance",
]

# Keywords indicating AI/digital/transformation commitment
COMMITMENT_KEYWORDS = [
    "ai", "artificial intelligence", "digital", "technology", "transformation",
    "innovation", "data", "automation", "machine learning", "cloud",
]

# Scoring: points per keyword (tunable). Combined score 0–100; no single hardcoded output.
POINTS_PER_LEADERSHIP_KEYWORD = 5   # need 10+ leadership keywords to reach 50 from this dimension
POINTS_PER_COMMITMENT_KEYWORD = 5   # need 10+ commitment keywords to reach 50 from this dimension
MAX_LEADERSHIP_POINTS = 50.0
MAX_COMMITMENT_POINTS = 50.0
MAX_TOTAL_SCORE = 100.0


class LeadershipSignalCollector:
    """Collect and score leadership (executive commitment) signals from company website."""

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

    def fetch_leadership_page(self, url: str) -> Optional[dict]:
        """
        Fetch a single leadership page by URL (e.g. company-specific /about/leadership).
        Returns {"text": str, "url": str} if successful and text is long enough, else None.
        """
        url = (url or "").strip()
        if not url:
            return None
        if not url.startswith("http"):
            url = f"https://{url}"
        try:
            r = self.client.get(url)
            if r.status_code != 200:
                logger.debug("leadership_fetch_failed url=%s status=%s", url, r.status_code)
                return None
            text = self._extract_text(r.text)
            if not text or len(text) < MIN_TEXT_LENGTH:
                logger.debug("leadership_fetch_too_short url=%s length=%s", url, len(text) if text else 0)
                return None
            logger.info("leadership_fetch_ok source=leadership_url url=%s length=%s", url, len(text))
            return {"text": text, "url": url}
        except Exception as e:
            logger.warning("leadership_fetch_failed url=%s error=%s", url, str(e))
            return None

    def fetch_from_company_website(self, domain: str) -> Optional[dict]:
        """
        Fetch leadership/about page content from company domain.
        Tries several common paths, then falls back to homepage (/). Returns None if domain is empty or all fail.
        """
        domain = (domain or "").strip().lower()
        if not domain:
            logger.info("leadership_fetch_skipped reason=no_domain")
            return None
        if not domain.startswith("http"):
            base = f"https://{domain}"
        else:
            base = domain.rstrip("/")

        paths_to_try = LEADERSHIP_PATHS + ["/"]  # add homepage as fallback
        last_status = None
        last_error = None

        for path in paths_to_try:
            url = f"{base}{path}" if path != "/" else base
            try:
                r = self.client.get(url)
                last_status = r.status_code
                if r.status_code != 200:
                    continue
                text = self._extract_text(r.text)
                if not text or len(text) < MIN_TEXT_LENGTH:
                    continue
                logger.info(
                    "leadership_fetch_ok source=company_website url=%s length=%s domain=%s",
                    url, len(text), domain,
                )
                return {"text": text, "url": url}
            except Exception as e:
                last_error = str(e)
                logger.debug("leadership_fetch_try_failed url=%s error=%s", url, last_error)
                continue

        logger.info(
            "leadership_fetch_no_page domain=%s last_status=%s last_error=%s",
            domain, last_status, last_error or "all paths failed or too little text",
        )
        return None

    def _extract_text(self, html: str) -> str:
        """Extract main text from HTML, strip scripts/styles."""
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return re.sub(r"\s+", " ", text)

    def _score_leadership_text(self, text: str) -> tuple[float, str, dict]:
        """
        Score text for leadership + commitment keywords. Returns (score 0-100, raw_value, metadata).
        """
        lower = text.lower()
        leadership_count = sum(1 for k in LEADERSHIP_KEYWORDS if k in lower)
        commitment_count = sum(1 for k in COMMITMENT_KEYWORDS if k in lower)

        # Heuristic: both dimensions contribute; use constants so score is not hardcoded
        leadership_score = min(
            leadership_count * POINTS_PER_LEADERSHIP_KEYWORD, MAX_LEADERSHIP_POINTS
        )
        commitment_score = min(
            commitment_count * POINTS_PER_COMMITMENT_KEYWORD, MAX_COMMITMENT_POINTS
        )
        score = min(leadership_score + commitment_score, MAX_TOTAL_SCORE)

        raw = f"leadership_mentions={leadership_count}, commitment_mentions={commitment_count}"
        metadata = {
            "leadership_keyword_count": leadership_count,
            "commitment_keyword_count": commitment_count,
            "text_length": len(text),
        }
        return round(score, 1), raw, metadata

    def analyze_leadership(
        self,
        company_id: UUID,
        website_data: Optional[dict] = None,
    ) -> list[ExternalSignalCreate]:
        """
        Produce up to one ExternalSignalCreate from company website data.
        Signal has category=LEADERSHIP_SIGNALS and source=COMPANY_WEBSITE.
        """
        signals: list[ExternalSignalCreate] = []
        now = datetime.now(timezone.utc)

        if website_data and website_data.get("text"):
            score, raw_value, meta = self._score_leadership_text(website_data["text"])
            meta["url"] = website_data.get("url", "")
            signals.append(
                ExternalSignalCreate(
                    company_id=company_id,
                    category=SignalCategory.LEADERSHIP_SIGNALS,
                    source=SignalSource.COMPANY_WEBSITE,
                    signal_date=now,
                    raw_value=raw_value[:500],
                    normalized_score=score,
                    confidence=0.75,
                    metadata=meta,
                )
            )

        return signals
