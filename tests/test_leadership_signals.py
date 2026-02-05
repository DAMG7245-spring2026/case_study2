"""Tests for leadership signal collector."""

import pytest
from uuid import uuid4

from app.models.signal import SignalCategory, SignalSource
from app.pipelines.leadership_signals import LeadershipSignalCollector


def test_analyze_leadership_returns_empty_when_no_data():
    """When neither website nor linkedin data is provided, analyze_leadership returns empty list."""
    collector = LeadershipSignalCollector()
    company_id = uuid4()
    signals = collector.analyze_leadership(company_id, website_data=None, linkedin_data=None)
    assert signals == []


def test_analyze_leadership_website_only():
    """With website data only, returns one signal with COMPANY_WEBSITE source and score 0-100."""
    collector = LeadershipSignalCollector()
    company_id = uuid4()
    website_data = {
        "text": "Our executive team and CEO are committed to AI and digital transformation. "
                "The board and leadership drive technology innovation.",
        "url": "https://example.com/about",
    }
    signals = collector.analyze_leadership(company_id, website_data=website_data, linkedin_data=None)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.company_id == company_id
    assert sig.category == SignalCategory.LEADERSHIP_SIGNALS
    assert sig.source == SignalSource.COMPANY_WEBSITE
    assert 0 <= sig.normalized_score <= 100
    assert "leadership_mentions" in sig.raw_value
    assert "commitment_mentions" in sig.raw_value
    assert sig.metadata.get("url") == "https://example.com/about"


def test_analyze_leadership_website_and_linkedin():
    """With both website and linkedin data, returns two signals."""
    collector = LeadershipSignalCollector()
    company_id = uuid4()
    website_data = {"text": "Our CEO and board focus on innovation and technology.", "url": "https://example.com/about"}
    linkedin_data = {"text": "Company leadership is committed to AI and digital transformation."}
    signals = collector.analyze_leadership(
        company_id, website_data=website_data, linkedin_data=linkedin_data
    )
    assert len(signals) == 2
    sources = {s.source for s in signals}
    assert sources == {SignalSource.COMPANY_WEBSITE, SignalSource.LINKEDIN}
    for sig in signals:
        assert sig.category == SignalCategory.LEADERSHIP_SIGNALS
        assert 0 <= sig.normalized_score <= 100


def test_analyze_leadership_empty_text_ignored():
    """Website or linkedin entry with no text does not produce a signal."""
    collector = LeadershipSignalCollector()
    company_id = uuid4()
    signals = collector.analyze_leadership(
        company_id,
        website_data={"text": "", "url": "https://x.com"},
        linkedin_data={"text": ""},
    )
    assert signals == []


def test_score_leadership_text_heuristic():
    """_score_leadership_text produces higher score for more keyword matches."""
    collector = LeadershipSignalCollector()
    low_text = "We are a company."
    high_text = (
        "Our executive team, CEO, chief officers, and board are committed to "
        "AI, digital transformation, technology, innovation, and machine learning."
    )
    low_score, _, _ = collector._score_leadership_text(low_text)
    high_score, _, _ = collector._score_leadership_text(high_text)
    assert high_score > low_score
    assert 0 <= low_score <= 100
    assert 0 <= high_score <= 100
