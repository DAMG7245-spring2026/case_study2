"""Tests for leadership signal collector."""

import pytest
from unittest.mock import MagicMock, patch
from uuid import uuid4

from app.models.signal import SignalCategory, SignalSource
from app.pipelines.leadership_signals import LeadershipSignalCollector, MIN_TEXT_LENGTH


def test_analyze_leadership_returns_empty_when_no_data():
    """When no website data is provided, analyze_leadership returns empty list."""
    collector = LeadershipSignalCollector()
    company_id = uuid4()
    signals = collector.analyze_leadership(company_id, website_data=None)
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
    signals = collector.analyze_leadership(company_id, website_data=website_data)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.company_id == company_id
    assert sig.category == SignalCategory.LEADERSHIP_SIGNALS
    assert sig.source == SignalSource.COMPANY_WEBSITE
    assert 0 <= sig.normalized_score <= 100
    assert "leadership_mentions" in sig.raw_value
    assert "commitment_mentions" in sig.raw_value
    assert sig.metadata.get("url") == "https://example.com/about"


def test_analyze_leadership_website_only_produces_single_signal():
    """With website data only, returns exactly one signal with COMPANY_WEBSITE source."""
    collector = LeadershipSignalCollector()
    company_id = uuid4()
    website_data = {"text": "Our CEO and board focus on innovation and technology.", "url": "https://example.com/about"}
    signals = collector.analyze_leadership(company_id, website_data=website_data)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.source == SignalSource.COMPANY_WEBSITE
    assert sig.category == SignalCategory.LEADERSHIP_SIGNALS
    assert 0 <= sig.normalized_score <= 100


def test_analyze_leadership_empty_text_ignored():
    """Website entry with no text does not produce a signal."""
    collector = LeadershipSignalCollector()
    company_id = uuid4()
    signals = collector.analyze_leadership(
        company_id,
        website_data={"text": "", "url": "https://x.com"},
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


def test_fetch_leadership_page_returns_none_for_empty_url():
    """fetch_leadership_page returns None when url is empty."""
    collector = LeadershipSignalCollector()
    assert collector.fetch_leadership_page("") is None
    assert collector.fetch_leadership_page(None) is None


def test_fetch_leadership_page_returns_dict_when_html_has_enough_text():
    """fetch_leadership_page returns {text, url} when GET returns 200 and extract_text yields enough text."""
    collector = LeadershipSignalCollector()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html></html>"
    long_text = "Our executive team, CEO, and board are committed to AI and digital transformation. " + "More content. " * 20
    with patch.object(collector.client, "get", return_value=mock_response), \
         patch.object(collector, "_extract_text", return_value=long_text):
        result = collector.fetch_leadership_page("https://example.com/leadership")
    assert result is not None
    assert "text" in result and "url" in result
    assert result["url"] == "https://example.com/leadership"
    assert result["text"] == long_text
    assert len(result["text"]) >= MIN_TEXT_LENGTH
