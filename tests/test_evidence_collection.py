import pytest
from uuid import uuid4
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, AsyncMock
from app.main import app
from app.services.snowflake import get_snowflake_service

@pytest.fixture
def override_snowflake(mock_snowflake):
    app.dependency_overrides[get_snowflake_service] = lambda: mock_snowflake
    # Also patch where it's used directly (not via Depends)
    with patch("app.routers.documents.get_snowflake_service", return_value=mock_snowflake):
        with patch("app.routers.signals.get_snowflake_service", return_value=mock_snowflake):
            with patch("app.routers.evidence.get_snowflake_service", return_value=mock_snowflake):
                yield mock_snowflake
    app.dependency_overrides.clear()

class TestDocumentEndpoints:
    """Tests for document collection and retrieval."""

    def test_collect_documents_endpoint(self, client, override_snowflake):
        """Test document collection trigger."""
        company_id = str(uuid4())
        request_data = {
            "company_id": company_id,
            "filing_types": ["10-K"],
            "years_back": 1
        }
        
        # Mock background task dependencies
        override_snowflake.execute_one.return_value = {"ticker": "AAPL", "name": "Apple"}
        
        with patch("app.routers.documents.SECEdgarPipeline") as mock_pipeline:
            mock_pipeline.return_value.download_filings.return_value = []
            response = client.post("/api/v1/documents/collect", json=request_data)
        
        assert response.status_code == 200
        data = response.json()
        assert "task_id" in data
        assert data["status"] == "queued"

    def test_list_documents(self, client, override_snowflake):
        """Test listing documents."""
        company_id = str(uuid4())
        override_snowflake.execute_query.return_value = [
            {
                "id": str(uuid4()),
                "company_id": company_id,
                "ticker": "AAPL",
                "filing_type": "10-K",
                "filing_date": datetime.now(timezone.utc).isoformat(),
                "status": "parsed",
                "created_at": datetime.now(timezone.utc).isoformat()
            }
        ]
        
        response = client.get(f"/api/v1/documents?company_id={company_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["ticker"] == "AAPL"

class TestSignalEndpoints:
    """Tests for external signal endpoints."""

    def test_collect_signals_endpoint(self, client, override_snowflake):
        """Test signal collection trigger."""
        company_id = str(uuid4())
        override_snowflake.execute_one.return_value = {"ticker": "AAPL", "name": "Apple"}
        
        with patch("app.pipelines.JobSignalCollector"), \
             patch("app.pipelines.DigitalPresenceCollector") as mock_dp, \
             patch("app.pipelines.PatentSignalCollector"), \
             patch("app.pipelines.LeadershipSignalCollector"):
            mock_dp.return_value.collect.return_value = ([], 0.0)
            response = client.post("/api/v1/signals/collect", json={"company_id": company_id})
        
        assert response.status_code == 200
        data = response.json()
        assert "task_id" in data
        assert data["status"] == "queued"

    def test_get_signal_summary(self, client, override_snowflake):
        """Test getting signal summary."""
        company_id = str(uuid4())
        override_snowflake.execute_one.return_value = {
            "company_id": company_id,
            "ticker": "AAPL",
            "technology_hiring_score": 80.0,
            "innovation_activity_score": 75.0,
            "digital_presence_score": 85.0,
            "leadership_signals_score": 70.0,
            "composite_score": 78.0,
            "signal_count": 10,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        response = client.get(f"/api/v1/signals/summary/{company_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["ticker"] == "AAPL"
        assert data["composite_score"] == 78.0

class TestEvidenceEndpoints:
    """Tests for unified evidence endpoints."""

    def test_get_company_evidence(self, client, override_snowflake):
        """Test getting all evidence for a company."""
        company_id = str(uuid4())
        override_snowflake.execute_query.side_effect = [
            [{"id": "doc1", "company_id": company_id, "ticker": "AAPL", "filing_type": "10-K", "filing_date": datetime.now(timezone.utc), "status": "parsed", "created_at": datetime.now(timezone.utc)}], # docs
            [{"id": str(uuid4()), "company_id": company_id, "category": "technology_hiring", "source": "indeed", "signal_date": datetime.now(timezone.utc), "raw_value": "10 jobs", "normalized_score": 80.0, "confidence": 0.9, "metadata": {}, "created_at": datetime.now(timezone.utc)}] # signals
        ]
        override_snowflake.execute_one.return_value = {"company_id": company_id, "ticker": "AAPL", "composite_score": 75.0} # summary
        
        response = client.get(f"/api/v1/evidence/{company_id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["document_count"] == 1
        assert data["signal_count"] == 1
        assert data["summary"]["composite_score"] == 75.0
