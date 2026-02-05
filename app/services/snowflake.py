"""Snowflake database service."""
import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional
from uuid import UUID
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from app.config import get_settings

logger = logging.getLogger(__name__)


class SnowflakeService:
    """Service for Snowflake database operations."""
    
    def __init__(self):
        self.settings = get_settings()
        self._connection: Optional[SnowflakeConnection] = None
    
    def _get_connection_params(self) -> dict[str, Any]:
        """Get connection parameters."""
        return {
            "account": self.settings.snowflake_account,
            "user": self.settings.snowflake_user,
            "password": self.settings.snowflake_password,
            "database": self.settings.snowflake_database,
            "schema": self.settings.snowflake_schema,
            "warehouse": self.settings.snowflake_warehouse,
        }
    
    def connect(self) -> SnowflakeConnection:
        """Establish connection to Snowflake."""
        if self._connection is None or self._connection.is_closed():
            self._connection = snowflake.connector.connect(
                **self._get_connection_params()
            )
        return self._connection
    
    def disconnect(self) -> None:
        """Close the Snowflake connection."""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            self._connection = None
    
    @contextmanager
    def cursor(self) -> Generator[SnowflakeCursor, None, None]:
        """Context manager for database cursor."""
        conn = self.connect()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cur.close()
    
    async def health_check(self) -> tuple[bool, Optional[str]]:
        """Check if Snowflake connection is healthy."""
        try:
            with self.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                return result is not None, None
        except Exception as e:
            return False, str(e)
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[tuple] = None
    ) -> list[dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        with self.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0].lower() for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]
    
    def execute_one(
        self, 
        query: str, 
        params: Optional[tuple] = None
    ) -> Optional[dict[str, Any]]:
        """Execute a query and return single result."""
        results = self.execute_query(query, params)
        return results[0] if results else None
    
    def execute_write(
        self, 
        query: str, 
        params: Optional[tuple] = None
    ) -> int:
        """Execute an INSERT/UPDATE/DELETE and return affected rows."""
        with self.cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount

    async def get_or_create_company(self, ticker: str, name: str, sector: str) -> Any:
        """Get a company by ticker or create it if it doesn't exist."""
        # First, check if industry exists for this sector
        industry_query = "SELECT id FROM industries WHERE name = %s"
        industry = self.execute_one(industry_query, (sector,))
        
        if not industry:
            # Create industry if it doesn't exist (using a default HR base for now)
            from uuid import uuid4
            industry_id = str(uuid4())
            insert_industry = """
                INSERT INTO industries (id, name, sector, h_r_base)
                VALUES (%s, %s, %s, %s)
            """
            self.execute_write(insert_industry, (industry_id, sector, sector, 75.0))
        else:
            industry_id = industry["id"]

        # Check if company exists
        company_query = "SELECT * FROM companies WHERE ticker = %s"
        company = self.execute_one(company_query, (ticker,))
        
        if not company:
            from uuid import uuid4
            from app.models.company import CompanyResponse
            company_id = str(uuid4())
            insert_company = """
                INSERT INTO companies (id, name, ticker, industry_id)
                VALUES (%s, %s, %s, %s)
            """
            self.execute_write(insert_company, (company_id, name, ticker, industry_id))
            company = self.execute_one(company_query, (ticker,))
        
        # Return as an object that has an 'id' attribute to match script expectations
        from dataclasses import dataclass
        @dataclass
        class CompanyStub:
            id: UUID
            ticker: str
            name: str
        
        return CompanyStub(id=UUID(company["id"]), ticker=company["ticker"], name=company["name"])

    async def insert_document(self, doc: Any) -> str:
        """Insert a document record into Snowflake and return the document ID."""
        # Check if document with same hash already exists
        existing = self.execute_one(
            "SELECT id FROM documents WHERE content_hash = %s",
            (doc.content_hash,)
        )
        if existing:
            return existing["id"]

        from uuid import uuid4
        doc_id = str(uuid4())
        
        query = """
            INSERT INTO documents (
                id, company_id, ticker, filing_type, filing_date, 
                source_url, local_path, content_hash, word_count, status,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        params = (
            doc_id,
            str(doc.company_id),
            doc.company_ticker if hasattr(doc, 'company_ticker') else doc.ticker,
            doc.filing_type,
            doc.filing_date,
            doc.source_path if hasattr(doc, 'source_path') else None,
            doc.source_path if hasattr(doc, 'source_path') else None,
            doc.content_hash,
            doc.word_count,
            "parsed"
        )
        self.execute_write(query, params)
        return doc_id

    async def insert_chunks(self, document_id: str, chunks: list[Any]) -> None:
        """Insert document chunks into Snowflake."""
        if not chunks:
            return
        
        # Check if chunks for this document already exist
        existing = self.execute_one(
            "SELECT count(*) as count FROM document_chunks WHERE document_id = %s",
            (document_id,)
        )
        if existing and existing["count"] > 0:
            return
            
        query = """
            INSERT INTO document_chunks (
                id, document_id, chunk_index, content, 
                section, start_char, end_char, word_count,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        
        from uuid import uuid4
        for chunk in chunks:
            params = (
                str(uuid4()),
                document_id,
                chunk.chunk_index,
                chunk.content,
                chunk.section,
                chunk.start_char,
                chunk.end_char,
                chunk.word_count
            )
            self.execute_write(query, params)

    async def insert_signal(self, signal: Any) -> None:
        """Insert an external signal into Snowflake."""
        # Check if signal with same ID already exists
        existing = self.execute_one(
            "SELECT id FROM external_signals WHERE id = %s",
            (str(signal.id),)
        )
        if existing:
            return

        import json
        query = """
            INSERT INTO external_signals (
                id, company_id, category, source, signal_date, 
                raw_value, normalized_score, confidence, metadata,
                created_at
            ) SELECT %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()
        """
        params = (
            str(signal.id),
            str(signal.company_id),
            signal.category.value if hasattr(signal.category, 'value') else signal.category,
            signal.source.value if hasattr(signal.source, 'value') else signal.source,
            signal.signal_date,
            signal.raw_value,
            signal.normalized_score,
            signal.confidence,
            json.dumps(signal.metadata)
        )
        self.execute_write(query, params)

    async def update_signal_summary(self, company_id: UUID) -> None:
        """Update the signal summary for a company."""
        # This is a simplified version that aggregates existing signals
        # In a real Snowflake environment, this might be a materialized view or a more complex query
        
        # Get all signals for the company
        signals_query = "SELECT category, normalized_score FROM external_signals WHERE company_id = %s"
        signals = self.execute_query(signals_query, (str(company_id),))
        
        if not signals:
            return

        # Calculate scores by category
        scores = {
            "technology_hiring": 0.0,
            "innovation_activity": 0.0,
            "digital_presence": 0.0,
            "leadership_signals": 0.0
        }
        counts = {cat: 0 for cat in scores}
        
        for s in signals:
            cat = s["category"]
            if cat in scores:
                scores[cat] += float(s["normalized_score"])
                counts[cat] += 1
        
        # Average the scores
        for cat in scores:
            if counts[cat] > 0:
                scores[cat] /= counts[cat]

        # Calculate composite
        composite_score = (
            0.30 * scores["technology_hiring"] +
            0.25 * scores["innovation_activity"] +
            0.25 * scores["digital_presence"] +
            0.20 * scores["leadership_signals"]
        )

        # Get ticker
        ticker_query = "SELECT ticker FROM companies WHERE id = %s"
        company = self.execute_one(ticker_query, (str(company_id),))
        ticker = company["ticker"] if company else "UNKNOWN"

        # Update or insert summary
        update_query = """
            MERGE INTO company_signal_summaries AS target
            USING (SELECT %s AS company_id) AS source
            ON target.company_id = source.company_id
            WHEN MATCHED THEN
                UPDATE SET 
                    technology_hiring_score = %s,
                    innovation_activity_score = %s,
                    digital_presence_score = %s,
                    leadership_signals_score = %s,
                    composite_score = %s,
                    signal_count = %s,
                    last_updated = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    company_id, ticker, technology_hiring_score, 
                    innovation_activity_score, digital_presence_score, 
                    leadership_signals_score, composite_score, 
                    signal_count, last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
        """
        params = (
            str(company_id),
            scores["technology_hiring"],
            scores["innovation_activity"],
            scores["digital_presence"],
            scores["leadership_signals"],
            composite_score,
            len(signals),
            str(company_id),
            ticker,
            scores["technology_hiring"],
            scores["innovation_activity"],
            scores["digital_presence"],
            scores["leadership_signals"],
            composite_score,
            len(signals)
        )
        self.execute_write(update_query, params)


# Singleton instance
_snowflake_service: Optional[SnowflakeService] = None


def get_snowflake_service() -> SnowflakeService:
    """Get or create Snowflake service singleton."""
    global _snowflake_service
    if _snowflake_service is None:
        _snowflake_service = SnowflakeService()
    return _snowflake_service