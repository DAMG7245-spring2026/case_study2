"""Snowflake database service."""
import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator, Optional
from uuid import UUID, uuid4

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

    # ================================================================
    # CS2: Document Methods
    # ================================================================

    def insert_document(
        self,
        company_id: UUID,
        ticker: str,
        filing_type: str,
        filing_date: datetime,
        content_hash: Optional[str] = None,
        word_count: Optional[int] = None,
        s3_key: Optional[str] = None,
        local_path: Optional[str] = None,
        source_url: Optional[str] = None,
        status: str = "pending"
    ) -> str:
        """Insert a document record."""
        doc_id = str(uuid4())
        
        query = """
            INSERT INTO documents (
                id, company_id, ticker, filing_type, filing_date,
                content_hash, word_count, s3_key, local_path, source_url,
                status, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        self.execute_write(query, (
            doc_id,
            str(company_id),
            ticker.upper(),
            filing_type,
            filing_date,
            content_hash,
            word_count,
            s3_key,
            local_path,
            source_url,
            status,
            datetime.now(timezone.utc)
        ))
        
        logger.info(f"Inserted document {doc_id} for {ticker}")
        return doc_id

    def update_document_status(
        self,
        document_id: str,
        status: str,
        chunk_count: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> None:
        """Update document status."""
        now = datetime.now(timezone.utc)
        
        if chunk_count is not None:
            query = """
                UPDATE documents 
                SET status = %s, chunk_count = %s, processed_at = %s
                WHERE id = %s
            """
            self.execute_write(query, (status, chunk_count, now, document_id))
        elif error_message:
            query = """
                UPDATE documents 
                SET status = %s, error_message = %s, processed_at = %s
                WHERE id = %s
            """
            self.execute_write(query, (status, error_message, now, document_id))
        else:
            query = """
                UPDATE documents 
                SET status = %s, processed_at = %s
                WHERE id = %s
            """
            self.execute_write(query, (status, now, document_id))

    def get_document(self, document_id: str) -> Optional[dict[str, Any]]:
        """Get a document by ID."""
        query = "SELECT * FROM documents WHERE id = %s"
        return self.execute_one(query, (document_id,))

    def get_documents(
        self,
        company_id: Optional[UUID] = None,
        ticker: Optional[str] = None,
        filing_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> list[dict[str, Any]]:
        """Get documents with optional filters."""
        conditions = []
        params = []
        
        if company_id:
            conditions.append("company_id = %s")
            params.append(str(company_id))
        if ticker:
            conditions.append("ticker = %s")
            params.append(ticker.upper())
        if filing_type:
            conditions.append("filing_type = %s")
            params.append(filing_type)
        if status:
            conditions.append("status = %s")
            params.append(status)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
            SELECT * FROM documents 
            WHERE {where_clause}
            ORDER BY filing_date DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        
        return self.execute_query(query, tuple(params))

    def count_documents(
        self,
        company_id: Optional[UUID] = None,
        ticker: Optional[str] = None,
        filing_type: Optional[str] = None,
        status: Optional[str] = None
    ) -> int:
        """Count documents with optional filters."""
        conditions = []
        params = []
        
        if company_id:
            conditions.append("company_id = %s")
            params.append(str(company_id))
        if ticker:
            conditions.append("ticker = %s")
            params.append(ticker.upper())
        if filing_type:
            conditions.append("filing_type = %s")
            params.append(filing_type)
        if status:
            conditions.append("status = %s")
            params.append(status)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"SELECT COUNT(*) as count FROM documents WHERE {where_clause}"
        
        result = self.execute_one(query, tuple(params) if params else None)
        return result["count"] if result else 0

    # ================================================================
    # CS2: Document Chunks Methods
    # ================================================================

    def insert_chunks(self, document_id: str, chunks: list[dict]) -> int:
        """Batch insert document chunks."""
        query = """
            INSERT INTO document_chunks (
                id, document_id, chunk_index, content, section,
                start_char, end_char, word_count, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        now = datetime.now(timezone.utc)
        count = 0
        
        with self.cursor() as cur:
            for chunk in chunks:
                cur.execute(query, (
                    str(uuid4()),
                    document_id,
                    chunk.get("chunk_index", count),
                    chunk.get("content", ""),
                    chunk.get("section"),
                    chunk.get("start_char"),
                    chunk.get("end_char"),
                    chunk.get("word_count"),
                    now
                ))
                count += 1
        
        logger.info(f"Inserted {count} chunks for document {document_id}")
        return count

    def get_chunks(
        self,
        document_id: str,
        section: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> list[dict[str, Any]]:
        """Get chunks for a document."""
        if section:
            query = """
                SELECT * FROM document_chunks 
                WHERE document_id = %s AND section = %s
                ORDER BY chunk_index
                LIMIT %s OFFSET %s
            """
            return self.execute_query(query, (document_id, section, limit, offset))
        else:
            query = """
                SELECT * FROM document_chunks 
                WHERE document_id = %s
                ORDER BY chunk_index
                LIMIT %s OFFSET %s
            """
            return self.execute_query(query, (document_id, limit, offset))

    def count_chunks(self, document_id: str) -> int:
        """Count chunks for a document."""
        query = "SELECT COUNT(*) as count FROM document_chunks WHERE document_id = %s"
        result = self.execute_one(query, (document_id,))
        return result["count"] if result else 0

    # ================================================================
    # CS2: External Signals Methods
    # ================================================================

    def insert_signal(
        self,
        company_id: UUID,
        category: str,
        source: str,
        signal_date: datetime,
        raw_value: str,
        normalized_score: float,
        confidence: float,
        metadata: dict
    ) -> str:
        """Insert an external signal."""
        signal_id = str(uuid4())
        
        # 使用 SELECT 代替 VALUES，因為 PARSE_JSON() 不能在 VALUES 中使用
        query = """
            INSERT INTO external_signals (
                id, company_id, category, source, signal_date,
                raw_value, normalized_score, confidence, metadata, created_at
            ) 
            SELECT %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s
        """
        
        self.execute_write(query, (
            signal_id,
            str(company_id),
            category,
            source,
            signal_date,
            raw_value,
            normalized_score,
            confidence,
            json.dumps(metadata),
            datetime.now(timezone.utc)
        ))
        
        logger.info(f"Inserted signal {signal_id} for company {company_id}")
        return signal_id

    def get_signals(
        self,
        company_id: Optional[UUID] = None,
        category: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> list[dict[str, Any]]:
        """Get signals with optional filters."""
        conditions = []
        params = []
        
        if company_id:
            conditions.append("company_id = %s")
            params.append(str(company_id))
        if category:
            conditions.append("category = %s")
            params.append(category)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
            SELECT * FROM external_signals 
            WHERE {where_clause}
            ORDER BY signal_date DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        
        results = self.execute_query(query, tuple(params))
        
        # 將 metadata 從 JSON 字串轉換為 dict
        for r in results:
            if r.get("metadata") and isinstance(r["metadata"], str):
                r["metadata"] = json.loads(r["metadata"])
        
        return results

    def count_signals(
        self,
        company_id: Optional[UUID] = None,
        category: Optional[str] = None
    ) -> int:
        """Count signals with optional filters."""
        conditions = []
        params = []
        
        if company_id:
            conditions.append("company_id = %s")
            params.append(str(company_id))
        if category:
            conditions.append("category = %s")
            params.append(category)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        query = f"SELECT COUNT(*) as count FROM external_signals WHERE {where_clause}"
        
        result = self.execute_one(query, tuple(params) if params else None)
        return result["count"] if result else 0

    # ================================================================
    # CS2: Signal Summary Methods
    # ================================================================

    def upsert_signal_summary(
        self,
        company_id: UUID,
        ticker: str,
        technology_hiring_score: float,
        innovation_activity_score: float,
        digital_presence_score: float,
        leadership_signals_score: float,
        signal_count: int
    ) -> None:
        """Update or insert company signal summary."""
        composite_score = round(
            0.30 * technology_hiring_score +
            0.25 * innovation_activity_score +
            0.25 * digital_presence_score +
            0.20 * leadership_signals_score,
            2
        )
        
        now = datetime.now(timezone.utc)
        cid = str(company_id)
        
        # Check if exists
        existing = self.execute_one(
            "SELECT 1 FROM company_signal_summaries WHERE company_id = %s",
            (cid,)
        )
        
        if existing:
            query = """
                UPDATE company_signal_summaries SET
                    ticker = %s,
                    technology_hiring_score = %s,
                    innovation_activity_score = %s,
                    digital_presence_score = %s,
                    leadership_signals_score = %s,
                    composite_score = %s,
                    signal_count = %s,
                    last_updated = %s
                WHERE company_id = %s
            """
            self.execute_write(query, (
                ticker.upper(), technology_hiring_score, innovation_activity_score,
                digital_presence_score, leadership_signals_score, composite_score,
                signal_count, now, cid
            ))
        else:
            query = """
                INSERT INTO company_signal_summaries (
                    company_id, ticker, technology_hiring_score, innovation_activity_score,
                    digital_presence_score, leadership_signals_score, composite_score,
                    signal_count, last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.execute_write(query, (
                cid, ticker.upper(), technology_hiring_score, innovation_activity_score,
                digital_presence_score, leadership_signals_score, composite_score,
                signal_count, now
            ))

    def get_signal_summary(self, company_id: UUID) -> Optional[dict[str, Any]]:
        """Get signal summary for a company."""
        query = "SELECT * FROM company_signal_summaries WHERE company_id = %s"
        return self.execute_one(query, (str(company_id),))

    # ================================================================
    # CS2: Evidence Statistics
    # ================================================================

    def get_evidence_stats(self) -> dict[str, Any]:
        """Get evidence collection statistics."""
        stats = {
            "total_companies": 0,
            "total_documents": 0,
            "total_chunks": 0,
            "total_signals": 0,
            "documents_by_type": {},
            "documents_by_status": {},
            "signals_by_category": {},
            "companies_with_documents": 0,
            "companies_with_signals": 0,
        }
        
        # Total companies
        result = self.execute_one("SELECT COUNT(*) as count FROM companies WHERE is_deleted = FALSE")
        stats["total_companies"] = result["count"] if result else 0
        
        # Document stats
        result = self.execute_one("""
            SELECT COUNT(*) as total, COUNT(DISTINCT company_id) as companies
            FROM documents
        """)
        if result:
            stats["total_documents"] = result["total"]
            stats["companies_with_documents"] = result["companies"]
        
        # Chunk count
        result = self.execute_one("SELECT COUNT(*) as count FROM document_chunks")
        stats["total_chunks"] = result["count"] if result else 0
        
        # Signal stats
        result = self.execute_one("""
            SELECT COUNT(*) as total, COUNT(DISTINCT company_id) as companies
            FROM external_signals
        """)
        if result:
            stats["total_signals"] = result["total"]
            stats["companies_with_signals"] = result["companies"]
        
        # Documents by type
        results = self.execute_query("""
            SELECT filing_type, COUNT(*) as count
            FROM documents GROUP BY filing_type
        """)
        stats["documents_by_type"] = {r["filing_type"]: r["count"] for r in results}
        
        # Documents by status
        results = self.execute_query("""
            SELECT status, COUNT(*) as count
            FROM documents GROUP BY status
        """)
        stats["documents_by_status"] = {r["status"]: r["count"] for r in results}
        
        # Signals by category
        results = self.execute_query("""
            SELECT category, COUNT(*) as count
            FROM external_signals GROUP BY category
        """)
        stats["signals_by_category"] = {r["category"]: r["count"] for r in results}
        
        return stats

    # ================================================================
    # CS2: Company Helpers
    # ================================================================

    def get_company_by_ticker(self, ticker: str) -> Optional[dict[str, Any]]:
        """Get company by ticker symbol."""
        query = "SELECT * FROM companies WHERE ticker = %s AND is_deleted = FALSE"
        return self.execute_one(query, (ticker.upper(),))

    def get_company_by_id(self, company_id: UUID) -> Optional[dict[str, Any]]:
        """Get company by ID (returns id, name, ticker, etc.)."""
        query = "SELECT id, name, ticker FROM companies WHERE id = %s AND is_deleted = FALSE"
        return self.execute_one(query, (str(company_id),))

    def get_or_create_company(
        self,
        ticker: str,
        name: str,
        industry_id: UUID
    ) -> dict[str, Any]:
        """Get existing company or create new one."""
        existing = self.get_company_by_ticker(ticker)
        if existing:
            return existing
        
        company_id = str(uuid4())
        now = datetime.now(timezone.utc)
        
        query = """
            INSERT INTO companies (id, name, ticker, industry_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.execute_write(query, (company_id, name, ticker.upper(), str(industry_id), now, now))
        
        logger.info(f"Created company {ticker}: {name}")
        
        return {
            "id": company_id,
            "name": name,
            "ticker": ticker.upper(),
            "industry_id": str(industry_id)
        }


# Singleton instance
_snowflake_service: Optional[SnowflakeService] = None


def get_snowflake_service() -> SnowflakeService:
    """Get or create Snowflake service singleton."""
    global _snowflake_service
    if _snowflake_service is None:
        _snowflake_service = SnowflakeService()
    return _snowflake_service