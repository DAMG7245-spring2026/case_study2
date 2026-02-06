"""Document parsing and chunking for SEC filings."""

import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pdfplumber
from bs4 import BeautifulSoup

from app.models.document import DocumentChunk, ParsedDocument

logger = logging.getLogger(__name__)


class DocumentParser:
    """Parse SEC filings from various formats (PDF, HTML, TXT)."""

    # Regex patterns for extracting key sections from 10-K filings
    SECTION_PATTERNS = {
        "item_1": r"(?:ITEM\s*1[.\s]*BUSINESS)",
        "item_1a": r"(?:ITEM\s*1A[.\s]*RISK\s*FACTORS)",
        "item_7": r"(?:ITEM\s*7[.\s]*MANAGEMENT)",
        "item_7a": r"(?:ITEM\s*7A[.\s]*QUANTITATIVE)",
    }

    def parse_filing(self, file_path: Path, ticker: str) -> ParsedDocument:
        """
        Parse a SEC filing and extract structured content.
        
        Args:
            file_path: Path to the filing document
            ticker: Company ticker symbol
            
        Returns:
            ParsedDocument with extracted content and metadata
        """
        suffix = file_path.suffix.lower()
        
        # Parse based on file type
        if suffix == ".pdf":
            content = self._parse_pdf(file_path)
        elif suffix in [".htm", ".html", ".txt"]:
            content = self._parse_html(file_path)
        else:
            # Try HTML parsing as default for unknown types
            content = self._parse_html(file_path)

        # Extract sections
        sections = self._extract_sections(content)

        # Generate content hash for deduplication
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        # Extract filing metadata from path
        filing_type, filing_date = self._extract_metadata(file_path)

        return ParsedDocument(
            company_ticker=ticker,
            filing_type=filing_type,
            filing_date=filing_date,
            content=content,
            sections=sections,
            source_path=str(file_path),
            content_hash=content_hash,
            word_count=len(content.split())
        )

    def _parse_pdf(self, file_path: Path) -> str:
        """Extract text from PDF using pdfplumber."""
        text_parts = []
        
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        text_parts.append(text)
        except Exception as e:
            logger.error(f"Error parsing PDF {file_path}: {e}")
            raise
            
        return "\n\n".join(text_parts)

    def _parse_html(self, file_path: Path) -> str:
        """Extract text from HTML/TXT filing."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
                
            soup = BeautifulSoup(content, "html.parser")

            # Remove script and style elements
            for element in soup(["script", "style"]):
                element.decompose()

            # Get text
            text = soup.get_text(separator="\n")

            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            text = "\n".join(line for line in lines if line)

            return text
            
        except Exception as e:
            logger.error(f"Error parsing HTML {file_path}: {e}")
            raise

    def _extract_sections(self, content: str) -> dict[str, str]:
        """Extract key sections from 10-K content."""
        sections = {}
        content_upper = content.upper()

        for section_name, pattern in self.SECTION_PATTERNS.items():
            match = re.search(pattern, content_upper)
            if match:
                start = match.start()
                # Find end (next ITEM or end of document)
                next_item = re.search(r"ITEM\s*\d", content_upper[start + 100:])
                end = start + 100 + next_item.start() if next_item else len(content)
                # Limit section size to 50000 chars
                sections[section_name] = content[start:end][:50000]

        return sections

    def _extract_metadata(self, file_path: Path) -> tuple[str, datetime]:
        """Extract filing type and date from file path."""
        parts = file_path.parts
        
        # Try to find filing type from path
        # Path structure: .../ticker/filing_type/accession/file
        filing_type = "UNKNOWN"
        for part in parts:
            if part in ["10-K", "10-Q", "8-K", "DEF-14A"]:
                filing_type = part
                break

        # Try to extract date from accession number (format: 0000000000-YY-NNNNNN)
        filing_date = datetime.now(timezone.utc)
        for part in parts:
            date_match = re.search(r"-(\d{2})-", part)
            if date_match:
                year = int(date_match.group(1))
                year = 2000 + year if year < 50 else 1900 + year
                filing_date = datetime(year, 1, 1, tzinfo=timezone.utc)
                break

        return filing_type, filing_date


class SemanticChunker:
    """Chunk documents with section awareness for LLM processing."""

    def __init__(
        self,
        chunk_size: int = 1000,      # Target words per chunk
        chunk_overlap: int = 100,    # Overlap in words
        min_chunk_size: int = 200    # Minimum chunk size
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size

    def chunk_document(self, doc: ParsedDocument) -> list[DocumentChunk]:
        """
        Split document into overlapping chunks.
        
        Chunks each section separately to preserve context.
        """
        chunks = []

        # Chunk each section separately to preserve context
        if doc.sections:
            for section_name, section_content in doc.sections.items():
                section_chunks = self._chunk_text(
                    section_content,
                    doc.content_hash,
                    section_name
                )
                chunks.extend(section_chunks)
        else:
            # Chunk the entire content if no sections found
            chunks = self._chunk_text(doc.content, doc.content_hash, None)

        # Re-index chunks sequentially
        for i, chunk in enumerate(chunks):
            chunk.chunk_index = i

        return chunks

    def _chunk_text(
        self,
        text: str,
        doc_id: str,
        section: Optional[str]
    ) -> list[DocumentChunk]:
        """Split text into overlapping chunks."""
        words = text.split()
        chunks = []
        
        if not words:
            return chunks

        start_idx = 0
        chunk_index = 0

        while start_idx < len(words):
            end_idx = min(start_idx + self.chunk_size, len(words))

            # Don't create tiny final chunks
            if len(words) - end_idx < self.min_chunk_size:
                end_idx = len(words)

            chunk_words = words[start_idx:end_idx]
            chunk_content = " ".join(chunk_words)

            # Calculate character positions (approximate)
            start_char = len(" ".join(words[:start_idx])) if start_idx > 0 else 0
            end_char = start_char + len(chunk_content)

            chunks.append(DocumentChunk(
                document_id=doc_id,
                chunk_index=chunk_index,
                content=chunk_content,
                section=section,
                start_char=start_char,
                end_char=end_char,
                word_count=len(chunk_words)
            ))

            # Move forward with overlap
            start_idx = end_idx - self.chunk_overlap
            chunk_index += 1

            if end_idx >= len(words):
                break

        return chunks