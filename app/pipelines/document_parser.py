"""Document parsing and chunking."""
import pdfplumber
from bs4 import BeautifulSoup
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import re


@dataclass
class ParsedDocument:
    """Represents a parsed SEC document."""
    company_ticker: str
    filing_type: str
    filing_date: datetime
    content: str
    sections: dict[str, str]
    source_path: str
    content_hash: str
    word_count: int


@dataclass
class DocumentChunk:
    """A chunk of a document for processing."""
    document_id: str
    chunk_index: int
    content: str
    section: str | None
    start_char: int
    end_char: int
    word_count: int


class DocumentParser:
    """Parse SEC filings from various formats."""

    # Regex patterns for section extraction
    SECTION_PATTERNS = {
        "item_1": r"(?:ITEM\s*1[.\s]*BUSINESS)",
        "item_1a": r"(?:ITEM\s*1A[.\s]*RISK\s*FACTORS)",
        "item_7": r"(?:ITEM\s*7[.\s]*MANAGEMENT)",
        "item_7a": r"(?:ITEM\s*7A[.\s]*QUANTITATIVE)",
    }

    def parse_filing(self, file_path: Path, ticker: str) -> ParsedDocument:
        """Parse a filing and extract structured content."""

        suffix = file_path.suffix.lower()

        if suffix == ".pdf":
            content = self._parse_pdf(file_path)
        elif suffix in [".htm", ".html", ".txt"]:
            content = self._parse_html(file_path)
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

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

        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text_parts.append(text)

        return "\n\n".join(text_parts)

    def _parse_html(self, file_path: Path) -> str:
        """Extract text from SEC SGML/HTML filing."""
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            raw_content = f.read()

        # Check if it's an SEC SGML format file
        if "<DOCUMENT>" in raw_content and "<TYPE>" in raw_content:
            return self._parse_sec_sgml(raw_content)

        # Otherwise parse as regular HTML
        soup = BeautifulSoup(raw_content, "html.parser")

        # Remove script, style, and XBRL elements
        for element in soup(["script", "style", "ix:hidden", "ix:nonfraction"]):
            element.decompose()

        # Get text
        text = soup.get_text(separator="\n")

        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        text = "\n".join(line for line in lines if line)

        return text

    def _parse_sec_sgml(self, content: str) -> str:
        """Parse SEC SGML format filing (full-submission.txt)."""
        # Extract only the main document content, skip headers
        documents = []

        # Split into document sections
        doc_pattern = r"<DOCUMENT>(.*?)</DOCUMENT>"
        doc_matches = re.findall(doc_pattern, content, re.DOTALL)

        for doc_content in doc_matches:
            # Check document type - we want the main filing, not exhibits
            type_match = re.search(r"<TYPE>(.*?)\n", doc_content)
            if not type_match:
                continue

            doc_type = type_match.group(1).strip()

            # Skip non-main documents (exhibits, graphics, etc.)
            if any(skip in doc_type for skip in ["EX-", "GRAPHIC", "XML", "ZIP", "EXCEL"]):
                continue

            # Extract the text portion (after <TEXT> tag)
            text_match = re.search(r"<TEXT>(.*?)</TEXT>", doc_content, re.DOTALL)
            if not text_match:
                continue

            text_content = text_match.group(1)

            # Parse HTML within the TEXT section
            soup = BeautifulSoup(text_content, "html.parser")

            # Remove unwanted elements
            for element in soup([
                "script", "style",
                "ix:hidden", "ix:nonfraction", "ix:nonnumeric",  # XBRL inline elements
                "table"  # Tables often contain layout/formatting, not content
            ]):
                element.decompose()

            # Get clean text
            text = soup.get_text(separator="\n")

            # Clean up common SEC artifacts
            text = self._clean_sec_text(text)

            documents.append(text)

        # Join all main documents
        return "\n\n".join(documents)

    def _clean_sec_text(self, text: str) -> str:
        """Clean up common SEC filing artifacts and formatting issues."""
        # Remove excessive whitespace while preserving paragraph breaks
        lines = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                # Remove multiple spaces
                line = re.sub(r'\s+', ' ', line)
                lines.append(line)

        text = "\n".join(lines)

        # Remove common SEC artifacts
        text = re.sub(r"UNITED STATES\s+SECURITIES AND EXCHANGE COMMISSION.*?FORM \d+-[KQ]", "", text, flags=re.DOTALL)
        text = re.sub(r"\*{3,}", "", text)  # Remove separator lines
        text = re.sub(r"-{3,}", "", text)
        text = re.sub(r"_{3,}", "", text)
        text = re.sub(r"={3,}", "", text)

        # Remove page numbers and headers (common patterns)
        text = re.sub(r"\n\d+\n", "\n", text)
        text = re.sub(r"Table of Contents", "", text, flags=re.IGNORECASE)

        # Remove extra blank lines (keep max 2 consecutive newlines)
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()

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
                sections[section_name] = content[start:end][:50000]  # Limit size

        return sections

    def _extract_metadata(self, file_path: Path) -> tuple[str, datetime]:
        """Extract filing type and date from file path."""
        # Path structure: .../ticker/filing_type/accession/file
        parts = file_path.parts
        filing_type = parts[-3] if len(parts) > 2 else "UNKNOWN"

        # Try to extract date from accession number (format: 0000000000-YY-NNNNNN)
        accession = parts[-2] if len(parts) > 1 else ""
        date_match = re.search(r"-(\d{2})-", accession)
        if date_match:
            year = int(date_match.group(1))
            year = 2000 + year if year < 50 else 1900 + year
            filing_date = datetime(year, 1, 1, tzinfo=timezone.utc)
        else:
            filing_date = datetime.now(timezone.utc)

        return filing_type, filing_date


class SemanticChunker:
    """Chunk documents with section awareness."""

    def __init__(
        self,
        chunk_size: int = 1000,  # Target words per chunk
        chunk_overlap: int = 100,  # Overlap in words
        min_chunk_size: int = 200  # Minimum chunk size
    ):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.min_chunk_size = min_chunk_size

    def chunk_document(
        self,
        doc: ParsedDocument
    ) -> list[DocumentChunk]:
        """Split document into overlapping chunks."""
        chunks = []

        # Chunk each section separately to preserve context
        for section_name, section_content in doc.sections.items():
            section_chunks = self._chunk_text(
                section_content,
                doc.content_hash,
                section_name
            )
            chunks.extend(section_chunks)

        # Also chunk any remaining content
        if not doc.sections:
            chunks = self._chunk_text(doc.content, doc.content_hash, None)

        return chunks

    def _chunk_text(
        self,
        text: str,
        doc_id: str,
        section: str | None
    ) -> list[DocumentChunk]:
        """Split text into overlapping chunks."""
        words = text.split()
        chunks = []

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
            start_char = len(" ".join(words[:start_idx]))
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
