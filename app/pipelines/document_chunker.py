"""Document chunking for SEC filings."""

import logging
from typing import Optional

from app.models.document import DocumentChunk, ParsedDocument

logger = logging.getLogger(__name__)


class SemanticChunker:
    """Chunk documents with section awareness for LLM processing."""

    def __init__(
        self,
        chunk_size: int = 1000,  # Target words per chunk
        chunk_overlap: int = 100,  # Overlap in words
        min_chunk_size: int = 200,  # Minimum chunk size
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
                    section_content, doc.content_hash, section_name
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
        self, text: str, doc_id: str, section: Optional[str]
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

            chunks.append(
                DocumentChunk(
                    document_id=doc_id,
                    chunk_index=chunk_index,
                    content=chunk_content,
                    section=section,
                    start_char=start_char,
                    end_char=end_char,
                    word_count=len(chunk_words),
                )
            )

            # Move forward with overlap
            start_idx = end_idx - self.chunk_overlap
            chunk_index += 1

            if end_idx >= len(words):
                break

        return chunks
