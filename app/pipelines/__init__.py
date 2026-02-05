"""Evidence collection pipelines."""
from app.pipelines.sec_edgar import SECEdgarPipeline
from app.pipelines.document_parser import DocumentParser, SemanticChunker, ParsedDocument, DocumentChunk
from app.pipelines.job_signals import JobSignalCollector, JobPosting
from app.pipelines.tech_signals import TechStackCollector, TechnologyDetection
from app.pipelines.patent_signals import PatentSignalCollector, Patent

__all__ = [
    "SECEdgarPipeline",
    "DocumentParser",
    "SemanticChunker",
    "ParsedDocument",
    "DocumentChunk",
    "JobSignalCollector",
    "JobPosting",
    "TechStackCollector",
    "TechnologyDetection",
    "PatentSignalCollector",
    "Patent",
]
