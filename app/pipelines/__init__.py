"""Evidence collection pipelines for PE Org-AI-R Platform."""

from app.pipelines.sec_edgar import SECEdgarPipeline
from app.pipelines.document_parser import DocumentParser
from app.pipelines.document_chunker import SemanticChunker
from app.pipelines.job_signals import JobSignalCollector
from app.pipelines.tech_signals import TechStackCollector
from app.pipelines.patent_signals import PatentSignalCollector

__all__ = [
    "SECEdgarPipeline",
    "DocumentParser",
    "SemanticChunker",
    "JobSignalCollector",
    "TechStackCollector",
    "PatentSignalCollector",
]