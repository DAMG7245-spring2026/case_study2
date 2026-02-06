"""Routers package - API endpoint routers."""

# CS1 Routers
from .health import router as health_router
from .companies import router as companies_router
from .assessments import router as assessments_router
from .scores import router as scores_router
# CS2 Routers
from .documents import router as documents_router
from .signals import router as signals_router
from .evidence import router as evidence_router
from .report import router as report_router

__all__ = [
    "health_router",
    "companies_router",
    "assessments_router",
    "scores_router",
    "documents_router",
    "signals_router",
    "evidence_router",
    "report_router",
]