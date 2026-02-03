"""Assessment endpoints."""
import math
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4
from fastapi import APIRouter, HTTPException, Query, status
from app.config import get_settings
from app.models import (
    AssessmentCreate, AssessmentResponse, AssessmentStatusUpdate,
    AssessmentStatus, AssessmentType, PaginatedResponse, 
    DimensionScoreCreate, DimensionScoreResponse, DimensionScoreBulkCreate,
    VALID_STATUS_TRANSITIONS
)
from app.services import get_snowflake_service, get_redis_cache, CacheKeys

router = APIRouter(prefix="/api/v1/assessments", tags=["Assessments"])


@router.post(
    "",
    response_model=AssessmentResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create Assessment"
)
async def create_assessment(assessment: AssessmentCreate):
    """Create a new assessment for a company."""
    db = get_snowflake_service()
    
    # Verify company exists
    company = db.execute_one(
        "SELECT id FROM companies WHERE id = %s AND is_deleted = FALSE",
        (str(assessment.company_id),)
    )
    if not company:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Company {assessment.company_id} not found"
        )
    
    assessment_id = str(uuid4())
    now = datetime.now(timezone.utc)
    
    db.execute_write(
        """
        INSERT INTO assessments 
        (id, company_id, assessment_type, assessment_date, status, 
         primary_assessor, secondary_assessor, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (assessment_id, str(assessment.company_id), assessment.assessment_type.value,
         assessment.assessment_date, AssessmentStatus.DRAFT.value,
         assessment.primary_assessor, assessment.secondary_assessor, now)
    )
    
    return AssessmentResponse(
        id=UUID(assessment_id),
        company_id=assessment.company_id,
        assessment_type=assessment.assessment_type,
        assessment_date=assessment.assessment_date,
        status=AssessmentStatus.DRAFT,
        primary_assessor=assessment.primary_assessor,
        secondary_assessor=assessment.secondary_assessor,
        v_r_score=None,
        confidence_lower=None,
        confidence_upper=None,
        created_at=now
    )


@router.get(
    "",
    response_model=PaginatedResponse[AssessmentResponse],
    summary="List Assessments"
)
async def list_assessments(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    company_id: Optional[UUID] = None,
    status_filter: Optional[AssessmentStatus] = Query(None, alias="status"),
    assessment_type: Optional[AssessmentType] = None
):
    """List assessments with filtering and pagination."""
    db = get_snowflake_service()
    
    # Build query
    base_query = "FROM assessments WHERE 1=1"
    params = []
    
    if company_id:
        base_query += " AND company_id = %s"
        params.append(str(company_id))
    if status_filter:
        base_query += " AND status = %s"
        params.append(status_filter.value)
    if assessment_type:
        base_query += " AND assessment_type = %s"
        params.append(assessment_type.value)
    
    # Get total count
    count_result = db.execute_one(f"SELECT COUNT(*) as count {base_query}", tuple(params))
    total = count_result["count"] if count_result else 0
    
    # Get paginated results
    offset = (page - 1) * page_size
    query = f"""
        SELECT id, company_id, assessment_type, assessment_date, status,
               primary_assessor, secondary_assessor, v_r_score, 
               confidence_lower, confidence_upper, created_at
        {base_query}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([page_size, offset])
    
    rows = db.execute_query(query, tuple(params))
    
    items = [
        AssessmentResponse(
            id=UUID(row["id"]),
            company_id=UUID(row["company_id"]),
            assessment_type=AssessmentType(row["assessment_type"]),
            assessment_date=row["assessment_date"],
            status=AssessmentStatus(row["status"]),
            primary_assessor=row["primary_assessor"],
            secondary_assessor=row["secondary_assessor"],
            v_r_score=float(row["v_r_score"]) if row["v_r_score"] else None,
            confidence_lower=float(row["confidence_lower"]) if row["confidence_lower"] else None,
            confidence_upper=float(row["confidence_upper"]) if row["confidence_upper"] else None,
            created_at=row["created_at"]
        )
        for row in rows
    ]
    
    return PaginatedResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=math.ceil(total / page_size) if total > 0 else 0
    )


@router.get(
    "/{assessment_id}",
    response_model=AssessmentResponse,
    summary="Get Assessment"
)
async def get_assessment(assessment_id: UUID):
    """Get an assessment by ID with dimension scores."""
    cache = get_redis_cache()
    settings = get_settings()
    cache_key = CacheKeys.assessment(str(assessment_id))
    
    # Try cache first
    cached = cache.get(cache_key, AssessmentResponse)
    if cached:
        return cached
    
    db = get_snowflake_service()
    row = db.execute_one(
        """
        SELECT id, company_id, assessment_type, assessment_date, status,
               primary_assessor, secondary_assessor, v_r_score, 
               confidence_lower, confidence_upper, created_at
        FROM assessments WHERE id = %s
        """,
        (str(assessment_id),)
    )
    
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment {assessment_id} not found"
        )
    
    assessment = AssessmentResponse(
        id=UUID(row["id"]),
        company_id=UUID(row["company_id"]),
        assessment_type=AssessmentType(row["assessment_type"]),
        assessment_date=row["assessment_date"],
        status=AssessmentStatus(row["status"]),
        primary_assessor=row["primary_assessor"],
        secondary_assessor=row["secondary_assessor"],
        v_r_score=float(row["v_r_score"]) if row["v_r_score"] else None,
        confidence_lower=float(row["confidence_lower"]) if row["confidence_lower"] else None,
        confidence_upper=float(row["confidence_upper"]) if row["confidence_upper"] else None,
        created_at=row["created_at"]
    )
    
    cache.set(cache_key, assessment, settings.cache_ttl_assessment)
    return assessment


@router.patch(
    "/{assessment_id}/status",
    response_model=AssessmentResponse,
    summary="Update Assessment Status"
)
async def update_assessment_status(assessment_id: UUID, update: AssessmentStatusUpdate):
    """Update an assessment's status with state machine validation."""
    db = get_snowflake_service()
    cache = get_redis_cache()
    
    # Get current status
    row = db.execute_one(
        "SELECT status FROM assessments WHERE id = %s",
        (str(assessment_id),)
    )
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment {assessment_id} not found"
        )
    
    current_status = AssessmentStatus(row["status"])
    new_status = update.status
    
    # Validate transition
    valid_transitions = VALID_STATUS_TRANSITIONS.get(current_status, [])
    if new_status not in valid_transitions:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid status transition from {current_status.value} to {new_status.value}. "
                   f"Valid transitions: {[s.value for s in valid_transitions]}"
        )
    
    # Update status
    db.execute_write(
        "UPDATE assessments SET status = %s WHERE id = %s",
        (new_status.value, str(assessment_id))
    )
    
    # Invalidate cache
    cache.delete(CacheKeys.assessment(str(assessment_id)))
    
    return await get_assessment(assessment_id)


@router.post(
    "/{assessment_id}/scores",
    response_model=list[DimensionScoreResponse],
    status_code=status.HTTP_201_CREATED,
    summary="Add Dimension Scores"
)
async def add_dimension_scores(assessment_id: UUID, scores: DimensionScoreBulkCreate):
    """Add dimension scores to an assessment."""
    db = get_snowflake_service()
    cache = get_redis_cache()
    
    # Verify assessment exists
    assessment = db.execute_one(
        "SELECT id FROM assessments WHERE id = %s",
        (str(assessment_id),)
    )
    if not assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment {assessment_id} not found"
        )
    
    created_scores = []
    now = datetime.now(timezone.utc)
    
    for score in scores.scores:
        score_id = str(uuid4())
        
        # Check for duplicate dimension
        existing = db.execute_one(
            "SELECT id FROM dimension_scores WHERE assessment_id = %s AND dimension = %s",
            (str(assessment_id), score.dimension.value)
        )
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Score for dimension {score.dimension.value} already exists"
            )
        
        db.execute_write(
            """
            INSERT INTO dimension_scores 
            (id, assessment_id, dimension, score, weight, confidence, evidence_count, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (score_id, str(assessment_id), score.dimension.value, score.score,
             score.weight, score.confidence, score.evidence_count, now)
        )
        
        created_scores.append(DimensionScoreResponse(
            id=UUID(score_id),
            assessment_id=assessment_id,
            dimension=score.dimension,
            score=score.score,
            weight=score.weight,
            confidence=score.confidence,
            evidence_count=score.evidence_count,
            created_at=now
        ))
    
    # Invalidate assessment cache
    cache.delete(CacheKeys.assessment(str(assessment_id)))
    
    return created_scores


@router.get(
    "/{assessment_id}/scores",
    response_model=list[DimensionScoreResponse],
    summary="Get Dimension Scores"
)
async def get_dimension_scores(assessment_id: UUID):
    """Get all dimension scores for an assessment."""
    db = get_snowflake_service()
    
    # Verify assessment exists
    assessment = db.execute_one(
        "SELECT id FROM assessments WHERE id = %s",
        (str(assessment_id),)
    )
    if not assessment:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assessment {assessment_id} not found"
        )
    
    rows = db.execute_query(
        """
        SELECT id, assessment_id, dimension, score, weight, confidence, evidence_count, created_at
        FROM dimension_scores WHERE assessment_id = %s
        ORDER BY dimension
        """,
        (str(assessment_id),)
    )
    
    from app.models.enums import Dimension
    return [
        DimensionScoreResponse(
            id=UUID(row["id"]),
            assessment_id=UUID(row["assessment_id"]),
            dimension=Dimension(row["dimension"]),
            score=float(row["score"]),
            weight=float(row["weight"]) if row["weight"] else None,
            confidence=float(row["confidence"]),
            evidence_count=row["evidence_count"],
            created_at=row["created_at"]
        )
        for row in rows
    ]