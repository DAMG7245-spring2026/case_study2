"""Dimension score endpoints."""
from uuid import UUID
from fastapi import APIRouter, HTTPException, status
from app.models import DimensionScoreUpdate, DimensionScoreResponse
from app.models.enums import Dimension
from app.services import get_snowflake_service, get_redis_cache, CacheKeys

router = APIRouter(prefix="/api/v1/scores", tags=["Dimension Scores"])


@router.put(
    "/{score_id}",
    response_model=DimensionScoreResponse,
    summary="Update Dimension Score"
)
async def update_dimension_score(score_id: UUID, update: DimensionScoreUpdate):
    """Update a dimension score."""
    db = get_snowflake_service()
    cache = get_redis_cache()
    
    # Get existing score
    row = db.execute_one(
        """
        SELECT id, assessment_id, dimension, score, weight, confidence, evidence_count, created_at
        FROM dimension_scores WHERE id = %s
        """,
        (str(score_id),)
    )
    
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dimension score {score_id} not found"
        )
    
    # Build update query
    updates = []
    params = []
    update_data = update.model_dump(exclude_unset=True)
    
    for field, value in update_data.items():
        if value is not None:
            updates.append(f"{field} = %s")
            params.append(value)
    
    if not updates:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No fields to update"
        )
    
    params.append(str(score_id))
    
    db.execute_write(
        f"UPDATE dimension_scores SET {', '.join(updates)} WHERE id = %s",
        tuple(params)
    )
    
    # Invalidate assessment cache
    cache.delete(CacheKeys.assessment(row["assessment_id"]))
    
    # Fetch and return updated score
    updated_row = db.execute_one(
        """
        SELECT id, assessment_id, dimension, score, weight, confidence, evidence_count, created_at
        FROM dimension_scores WHERE id = %s
        """,
        (str(score_id),)
    )
    
    return DimensionScoreResponse(
        id=UUID(updated_row["id"]),
        assessment_id=UUID(updated_row["assessment_id"]),
        dimension=Dimension(updated_row["dimension"]),
        score=float(updated_row["score"]),
        weight=float(updated_row["weight"]) if updated_row["weight"] else None,
        confidence=float(updated_row["confidence"]),
        evidence_count=updated_row["evidence_count"],
        created_at=updated_row["created_at"]
    )