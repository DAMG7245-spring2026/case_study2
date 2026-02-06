"""Initial core tables - v1.0

Revision ID: 001_core_tables
Revises:
Create Date: 2026-02-04 16:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '001_core_tables'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create initial 4 core tables for PE Org-AI-R Platform."""

    # ===== 1. INDUSTRIES =====
    op.create_table(
        'industries',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('name', sa.String(255), nullable=False, unique=True),
        sa.Column('sector', sa.String(100), nullable=False),
        sa.Column('h_r_base', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
    )

    # ===== 2. COMPANIES =====
    op.create_table(
        'companies',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('ticker', sa.String(10), nullable=True),
        sa.Column('industry_id', sa.String(36), nullable=False),
        sa.Column('position_factor', sa.Float(), nullable=False),
        sa.Column('is_deleted', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['industry_id'], ['industries.id']),
    )

    # ===== 3. ASSESSMENTS =====
    op.create_table(
        'assessments',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('company_id', sa.String(36), nullable=False),
        sa.Column('assessment_type', sa.String(20), nullable=False),
        sa.Column('assessment_date', sa.Date(), nullable=False),
        sa.Column('status', sa.String(20), nullable=False),
        sa.Column('primary_assessor', sa.String(255), nullable=True),
        sa.Column('secondary_assessor', sa.String(255), nullable=True),
        sa.Column('v_r_score', sa.Float(), nullable=True),
        sa.Column('confidence_lower', sa.Float(), nullable=True),
        sa.Column('confidence_upper', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
    )

    # ===== 4. DIMENSION_SCORES =====
    op.create_table(
        'dimension_scores',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('assessment_id', sa.String(36), nullable=False),
        sa.Column('dimension', sa.String(30), nullable=False),
        sa.Column('score', sa.Float(), nullable=False),
        sa.Column('weight', sa.Float(), nullable=True),
        sa.Column('confidence', sa.Float(), nullable=False),
        sa.Column('evidence_count', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['assessment_id'], ['assessments.id']),
    )

    # ===== INDEXES =====
    op.create_index('idx_companies_industry', 'companies', ['industry_id'])
    op.create_index('idx_companies_deleted', 'companies', ['is_deleted'])
    op.create_index('idx_assessments_company', 'assessments', ['company_id'])
    op.create_index('idx_assessments_status', 'assessments', ['status'])
    op.create_index('idx_assessments_type', 'assessments', ['assessment_type'])
    op.create_index('idx_dimension_scores_assessment', 'dimension_scores', ['assessment_id'])


def downgrade() -> None:
    """Drop all core tables."""
    op.drop_table('dimension_scores')
    op.drop_table('assessments')
    op.drop_table('companies')
    op.drop_table('industries')
