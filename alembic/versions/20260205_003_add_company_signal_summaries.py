"""Add company signal summaries table - v3.0

Revision ID: 003_signal_summaries
Revises: 002_cs2_extensions
Create Date: 2026-02-05 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '003_signal_summaries'
down_revision: Union[str, None] = '002_cs2_extensions'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add company_signal_summaries table and missing constraints/indexes."""

    # ===== 8. COMPANY_SIGNAL_SUMMARIES =====
    op.create_table(
        'company_signal_summaries',
        sa.Column('company_id', sa.String(36), primary_key=True),
        sa.Column('ticker', sa.String(10), nullable=False),
        sa.Column('technology_hiring_score', sa.Float(), nullable=False),
        sa.Column('innovation_activity_score', sa.Float(), nullable=False),
        sa.Column('digital_presence_score', sa.Float(), nullable=False),
        sa.Column('leadership_signals_score', sa.Float(), nullable=False),
        sa.Column('composite_score', sa.Float(), nullable=False),
        sa.Column('signal_count', sa.Integer(), nullable=False),
        sa.Column('last_updated', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
    )

    # ===== ADD MISSING INDEXES =====
    # Index for documents ticker (from schema_cs2.sql)
    op.create_index('idx_documents_ticker', 'documents', ['ticker'])

    # ===== ADD MISSING CONSTRAINTS =====
    # Unique constraint for document_chunks (document_id, chunk_index)
    op.create_unique_constraint(
        'uq_document_chunks_document_index',
        'document_chunks',
        ['document_id', 'chunk_index']
    )


def downgrade() -> None:
    """Drop company_signal_summaries table and remove constraints/indexes."""
    # Drop unique constraint
    op.drop_constraint('uq_document_chunks_document_index', 'document_chunks')

    # Drop index
    op.drop_index('idx_documents_ticker', 'documents')

    # Drop table
    op.drop_table('company_signal_summaries')
