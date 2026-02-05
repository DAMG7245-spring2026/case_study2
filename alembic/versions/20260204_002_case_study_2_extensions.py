"""Case Study 2 extensions - Documents and Signals - v2.0

Revision ID: 002_cs2_extensions
Revises: 001_core_tables
Create Date: 2026-02-04 16:10:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from snowflake.sqlalchemy import VARIANT

# revision identifiers, used by Alembic.
revision: str = '002_cs2_extensions'
down_revision: Union[str, None] = '001_core_tables'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add Case Study 2 tables: documents, document_chunks, external_signals."""

    # ===== 5. DOCUMENTS =====
    op.create_table(
        'documents',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('company_id', sa.String(36), nullable=False),
        sa.Column('ticker', sa.String(10), nullable=False),
        sa.Column('filing_type', sa.String(20), nullable=False),
        sa.Column('filing_date', sa.Date(), nullable=False),
        sa.Column('source_url', sa.String(500), nullable=True),
        sa.Column('local_path', sa.String(500), nullable=True),
        sa.Column('s3_key', sa.String(500), nullable=True),
        sa.Column('content_hash', sa.String(64), nullable=True),
        sa.Column('word_count', sa.Integer(), nullable=True),
        sa.Column('chunk_count', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(20), nullable=False),
        sa.Column('error_message', sa.String(1000), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('processed_at', sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
    )

    # ===== 6. DOCUMENT_CHUNKS =====
    op.create_table(
        'document_chunks',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('document_id', sa.String(36), nullable=False),
        sa.Column('chunk_index', sa.Integer(), nullable=False),
        sa.Column('content', sa.Text(), nullable=False),
        sa.Column('section', sa.String(50), nullable=True),
        sa.Column('start_char', sa.Integer(), nullable=True),
        sa.Column('end_char', sa.Integer(), nullable=True),
        sa.Column('word_count', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['document_id'], ['documents.id']),
    )

    # ===== 7. EXTERNAL_SIGNALS =====
    op.create_table(
        'external_signals',
        sa.Column('id', sa.String(36), primary_key=True),
        sa.Column('company_id', sa.String(36), nullable=False),
        sa.Column('category', sa.String(30), nullable=False),
        sa.Column('source', sa.String(30), nullable=False),
        sa.Column('signal_date', sa.Date(), nullable=False),
        sa.Column('raw_value', sa.String(500), nullable=True),
        sa.Column('normalized_score', sa.Float(), nullable=True),
        sa.Column('confidence', sa.Float(), nullable=True),
        sa.Column('metadata', VARIANT, nullable=True),  # Snowflake VARIANT type
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(['company_id'], ['companies.id']),
    )

    # ===== INDEXES =====
    op.create_index('idx_documents_company', 'documents', ['company_id'])
    op.create_index('idx_documents_status', 'documents', ['status'])
    op.create_index('idx_chunks_document', 'document_chunks', ['document_id'])
    op.create_index('idx_signals_company', 'external_signals', ['company_id'])
    op.create_index('idx_signals_category', 'external_signals', ['category'])


def downgrade() -> None:
    """Drop Case Study 2 tables."""
    op.drop_table('external_signals')
    op.drop_table('document_chunks')
    op.drop_table('documents')
