-- =====================================================
-- Industries table
-- =====================================================
CREATE TABLE IF NOT EXISTS industries (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100) NOT NULL,
    h_r_base DECIMAL(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- Companies table
-- =====================================================
CREATE TABLE IF NOT EXISTS companies (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    ticker VARCHAR(10),
    industry_id VARCHAR(36),
    position_factor DECIMAL(4,3) DEFAULT 0.0,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- Assessments table
-- =====================================================
CREATE TABLE IF NOT EXISTS assessments (
    id VARCHAR(36) PRIMARY KEY,
    company_id VARCHAR(36) NOT NULL,
    assessment_type VARCHAR(20) NOT NULL,
    assessment_date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'draft',
    primary_assessor VARCHAR(255),
    secondary_assessor VARCHAR(255),
    v_r_score DECIMAL(5,2),
    confidence_lower DECIMAL(5,2),
    confidence_upper DECIMAL(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- Dimension scores table
-- =====================================================
CREATE TABLE IF NOT EXISTS dimension_scores (
    id VARCHAR(36) PRIMARY KEY,
    assessment_id VARCHAR(36) NOT NULL,
    dimension VARCHAR(30) NOT NULL,
    score DECIMAL(5,2) NOT NULL,
    weight DECIMAL(4,3),
    confidence DECIMAL(4,3) DEFAULT 0.8,
    evidence_count INT DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- Seed data for industries
-- =====================================================
-- Using MERGE to avoid duplicate inserts on re-runs
MERGE INTO industries AS target
USING (
    SELECT '550e8400-e29b-41d4-a716-446655440001' as id, 'Manufacturing' as name, 'Industrials' as sector, 72 as h_r_base UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440002', 'Healthcare Services', 'Healthcare', 78 UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440003', 'Business Services', 'Services', 75 UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440004', 'Retail', 'Consumer', 70 UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440005', 'Financial Services', 'Financial', 80 UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440006', 'Technology', 'Technology', 85 UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440007', 'Energy', 'Energy', 68 UNION ALL
    SELECT '550e8400-e29b-41d4-a716-446655440008', 'Real Estate', 'Real Estate', 65
) AS source
ON target.id = source.id
WHEN NOT MATCHED THEN
    INSERT (id, name, sector, h_r_base)
    VALUES (source.id, source.name, source.sector, source.h_r_base);

-- =====================================================
-- Documents table
-- =====================================================
CREATE TABLE IF NOT EXISTS documents (
    id VARCHAR(36) PRIMARY KEY,
    company_id VARCHAR(36) NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    filing_type VARCHAR(20) NOT NULL,
    filing_date DATE NOT NULL,
    source_url VARCHAR(500),
    local_path VARCHAR(500),
    s3_key VARCHAR(500),
    content_hash VARCHAR(64),
    word_count INT,
    chunk_count INT,
    status VARCHAR(20) DEFAULT 'pending',
    error_message VARCHAR(1000),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    processed_at TIMESTAMP_NTZ
);

-- =====================================================
-- Document chunks table
-- =====================================================
CREATE TABLE IF NOT EXISTS document_chunks (
    id VARCHAR(36) PRIMARY KEY,
    document_id VARCHAR(36) NOT NULL,
    chunk_index INT NOT NULL,
    content TEXT NOT NULL,
    section VARCHAR(50),
    start_char INT,
    end_char INT,
    word_count INT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- External signals table
-- =====================================================
CREATE TABLE IF NOT EXISTS external_signals (
    id VARCHAR(36) PRIMARY KEY,
    company_id VARCHAR(36) NOT NULL,
    category VARCHAR(30) NOT NULL,
    source VARCHAR(30) NOT NULL,
    signal_date DATE NOT NULL,
    raw_value VARCHAR(500),
    normalized_score DECIMAL(5,2),
    confidence DECIMAL(4,3),
    metadata VARIANT, -- Snowflake JSON type
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- Company signal summary
-- =====================================================
CREATE TABLE IF NOT EXISTS company_signal_summaries (
    company_id VARCHAR(36) PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    technology_hiring_score DECIMAL(5,2),
    innovation_activity_score DECIMAL(5,2),
    digital_presence_score DECIMAL(5,2),
    leadership_signals_score DECIMAL(5,2),
    composite_score DECIMAL(5,2),
    signal_count INT,
    last_updated TIMESTAMP_NTZ
);
