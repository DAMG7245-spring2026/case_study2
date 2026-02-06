# PE Org-AI-R Platform


## Prerequisites

- **Python 3.11+**
- **Poetry** (dependency management)
- **Docker & Docker Compose**
- **Snowflake account** (free trial available)
- **AWS account** (for S3, free tier available)

---

## Quick Start

```bash
# 1. Clone the repository
cd folder name

# 2. Install Poetry (if not installed)

# 3. Install dependencies
poetry install

# 4. Configure environment
# Edit .env with your credentials

# 5. Start Redis
docker run -d --name redis-local -p 6379:6379 redis:7-alpine

# 6. Run the application
poetry run uvicorn app.main:app --reload
```

**Data:** The `data/` directory is used at runtime for transient SEC EDGAR downloads and local artifacts (e.g. `data/evidence_summary.json`); it is gitignored. Snowflake (and S3) are the source of truth for filing data.

---

## Configuration Guide

### 1. Snowflake Setup

#### Step 1: Create a Snowflake Account

1. Go to [https://signup.snowflake.com/](https://signup.snowflake.com/)
2. Sign up for a **free 30-day trial**
3. Choose your cloud provider (AWS/Azure/GCP) and region
4. Complete registration and verify your email

#### Step 2: Get Your Account Identifier


#### Step 3: Create Database and Tables

1. Log in to Snowflake Web UI
2. Click **"Worksheets"** → **"+"** to create a new worksheet
3. Execute the following SQL **step by step**:

```sql
-- Step 1: Create Warehouse and Database
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE = 'XSMALL';
USE WAREHOUSE COMPUTE_WH;
CREATE DATABASE IF NOT EXISTS PE_ORG_AIR;
USE DATABASE PE_ORG_AIR;
USE SCHEMA PUBLIC;
```

```sql
-- Step 2: Create Tables
CREATE OR REPLACE TABLE industries (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    sector VARCHAR(100) NOT NULL,
    h_r_base DECIMAL(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE companies (
    id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    ticker VARCHAR(10),
    industry_id VARCHAR(36),
    position_factor DECIMAL(4,3) DEFAULT 0.0,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE assessments (
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

CREATE OR REPLACE TABLE dimension_scores (
    id VARCHAR(36) PRIMARY KEY,
    assessment_id VARCHAR(36) NOT NULL,
    dimension VARCHAR(30) NOT NULL,
    score DECIMAL(5,2) NOT NULL,
    weight DECIMAL(4,3),
    confidence DECIMAL(4,3) DEFAULT 0.8,
    evidence_count INT DEFAULT 0,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

```sql
-- Step 3: Insert Seed Data
INSERT INTO industries (id, name, sector, h_r_base) VALUES
    ('550e8400-e29b-41d4-a716-446655440001', 'Manufacturing', 'Industrials', 72),
    ('550e8400-e29b-41d4-a716-446655440002', 'Healthcare Services', 'Healthcare', 78),
    ('550e8400-e29b-41d4-a716-446655440003', 'Business Services', 'Services', 75),
    ('550e8400-e29b-41d4-a716-446655440004', 'Retail', 'Consumer', 70),
    ('550e8400-e29b-41d4-a716-446655440005', 'Financial Services', 'Financial', 80),
    ('550e8400-e29b-41d4-a716-446655440006', 'Technology', 'Technology', 85),
    ('550e8400-e29b-41d4-a716-446655440007', 'Energy', 'Energy', 68),
    ('550e8400-e29b-41d4-a716-446655440008', 'Real Estate', 'Real Estate', 65);
```

```sql
-- Step 4: Verify Setup
SELECT * FROM industries;  -- Should show 8 rows
```

#### Step 4: Update .env File

```bash
SNOWFLAKE_ACCOUNT=XXXXX-YYYYY        # Your account identifier
SNOWFLAKE_USER=your_username          # Your login username
SNOWFLAKE_PASSWORD=your_password      # Your password
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_WAREHOUSE=
```

---

### 2. Redis Setup

Redis is used for caching to improve API performance.

#### Option A: Using Docker (Recommended)

```bash
# Start Redis container
docker run -d --name redis-local -p 6379:6379 redis:7-alpine

# Verify it's running
docker ps
```

#### Option B: Using Docker Compose

Redis is included in `docker-compose.yml` and will start automatically:

```bash
cd docker
docker-compose --env-file ../.env up
```

#### Update .env File

```bash
REDIS_HOST=localhost    # Use 'redis' if running with docker-compose
REDIS_PORT=6379
REDIS_DB=0
```

### 3. AWS S3 Setup

S3 is used for document storage (SEC filings, reports, etc.).

#### Step 1: Create an AWS Account

1. Go to [https://aws.amazon.com/](https://aws.amazon.com/)
2. Click **"Create an AWS Account"**
3. Complete the registration process

#### Step 2: Create an S3 Bucket

1. Log in to **AWS Console**
2. Search for **"S3"** and click to enter
3. Click **"Create bucket"**
4. Configure:
   - **Bucket name**: `yourname` (must be globally unique)
   - **AWS Region**: `us-east-1` (or your preferred region)
5. Keep other settings as default
6. Click **"Create bucket"**

#### Step 3: Create IAM User and Access Keys

1. In AWS Console, search for **"IAM"**
2. Click **"Users"** → **"Create user"**
3. **User name**: `your name`
4. Click **"Next"**
5. Select **"Attach policies directly"**
6. Search and select **`AmazonS3FullAccess`**
7. Click **"Next"** → **"Create user"**

8. Click on the created user → **"Security credentials"** tab
9. Scroll to **"Access keys"** → **"Create access key"**
10. Select **"Local code"** → **"Next"**
11. Click **"Create access key"**
12. **⚠️ IMPORTANT**: Copy and save both:
    - **Access key ID**: `.......`
    - **Secret access key**: `xxxxxxxx`
    
    > The secret key is shown only once!

---

## Environment Variables

Create a `.env` file in the project root:

```bash
# ==============================================
# Application Settings
# ==============================================
APP_NAME="PE Org-AI-R Platform"
APP_VERSION="1.0.0"
DEBUG=true

# ==============================================
# Snowflake Configuration
# ==============================================
SNOWFLAKE_ACCOUNT=XXXXX-YYYYY
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database_name
SNOWFLAKE_SCHEMA=
SNOWFLAKE_WAREHOUSE=

# ==============================================
# Redis Configuration
# ==============================================
REDIS_HOST=localhost    # Use 'redis' if running with docker-compose
REDIS_PORT=6379
REDIS_DB=0

# ==============================================
# AWS S3 Configuration
# ==============================================
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_aws_region
S3_BUCKET=your_bucket_name
```

**S3 is optional:** If these are empty, SEC filings are still downloaded, parsed, and stored in Snowflake; only the S3 copy is skipped. If you see `SignatureDoesNotMatch` when uploading, check: no trailing spaces/newlines in `AWS_SECRET_ACCESS_KEY`, correct `AWS_ACCESS_KEY_ID`, and `AWS_REGION` matching your bucket.

**Optional – External Signals (jobs, tech stack, patents):**  
If set, the evidence collection script will fetch real data; if omitted, those sources are skipped.

- `SERPAPI_KEY` – [SerpApi](https://serpapi.com/) (Google Jobs) for job postings
- `BUILTWITH_API_KEY` – [BuiltWith Free API](https://api.builtwith.com/free-api#usage) for tech stack / digital presence. Get the key from the Free API product; 1 req/s limit.
- `LENS_API_KEY` – [Lens.org](https://docs.api.lens.org/) patent API token for innovation/patent data (request access via Lens.org)
- `LINKEDIN_API_KEY` – (optional) Third-party LinkedIn company/exec data API (e.g. RapidAPI). If set, leadership signals can include LinkedIn-sourced data; if omitted, leadership signals use only the company website (about/leadership pages).

---

## Running the Application

### Option 1: Local Development

```bash
# Make sure Redis is running
docker run -d --name redis-local -p 6379:6379 redis:7-alpine

# Start the API
poetry run uvicorn app.main:app --reload
```

### Option 2: Docker Compose

```bash
# Start all services
cd docker
docker-compose --env-file ../.env up --build
```


### Interactive Documentation

- **Swagger UI**: http://127.0.0.1:8000/docs
- **ReDoc**: http://127.0.0.1:8000/redoc
- **OpenAPI JSON**: http://127.0.0.1:8000/openapi.json

---

## Testing

### Run All Tests

```bash
poetry run pytest
```