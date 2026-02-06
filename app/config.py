"""Configuration management using Pydantic Settings."""
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    # Application
    app_name: str = "PE Org-AI-R Platform"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # Snowflake
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_database: str = "PE_ORG_AIR"
    snowflake_schema: str = "PUBLIC"
    snowflake_warehouse: str = "COMPUTE_WH"
    
    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    
    # AWS S3
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""
    aws_region: str = "us-east-1"
    s3_bucket: str = ""
    
    # Cache TTLs (seconds)
    cache_ttl_company: int = 300  # 5 minutes
    cache_ttl_industry: int = 3600  # 1 hour
    cache_ttl_assessment: int = 120  # 2 minutes
    cache_ttl_dimension_weights: int = 86400  # 24 hours

    # External signals APIs (optional; if missing, that source is skipped)
    # Use SERPAPI_KEY, BUILTWITH_API_KEY, LENS_API_KEY, LINKEDIN_API_KEY in .env (or alternate names below)
    serpapi_key: str = Field(default="", validation_alias="SERPAPI_KEY")
    builtwith_api_key: str = Field(default="", validation_alias="BUILTWITH_API_KEY")
    lens_api_key: str = Field(default="", validation_alias="LENS_API_KEY")
    # Optional: third-party LinkedIn company/exec data API (e.g. RapidAPI); if empty, LinkedIn source is skipped
    linkedin_api_key: str = Field(default="", validation_alias="LINKEDIN_API_KEY")


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()