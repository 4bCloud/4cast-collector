from __future__ import annotations
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Collection
    max_concurrent_accounts: int = Field(10, description="Max accounts collected in parallel")
    max_concurrent_apis_per_account: int = Field(
        8, description="Max API calls per account in parallel"
    )
    collection_timeout_seconds: int = Field(300, description="Max collection time per run")
    active_region_min_spend_usd: float = Field(
        1.0,
        description="Minimum CE spend for a region to be auto-scanned.",
    )

    # SaaS worker
    worker_mode: bool = Field(False, description="Run as stateless SaaS worker")
    log_level: str = Field("info", description="Worker log level")
    log_format: str = Field("json", description="Worker log format: json | text")
    
    # Redis is for progress and cache ONLY (ADR-5)
    redis_url: str = Field("redis://localhost:6379/0", description="Redis URL for progress/real-time")
    
    # Postgres is the durable job queue (ADR-1)
    postgres_jobs_database_url: str = Field(
        "",
        description="Postgres URL for durable jobs. Defaults to DATABASE_URL when empty.",
    )
    database_url: str = Field(
        "",
        description="Fallback database URL used when POSTGRES_JOBS_DATABASE_URL is empty.",
    )
    worker_stages: str = Field(
        "collect",
        description="Comma-separated Postgres job stages this worker can claim.",
    )
    worker_heartbeat_interval_seconds: int = Field(
        30,
        description="Seconds between Postgres job heartbeats.",
    )
    
    # API & Auth
    api_base_url: str = Field("http://localhost:8000", description="4Cast API base URL")
    worker_api_key: str = Field("", description="Shared secret for internal API endpoints")
    worker_idle_sleep_seconds: float = Field(1.0, description="Sleep between idle polls")
    worker_concurrency: int = Field(1, description="Max concurrent jobs per worker pod")
    job_timeout_seconds: int = Field(1800, description="Max seconds per scan job")
    
    # Object Storage
    object_storage_endpoint: str = Field("", description="Optional S3-compatible endpoint")
    object_storage_access_key: str = Field("", description="Optional access key")
    object_storage_secret_key: str = Field("", description="Optional secret key")
    object_storage_bucket: str = Field("", description="Artifact bucket")

    @property
    def effective_postgres_jobs_database_url(self) -> str:
        return self.postgres_jobs_database_url or self.database_url

    @property
    def worker_stage_list(self) -> list[str]:
        return [stage.strip() for stage in self.worker_stages.split(",") if stage.strip()]

settings = Settings()
