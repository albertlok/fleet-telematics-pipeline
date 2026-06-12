"""
Configuration for the processor service.

Each field is read from an environment variable of the same name
(case-insensitive). The defaults below suit local development; in
Kubernetes the real values come from the ConfigMap and Secret.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Also load values from a local .env file if present (dev convenience).
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:9092"

    # Topic the ingestion service publishes raw events to.
    kafka_topic_raw: str = "telemetry.raw.events"

    # Dead Letter Queue: where unprocessable messages get parked.
    kafka_topic_dlq: str = "telemetry.dlq"

    # All processor pods share this group id so Kafka load-balances
    # partitions across them.
    kafka_consumer_group: str = "telemetry-processor"

    redis_url: str = "redis://localhost:6379/0"

    # PostgreSQL connection string (DSN).
    db_dsn: str = "postgresql://fleet:fleet@localhost:5432/telemetry"

    # Batch tuning: flush to the DB every `batch_size` events OR every
    # `batch_flush_seconds` seconds, whichever comes first. Bigger
    # batches = fewer DB round-trips but more data at risk per crash.
    batch_size: int = 500
    batch_flush_seconds: float = 2.0

    log_level: str = "INFO"
