"""
Configuration for the ingestion service.

pydantic-settings reads each field from an environment variable with the
same name (case-insensitive): webhook_secret ← WEBHOOK_SECRET, etc.
The values below are only DEFAULTS, used when the env var is not set —
handy for local development, overridden by the ConfigMap/Secret in
Kubernetes.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Also load values from a local .env file if present (dev convenience).
    model_config = SettingsConfigDict(env_file=".env")

    # Shared secret for verifying webhook signatures.
    # Empty string = verification disabled (local dev only!).
    webhook_secret: str = ""

    # Comma-separated list of Kafka brokers.
    # "kafka" resolves inside Kubernetes; use localhost for local dev.
    kafka_bootstrap_servers: str = "localhost:9092"

    # Topic where raw, untransformed webhook payloads are published.
    kafka_topic_raw: str = "telemetry.raw.events"

    log_level: str = "INFO"
