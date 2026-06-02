from __future__ import annotations

from enum import Enum
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KalshiEnv(str, Enum):
    demo = "demo"
    prod = "prod"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- Kalshi credentials ---
    kalshi_env: KalshiEnv = KalshiEnv.demo
    kalshi_api_key_id: str = Field(..., description="Key ID from Kalshi dashboard")
    kalshi_private_key_path: Path = Field(
        Path("/run/secrets/kalshi_private_key.pem"),
        description="Path to RSA private key PEM file",
    )

    # --- Postgres ---
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "kalshi"
    postgres_user: str = "kalshi"
    postgres_password: str = Field(..., description="Postgres password")
    postgres_pool_min: int = 2
    postgres_pool_max: int = 10

    # --- Alerting ---
    discord_webhook_url: str | None = None

    # --- Collector tuning ---
    orderbook_poll_interval_ms: int = 1000
    discovery_interval_sec: int = 60
    stale_market_threshold_sec: int = 1800   # 30 min; configurable per category
    monitor_check_interval_sec: int = 30

    # --- Logging ---
    log_level: str = "INFO"

    # --- Runtime metadata (injected at startup, not from env) ---
    version: str = "0.1.0"

    @field_validator("kalshi_private_key_path", mode="before")
    @classmethod
    def expand_path(cls, v: object) -> Path:
        return Path(str(v)).expanduser()

    @property
    def rest_base_url(self) -> str:
        if self.kalshi_env == KalshiEnv.prod:
            return "https://external-api.kalshi.com/trade-api/v2"
        return "https://external-api.demo.kalshi.co/trade-api/v2"

    @property
    def ws_url(self) -> str:
        if self.kalshi_env == KalshiEnv.prod:
            return "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
        return "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )
