from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Environment: "dev" or "prod"
    app_env: str = "prod"

    # MongoDB — cloud (prod, currently unused — Atlas unreachable since 2026-04-01)
    mongodb_uri_read: str = ""
    db_name: str = "HadithData"

    # MongoDB — local (dev)
    mongodb_uri_local: str = "mongodb://localhost:27017/"
    db_name_dev: str = "HadithDataDev"

    # Server
    port: int = 8000
    port_dev: int = 8001

    # CORS
    cors_origins: str = "http://localhost:3000"
    cors_origins_dev: str = "http://localhost:3000,http://localhost:5173"

    # Chatbot
    chatbot_enabled: bool = False
    cohere_api_key: str = ""
    openrouter_api_key: str = ""
    qdrant_url: str = "http://qdrant:6333"
    chatbot_model: str = "google/gemini-3-flash-preview"
    quota_free_daily: int = 3
    quota_supporter_daily: int = 10
    quota_unlimited_daily: int = -1

    # Auth (JWT + session)
    jwt_secret: str = "changeme-generate-with-openssl-rand-hex-32"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 30
    reset_token_expire_minutes: int = 30

    # Email (Resend)
    resend_api_key: str = ""
    from_email: str = "noreply@hadathana.app"

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def is_dev(self) -> bool:
        return self.app_env == "dev"

    def get_mongodb_uri(self) -> str:
        # Both dev and prod use local Docker MongoDB (Atlas unreachable since 2026-04-01).
        # URI is always injected by docker-compose via MONGODB_URI_LOCAL env var.
        return self.mongodb_uri_local

    def get_db_name(self) -> str:
        return self.db_name_dev if self.is_dev else self.db_name

    def get_port(self) -> int:
        return self.port_dev if self.is_dev else self.port

    def get_cors_origins(self) -> list[str]:
        raw = self.cors_origins_dev if self.is_dev else self.cors_origins
        return [o.strip() for o in raw.split(",") if o.strip()]

    def get_daily_limit(self, tier: str) -> int:
        return {
            "free": self.quota_free_daily,
            "supporter": self.quota_supporter_daily,
            "unlimited": self.quota_unlimited_daily,
        }.get(tier, self.quota_free_daily)


settings = Settings()
