from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    mongodb_uri_read: str
    db_name: str = "HadithData"
    cors_origins: str = "http://localhost:3000"

    model_config = {"env_file": ".env", "extra": "ignore"}

    def get_cors_origins(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


settings = Settings()
