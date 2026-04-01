from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Environment: "dev" or "prod"
    app_env: str = "prod"

    # MongoDB — cloud (prod)
    mongodb_uri_read: str
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

    model_config = {"env_file": ".env", "extra": "ignore"}

    @property
    def is_dev(self) -> bool:
        return self.app_env == "dev"

    def get_mongodb_uri(self) -> str:
        return self.mongodb_uri_local if self.is_dev else self.mongodb_uri_read

    def get_db_name(self) -> str:
        return self.db_name_dev if self.is_dev else self.db_name

    def get_port(self) -> int:
        return self.port_dev if self.is_dev else self.port

    def get_cors_origins(self) -> list[str]:
        raw = self.cors_origins_dev if self.is_dev else self.cors_origins
        return [o.strip() for o in raw.split(",") if o.strip()]


settings = Settings()
