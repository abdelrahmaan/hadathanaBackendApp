from app.config import Settings


def test_langsmith_prod_uses_prod_specific_alias(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    monkeypatch.setenv("LANGSMITH_API_KEY_PROD", "prod-key")
    monkeypatch.delenv("LANGSMITH_PROJECT", raising=False)
    monkeypatch.setenv("LANGSMITH_PROJECT_PROD", "prod-project")

    settings = Settings(_env_file=None)

    assert settings.get_langsmith_api_key() == "prod-key"
    assert settings.get_langsmith_project() == "prod-project"


def test_langsmith_dev_uses_dev_values(monkeypatch):
    monkeypatch.setenv("APP_ENV", "dev")
    monkeypatch.setenv("LANGSMITH_API_KEY_DEV", "dev-key")
    monkeypatch.setenv("LANGSMITH_PROJECT_DEV", "dev-project")

    settings = Settings(_env_file=None)

    assert settings.get_langsmith_api_key() == "dev-key"
    assert settings.get_langsmith_project() == "dev-project"
