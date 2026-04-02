from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # 'Field(default=...)' is the magic trick.
    # It satisfies Pylance's requirement for a default value
    # while keeping the field mandatory for Pydantic at runtime.
    db_user: str = Field(default=...)
    db_password: str = Field(default=...)
    DATABASE_URL: str = Field(default=...)
    REDIS_URL: str = Field(default=...)

    model_config = SettingsConfigDict(
        env_file=(".env", "../.env"), env_file_encoding="utf-8"
    )


# Pylance is now silent.
# If DATABASE_URL is missing from .env, Pydantic will throw a
# ValidationError here at runtime.
settings = Settings()
