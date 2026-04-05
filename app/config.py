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














# from pydantic import Field
# from pydantic_settings import BaseSettings, SettingsConfigDict


# class Settings(BaseSettings):
#     """
#     Application settings loaded from environment variables.
#     Only includes fields that are accessed via settings.X in code.
#     """
    
#     # Required fields (must be set in .env or container env)
#     DB_USER: str = Field(default=...)
#     DB_PASSWORD: str = Field(default=...)
#     DATABASE_URL: str = Field(default=...)
#     REDIS_URL: str = Field(default=...)
    
#     # ✅ REMOVED: DB_POOL_SIZE and DB_MAX_OVERFLOW
#     # These are read via os.getenv() in database.py, not via settings.X
#     # Keeping them here causes Pydantic validation conflicts with string env vars

#     # ✅ Pydantic v2 configuration
#     model_config = SettingsConfigDict(
#         extra="ignore",              # Allow unmapped env vars from docker-compose
#         env_file=".env",             # Load defaults from .env file
#         env_file_encoding="utf-8",   # Explicit encoding
#         case_sensitive=True,         # Env var names must match exactly
#         protected_namespaces=(),     # Avoid warnings for fields starting with 'model_'
#         validate_default=True,       # Validate default values too
#     )


# # Runtime instantiation: throws ValidationError ONLY for required fields (db_user, etc.)
# # Pool settings are handled separately via os.getenv() in database.py
# settings = Settings()