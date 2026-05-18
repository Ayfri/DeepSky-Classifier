from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
	"""Runtime configuration loaded from environment variables.

	All variables are prefixed with ``DEEPSKY_`` to avoid collisions.
	PostgreSQL connection is built from individual parts so that each
	component (host, port, credentials) can be injected independently
	in containerised deployments.
	"""

	model_config = SettingsConfigDict(
		env_prefix="DEEPSKY_",
		env_file=".env",
		env_file_encoding="utf-8",
		case_sensitive=False,
	)

	# Database
	db_url: str | None = Field(
		default=None,
		description="Full SQLAlchemy DB URL. When set, overrides individual DB_* vars.",
	)
	db_host: str = Field(default="localhost")
	db_port: int = Field(default=5432, ge=1, le=65535)
	db_name: str = Field(default="deepsky")
	db_user: str = Field(default="deepsky")
	db_password: str = Field(default="deepsky")
	db_pool_size: int = Field(default=5, ge=1)
	db_max_overflow: int = Field(default=10, ge=0)

	# Model artefacts
	model_dir: Path = Field(default=Path("models"))

	# API
	api_host: str = Field(default="0.0.0.0")
	api_port: int = Field(default=8000, ge=1, le=65535)
	api_workers: int = Field(default=1, ge=1)

	@property
	def postgres_url(self) -> str:
		"""Synchronous psycopg URL, used by SQLAlchemy and Alembic."""
		if self.db_url:
			return self.db_url
		return (
			f"postgresql+psycopg://{self.db_user}:{self.db_password}"
			f"@{self.db_host}:{self.db_port}/{self.db_name}"
		)

	@property
	def sqlite_url(self) -> str:
		"""SQLite fallback used when no DEEPSKY_DB_* vars are set."""
		db_path = Path("data") / "deepsky.db"
		db_path.parent.mkdir(parents=True, exist_ok=True)
		return f"sqlite:///{db_path}"

	@property
	def effective_db_url(self) -> str:
		"""Return postgres_url if DB env vars are configured, else sqlite_url."""
		has_postgres_config = (
			self.db_url is not None
			or self.db_host != "localhost"
			or self.db_name != "deepsky"
			or self.db_user != "deepsky"
			or self.db_password != "deepsky"
		)
		return self.postgres_url if has_postgres_config else self.sqlite_url


_settings: Settings | None = None


def get_settings() -> Settings:
	"""Return the singleton Settings instance (lazy init)."""
	global _settings
	if _settings is None:
		_settings = Settings()
	return _settings
