"""Tests for the SQLite/PostgreSQL arbitration.

The project runs on SQLite in development and on PostgreSQL in CI and production. The switch is
driven by configuration alone: without `DEEPSKY_DB_*` variables, the application falls back to a
local SQLite file. That fallback is the whole point of the arbitration, so it is pinned here.
"""

from pathlib import Path

import pytest

from src.core.settings import Settings

DB_ENV_VARS = (
	"DEEPSKY_DB_URL",
	"DEEPSKY_DB_HOST",
	"DEEPSKY_DB_NAME",
	"DEEPSKY_DB_USER",
	"DEEPSKY_DB_PASSWORD",
)


@pytest.fixture(autouse=True)
def clear_db_env(monkeypatch: pytest.MonkeyPatch) -> None:
	"""Neutralise the developer's own .env so the tests read defaults only."""
	for name in DB_ENV_VARS:
		monkeypatch.delenv(name, raising=False)


class TestEffectiveDbUrl:
	def test_falls_back_to_sqlite_when_no_db_variable_is_set(self, tmp_path: Path) -> None:
		monkeypatched_cwd = tmp_path
		with pytest.MonkeyPatch.context() as patch:
			patch.chdir(monkeypatched_cwd)
			settings = Settings(_env_file=None)

			assert settings.effective_db_url.startswith("sqlite:///")
			assert "deepsky.db" in settings.effective_db_url

	def test_switches_to_postgres_when_a_host_is_configured(self) -> None:
		settings = Settings(_env_file=None, db_host="db.internal")

		assert settings.effective_db_url == (
			"postgresql+psycopg://deepsky:deepsky@db.internal:5432/deepsky"
		)

	def test_switches_to_postgres_when_credentials_are_configured(self) -> None:
		settings = Settings(_env_file=None, db_user="deepsky_app", db_password="secret")

		assert settings.effective_db_url == (
			"postgresql+psycopg://deepsky_app:secret@localhost:5432/deepsky"
		)

	def test_an_explicit_url_overrides_the_individual_parts(self) -> None:
		settings = Settings(
			_env_file=None,
			db_url="postgresql+psycopg://someone@elsewhere:6543/other",
			db_host="ignored.internal",
		)

		assert settings.effective_db_url == "postgresql+psycopg://someone@elsewhere:6543/other"


class TestPostgresUrl:
	def test_builds_the_url_from_its_parts(self) -> None:
		settings = Settings(
			_env_file=None,
			db_host="postgres",
			db_port=5433,
			db_name="deepsky_prod",
			db_user="deepsky_app",
			db_password="secret",
		)

		assert settings.postgres_url == (
			"postgresql+psycopg://deepsky_app:secret@postgres:5433/deepsky_prod"
		)

	def test_rejects_an_out_of_range_port_from_the_environment(
		self,
		monkeypatch: pytest.MonkeyPatch,
	) -> None:
		monkeypatch.setenv("DEEPSKY_DB_PORT", "70000")

		with pytest.raises(ValueError, match="db_port"):
			Settings(_env_file=None)
