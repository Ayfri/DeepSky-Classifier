"""Integration tests — require a live Postgres instance (DEEPSKY_DB_* env vars)."""

import os
from collections.abc import Generator

import pytest
from sqlalchemy import Engine, inspect, text

from src.core.database import get_engine, ping
from src.core.models import CelestialBody, CuratedCelestialBody
from src.core.settings import Settings


def _pg_settings() -> Settings:
	return Settings(
		db_host=os.environ.get("DEEPSKY_DB_HOST", "localhost"),
		db_port=int(os.environ.get("DEEPSKY_DB_PORT", "5432")),
		db_name=os.environ.get("DEEPSKY_DB_NAME", "deepsky"),
		db_user=os.environ.get("DEEPSKY_DB_USER", "deepsky"),
		db_password=os.environ.get("DEEPSKY_DB_PASSWORD", "deepsky"),
	)


@pytest.fixture(scope="module")
def pg_engine() -> Generator[Engine]:
	engine = get_engine(_pg_settings())
	yield engine
	engine.dispose()


# ── connectivity ─────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_db_ping(pg_engine: Engine) -> None:
	assert ping(pg_engine) is True


@pytest.mark.integration
def test_db_select_one(pg_engine: Engine) -> None:
	with pg_engine.connect() as conn:
		row = conn.execute(text("SELECT 1 AS val")).fetchone()
	assert row is not None
	assert row.val == 1


# ── schema (applied by Alembic in CI before this step runs) ──────────────────


@pytest.mark.integration
def test_tables_exist(pg_engine: Engine) -> None:
	insp = inspect(pg_engine)
	tables = insp.get_table_names()
	assert "celestial_bodies" in tables
	assert "curated_celestial_bodies" in tables


@pytest.mark.integration
def test_celestial_bodies_columns(pg_engine: Engine) -> None:
	insp = inspect(pg_engine)
	cols = {c["name"] for c in insp.get_columns("celestial_bodies")}
	expected = {"objid", "class_label", "ra", "dec", "u", "g", "r", "i", "z_mag", "redshift"}
	assert expected <= cols


@pytest.mark.integration
def test_curated_celestial_bodies_columns(pg_engine: Engine) -> None:
	insp = inspect(pg_engine)
	cols = {c["name"] for c in insp.get_columns("curated_celestial_bodies")}
	expected = {
		"objid",
		"class_label",
		"ra",
		"dec",
		"u",
		"g",
		"r",
		"i",
		"z_mag",
		"redshift",
		"source",
		"gaia_parallax",
		"gaia_pmdec",
		"gaia_pmra",
		"gaia_source_id",
	}
	assert expected <= cols


# ── ORM round-trip ────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_celestial_body_insert_and_query(pg_engine: Engine) -> None:
	from sqlalchemy.orm import Session

	record = CelestialBody(
		objid=999_000_001,
		class_label="STAR",
		ra=10.0,
		dec=20.0,
		u=18.1,
		g=17.5,
		r=17.0,
		i=16.8,
		z_mag=16.6,
		redshift=0.0,
	)
	with Session(pg_engine) as session:
		session.merge(record)
		session.commit()
		fetched = session.get(CelestialBody, 999_000_001)
		assert fetched is not None
		assert fetched.class_label == "STAR"
		assert fetched.ra == pytest.approx(10.0)
		session.delete(fetched)
		session.commit()


@pytest.mark.integration
def test_curated_celestial_body_insert_and_query(pg_engine: Engine) -> None:
	from sqlalchemy.orm import Session

	record = CuratedCelestialBody(
		objid=999_000_002,
		class_label="GALAXY",
		ra=15.0,
		dec=-30.0,
		u=20.1,
		g=19.8,
		r=19.5,
		i=19.2,
		z_mag=19.0,
		redshift=0.35,
		source="sdss",
		gaia_parallax=None,
		gaia_pmdec=None,
		gaia_pmra=None,
		gaia_source_id=None,
	)
	with Session(pg_engine) as session:
		session.merge(record)
		session.commit()
		fetched = session.get(CuratedCelestialBody, 999_000_002)
		assert fetched is not None
		assert fetched.class_label == "GALAXY"
		assert fetched.source == "sdss"
		assert fetched.gaia_parallax is None
		session.delete(fetched)
		session.commit()


@pytest.mark.integration
def test_curated_celestial_body_with_gaia(pg_engine: Engine) -> None:
	from sqlalchemy.orm import Session

	record = CuratedCelestialBody(
		objid=999_000_003,
		class_label="STAR",
		ra=45.0,
		dec=12.0,
		u=14.0,
		g=13.8,
		r=13.5,
		i=13.3,
		z_mag=13.1,
		redshift=0.0,
		source="gaia",
		gaia_parallax=12.5,
		gaia_pmdec=-3.2,
		gaia_pmra=7.8,
		gaia_source_id=123456789,
	)
	with Session(pg_engine) as session:
		session.merge(record)
		session.commit()
		fetched = session.get(CuratedCelestialBody, 999_000_003)
		assert fetched is not None
		assert fetched.gaia_parallax == pytest.approx(12.5)
		assert fetched.gaia_source_id == 123456789
		session.delete(fetched)
		session.commit()
