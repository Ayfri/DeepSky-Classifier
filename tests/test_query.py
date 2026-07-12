import pytest

from src.core.models import CelestialBody

PLATES = [266, 1904, 7331]


class TestBuildSDSSQuery:
	def test_default_fields_are_sorted(self):
		query = CelestialBody.build_sdss_query(label="STAR", plates=PLATES)
		select_part = query.split("FROM")[0]
		assert "class_label" in select_part
		assert "redshift" in select_part
		assert "p.objid" in select_part

	def test_label_filter(self):
		query = CelestialBody.build_sdss_query(label="GALAXY", plates=PLATES)
		assert "s.class = 'GALAXY'" in query

	def test_custom_fields(self):
		query = CelestialBody.build_sdss_query(
			label="STAR",
			plates=PLATES,
			fields=["ra", "dec"],
		)
		assert "p.ra" in query
		assert "p.dec" in query
		assert "redshift" not in query

	def test_quality_filters(self):
		query = CelestialBody.build_sdss_query(label="STAR", plates=PLATES)
		assert "s.zWarning = 0" in query
		assert "p.clean = 1" in query

	def test_restricts_to_sampled_plates(self):
		query = CelestialBody.build_sdss_query(label="STAR", plates=PLATES)
		assert "s.plate IN (266, 1904, 7331)" in query

	def test_max_rows_caps_the_pool(self):
		query = CelestialBody.build_sdss_query(label="QSO", plates=PLATES, max_rows=4242)
		assert "SELECT TOP 4242" in query

	def test_requires_at_least_one_plate(self):
		with pytest.raises(ValueError, match="at least one plate"):
			CelestialBody.build_sdss_query(label="STAR", plates=[])
