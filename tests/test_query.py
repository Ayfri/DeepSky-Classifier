from src.core.models import CelestialBody


class TestBuildSDSSQuery:
	def test_default_fields_are_sorted(self):
		query = CelestialBody.build_sdss_query(limit=10, label="STAR")
		select_part = query.split("FROM")[0]
		assert "class_label" in select_part
		assert "redshift" in select_part
		assert "p.objid" in select_part

	def test_limit_is_respected(self):
		query = CelestialBody.build_sdss_query(limit=42, label="QSO")
		assert "SELECT TOP 42" in query

	def test_label_filter(self):
		query = CelestialBody.build_sdss_query(limit=5, label="GALAXY")
		assert "s.class = 'GALAXY'" in query

	def test_custom_fields(self):
		query = CelestialBody.build_sdss_query(
			limit=1, label="STAR", fields=["ra", "dec"],
		)
		assert "p.ra" in query
		assert "p.dec" in query
		assert "redshift" not in query

	def test_quality_filters(self):
		query = CelestialBody.build_sdss_query(limit=1, label="STAR")
		assert "s.zWarning = 0" in query
		assert "p.clean = 1" in query
