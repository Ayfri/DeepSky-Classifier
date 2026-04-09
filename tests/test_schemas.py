import pytest
from pydantic import ValidationError

from src.core.schemas import CuratedFeatureRecord, SDSSRawRecord


def _valid_sdss_record(**overrides: object) -> dict:
	base = {
		"class_label": "STAR",
		"dec": 45.0,
		"g": 18.5,
		"i": 17.8,
		"objid": 1234567890,
		"r": 18.1,
		"ra": 180.0,
		"redshift": 0.001,
		"u": 19.2,
		"z_mag": 17.5,
	}
	base.update(overrides)
	return base


class TestSDSSRawRecord:
	def test_valid_record(self):
		rec = SDSSRawRecord.model_validate(_valid_sdss_record())
		assert rec.class_label == "STAR"

	def test_class_label_normalized(self):
		rec = SDSSRawRecord.model_validate(_valid_sdss_record(class_label="star"))
		assert rec.class_label == "STAR"

	def test_invalid_class_rejected(self):
		with pytest.raises(ValidationError):
			SDSSRawRecord.model_validate(_valid_sdss_record(class_label="PLANET"))


class TestCuratedFeatureRecord:
	def test_valid_baseline(self):
		rec = CuratedFeatureRecord.model_validate(_valid_sdss_record())
		assert rec.source == "sdss"
		assert rec.gaia_parallax is None

	def test_ra_bounds(self):
		with pytest.raises(ValidationError):
			CuratedFeatureRecord.model_validate(_valid_sdss_record(ra=400.0))

	def test_dec_bounds(self):
		with pytest.raises(ValidationError):
			CuratedFeatureRecord.model_validate(_valid_sdss_record(dec=-100.0))

	def test_redshift_bounds(self):
		with pytest.raises(ValidationError):
			CuratedFeatureRecord.model_validate(_valid_sdss_record(redshift=15.0))

	def test_gaia_enrichment_optional(self):
		rec = CuratedFeatureRecord.model_validate(
			_valid_sdss_record(gaia_parallax=1.2, gaia_pmra=3.4, gaia_pmdec=-0.5),
		)
		assert rec.gaia_parallax == 1.2

	def test_source_field_preserved(self):
		rec = CuratedFeatureRecord.model_validate(
			_valid_sdss_record(source="sdss+gaia"),
		)
		assert rec.source == "sdss+gaia"
