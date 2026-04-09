"""
Raw and curated data contracts.

Raw schemas mirror the source catalog shape exactly.
The curated schema defines the stable ML-ready feature contract
that downstream training code depends on.
"""
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.core.config import ALLOWED_CLASSES


# ---------------------------------------------------------------------------
# Raw source schemas (one per catalog, preserving provenance)
# ---------------------------------------------------------------------------

class SDSSRawRecord(BaseModel):
	model_config = ConfigDict(from_attributes=True)

	class_label: str
	dec: float
	g: float
	i: float
	objid: int
	r: float
	ra: float
	redshift: float
	u: float
	z_mag: float

	@field_validator("class_label")
	@classmethod
	def normalize_class(cls, v: str) -> str:
		normalized = v.upper()
		if normalized not in ALLOWED_CLASSES:
			raise ValueError(f"Unknown class {v!r}, expected one of {ALLOWED_CLASSES}")
		return normalized


class GaiaRawRecord(BaseModel):
	model_config = ConfigDict(from_attributes=True)

	dec: float
	parallax: float | None = None
	phot_g_mean_mag: float | None = None
	pmdec: float | None = None
	pmra: float | None = None
	ra: float
	source_id: int


# ---------------------------------------------------------------------------
# Curated ML-ready contract
# ---------------------------------------------------------------------------

class CuratedFeatureRecord(BaseModel):
	"""Stable feature contract consumed by training and inference code."""
	model_config = ConfigDict(from_attributes=True)

	class_label: str
	dec: float = Field(ge=-90.0, le=90.0)
	g: float
	i: float
	objid: int
	r: float
	ra: float = Field(ge=0.0, le=360.0)
	redshift: float = Field(ge=-0.01, le=10.0)
	source: str = Field(default="sdss")
	u: float
	z_mag: float

	# Gaia enrichment fields (nullable until federation is wired)
	gaia_parallax: float | None = None
	gaia_pmdec: float | None = None
	gaia_pmra: float | None = None
	gaia_source_id: int | None = None

	# Infrared enrichment (nullable until WISE/DESI is wired)
	wise_w1: float | None = None
	wise_w2: float | None = None

	@field_validator("class_label")
	@classmethod
	def check_class_label(cls, v: str) -> str:
		normalized = v.upper()
		if normalized not in ALLOWED_CLASSES:
			raise ValueError(f"Unknown class {v!r}, expected one of {ALLOWED_CLASSES}")
		return normalized

	@property
	def base_feature_columns(self) -> list[str]:
		return ["dec", "g", "i", "r", "ra", "redshift", "u", "z_mag"]
