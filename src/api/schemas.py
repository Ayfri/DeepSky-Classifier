from typing import Any

from pydantic import BaseModel, Field


class PredictionRequest(BaseModel):
	"""Single-object inference request.

	Feature bounds follow the SDSS photometric survey limits.
	Redshift is bounded to cover quasar regime (z ≤ 10) while allowing
	small negative values from spectral noise.
	"""

	dec: float = Field(ge=-90.0, le=90.0, description="Declination in degrees")
	g: float = Field(description="SDSS g-band magnitude")
	i: float = Field(description="SDSS i-band magnitude")
	r: float = Field(description="SDSS r-band magnitude")
	ra: float = Field(ge=0.0, le=360.0, description="Right ascension in degrees")
	redshift: float = Field(ge=-0.01, le=10.0, description="Spectroscopic redshift")
	u: float = Field(description="SDSS u-band magnitude")
	z_mag: float = Field(description="SDSS z-band magnitude")


class PredictionResponse(BaseModel):
	"""Classifier output for a single object."""

	predicted_class: str
	probabilities: dict[str, float]
	model_sha256: str


class BatchPredictionRequest(BaseModel):
	"""Batch inference request — list of feature records."""

	objects: list[PredictionRequest] = Field(min_length=1, max_length=10_000)


class BatchPredictionResponse(BaseModel):
	"""Batch inference results in the same order as the request."""

	predictions: list[PredictionResponse]
	model_sha256: str


class ModelInfo(BaseModel):
	"""Metadata about the loaded classifier artefact."""

	feature_columns: list[str]
	labels: list[str]
	model_sha256: str
	n_estimators: int | None = None
	trained_at: str | None = None
	dataset_sha256: str | None = None
	metrics: dict[str, Any] | None = None
