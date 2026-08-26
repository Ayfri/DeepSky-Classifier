from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy import Engine

from src.api.deps import get_metadata, get_model, get_prediction_engine
from src.api.history import record_predictions
from src.api.schemas import (
	BatchPredictionRequest,
	BatchPredictionResponse,
	PredictionRequest,
	PredictionResponse,
)

router = APIRouter(prefix="/predict", tags=["predict"])

_FEATURE_ORDER = ["dec", "g", "i", "r", "ra", "redshift", "u", "z_mag"]


def _request_to_array(req: PredictionRequest) -> list[float]:
	"""Serialise a request into a feature vector matching training column order."""
	return [getattr(req, col) for col in _FEATURE_ORDER]


def _build_response(
	predicted_class: str,
	proba_row: np.ndarray,
	labels: list[str],
	model_sha256: str,
) -> PredictionResponse:
	return PredictionResponse(
		predicted_class=predicted_class,
		probabilities={label: float(p) for label, p in zip(labels, proba_row, strict=True)},
		model_sha256=model_sha256,
	)


@router.post("", response_model=PredictionResponse)
def predict(
	body: PredictionRequest,
	background: BackgroundTasks,
	model: RandomForestClassifier = Depends(get_model),
	metadata: dict[str, Any] = Depends(get_metadata),
	engine: Engine | None = Depends(get_prediction_engine),
) -> PredictionResponse:
	"""Classify a single astronomical object.

	The feature vector order is fixed at training time and stored in
	run_metadata.json; the API contract mirrors that order so that
	model upgrades only require re-deploying the artefact, not the API.
	"""
	X = np.array([_request_to_array(body)])
	predicted_class: str = model.predict(X)[0]
	proba_row: np.ndarray = model.predict_proba(X)[0]
	labels: list[str] = metadata.get("labels", list(model.classes_))
	sha: str = metadata.get("model_sha256", "")

	response = _build_response(predicted_class, proba_row, labels, sha)
	if engine is not None:
		background.add_task(record_predictions, engine, [body], [response])
	return response


@router.post("/batch", response_model=BatchPredictionResponse)
def predict_batch(
	body: BatchPredictionRequest,
	background: BackgroundTasks,
	model: RandomForestClassifier = Depends(get_model),
	metadata: dict[str, Any] = Depends(get_metadata),
	engine: Engine | None = Depends(get_prediction_engine),
) -> BatchPredictionResponse:
	"""Classify multiple objects in a single call.

	Batching amortises model.predict overhead; the response preserves
	input ordering so callers can zip results with their original list.
	"""
	X = np.array([_request_to_array(req) for req in body.objects])
	predictions_raw: np.ndarray = model.predict(X)
	probas: np.ndarray = model.predict_proba(X)
	labels: list[str] = metadata.get("labels", list(model.classes_))
	sha: str = metadata.get("model_sha256", "")

	responses = [
		_build_response(str(pred), proba_row, labels, sha)
		for pred, proba_row in zip(predictions_raw, probas, strict=True)
	]
	if engine is not None:
		background.add_task(record_predictions, engine, list(body.objects), responses)
	return BatchPredictionResponse(predictions=responses, model_sha256=sha)
