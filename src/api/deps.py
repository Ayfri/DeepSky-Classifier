from typing import Any

from fastapi import HTTPException, Request
from sklearn.ensemble import RandomForestClassifier
from sqlalchemy import Engine


def get_model(request: Request) -> RandomForestClassifier:
	"""Resolve the loaded classifier from app state.

	The model is loaded once at startup via the lifespan context; if it
	is absent (startup failure or unit-test isolation) the endpoint
	returns 503 rather than 500 so callers can distinguish unavailability
	from bugs.
	"""
	model = getattr(request.app.state, "model", None)
	if model is None:
		raise HTTPException(status_code=503, detail="Model not loaded")
	return model


def get_metadata(request: Request) -> dict[str, Any]:
	"""Resolve run_metadata.json content from app state."""
	metadata = getattr(request.app.state, "metadata", None)
	if metadata is None:
		raise HTTPException(status_code=503, detail="Model metadata not available")
	return metadata


def get_prediction_engine(request: Request) -> Engine | None:
	"""Resolve the historisation engine from app state, or None when disabled.

	Unlike the model, the engine is optional: predictions are served even
	when the database is unavailable, they are just not historised.
	"""
	return getattr(request.app.state, "engine", None)
