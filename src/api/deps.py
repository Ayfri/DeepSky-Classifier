from typing import TYPE_CHECKING, Any

from fastapi import Depends, HTTPException, Request

from src.core.settings import Settings, get_settings

if TYPE_CHECKING:
	from sklearn.ensemble import RandomForestClassifier


def get_model(request: Request) -> "RandomForestClassifier":
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


def settings_dep() -> Settings:
	"""FastAPI Depends wrapper around the Settings singleton."""
	return get_settings()


SettingsDep = Depends(settings_dep)
