from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI
import joblib
import uvicorn

from src.api.routes import health, model, predict
from src.core.settings import get_settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def _load_model_artefacts(model_dir: Path) -> tuple[object, dict[str, Any]]:
	"""Load classifier and metadata from the model directory.

	Both files must exist; if either is missing the application refuses
	to start so that readiness probes fail cleanly instead of serving
	garbage predictions.
	"""
	model_path = model_dir / "rf_classifier.joblib"
	metadata_path = model_dir / "run_metadata.json"

	if not model_path.exists():
		raise FileNotFoundError(f"Model artefact not found: {model_path}")
	if not metadata_path.exists():
		raise FileNotFoundError(f"Run metadata not found: {metadata_path}")

	clf = joblib.load(model_path)
	metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
	return clf, metadata


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
	"""Load the ML model once at startup; release resources on shutdown."""
	settings = get_settings()
	try:
		clf, metadata = _load_model_artefacts(settings.model_dir)
		app.state.model = clf
		app.state.metadata = metadata
		logger.info(
			"Model loaded from %s (sha256=%s…)",
			settings.model_dir,
			metadata.get("model_sha256", "unknown")[:12],
		)
	except FileNotFoundError:
		logger.exception("Startup failed — model artefacts missing")
		app.state.model = None
		app.state.metadata = None

	yield

	app.state.model = None
	app.state.metadata = None
	logger.info("Model unloaded, shutdown complete")


app = FastAPI(
	title="DeepSky Classifier API",
	description="Classify astronomical objects (STAR / GALAXY / QSO) via Random Forest.",
	version="0.2.0",
	lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(predict.router)
app.include_router(model.router)


def run() -> None:
	"""CLI entry point declared in pyproject.toml [project.scripts]."""
	settings = get_settings()
	uvicorn.run(
		"src.api.main:app",
		host=settings.api_host,
		port=settings.api_port,
		workers=settings.api_workers,
		reload=False,
	)
