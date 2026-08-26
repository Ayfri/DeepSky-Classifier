import json

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from src.api.schemas import PredictionRequest, PredictionResponse
from src.core.models import PredictionLog
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def record_predictions(
	engine: Engine,
	requests: list[PredictionRequest],
	responses: list[PredictionResponse],
) -> None:
	"""Persist served predictions to ``prediction_log``.

	Runs as a FastAPI background task after the response is sent, so a
	database outage degrades to a logged warning instead of a failed
	prediction: historisation is an observability feature, not a
	dependency of the inference path.
	"""
	rows = [
		PredictionLog(
			features=json.dumps(req.model_dump(), sort_keys=True),
			max_proba=max(resp.probabilities.values()),
			model_sha256=resp.model_sha256,
			predicted_class=resp.predicted_class,
		)
		for req, resp in zip(requests, responses, strict=True)
	]
	try:
		with Session(engine) as session:
			session.add_all(rows)
			session.commit()
	except Exception as exc:
		logger.warning("Prediction historisation failed (%d rows): %s", len(rows), exc)
