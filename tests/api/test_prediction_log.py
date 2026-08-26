import json

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.models import PredictionLog
from tests.api._helpers import fastapi_app

_VALID_BODY = {
	"dec": 45.0,
	"g": 18.5,
	"i": 17.8,
	"r": 18.1,
	"ra": 180.0,
	"redshift": 0.001,
	"u": 19.2,
	"z_mag": 17.5,
}


def _fetch_logs(client: TestClient) -> list[PredictionLog]:
	engine = fastapi_app(client).state.engine
	with Session(engine) as session:
		return list(session.scalars(select(PredictionLog).order_by(PredictionLog.id)))


class TestPredictionHistorisation:
	def test_single_prediction_is_logged(self, api_client: TestClient):
		resp = api_client.post("/predict", json=_VALID_BODY)
		assert resp.status_code == 200

		rows = _fetch_logs(api_client)
		assert len(rows) == 1
		assert rows[0].predicted_class == resp.json()["predicted_class"]
		assert rows[0].model_sha256 == resp.json()["model_sha256"]

	def test_logged_row_carries_input_vector_and_confidence(self, api_client: TestClient):
		resp = api_client.post("/predict", json=_VALID_BODY)

		row = _fetch_logs(api_client)[0]
		assert json.loads(row.features) == _VALID_BODY
		assert row.max_proba == max(resp.json()["probabilities"].values())
		assert 0.0 < row.max_proba <= 1.0
		assert row.timestamp is not None

	def test_batch_logs_one_row_per_object(self, api_client: TestClient):
		batch = {"objects": [_VALID_BODY, _VALID_BODY, _VALID_BODY]}
		resp = api_client.post("/predict/batch", json=batch)
		assert resp.status_code == 200
		assert len(_fetch_logs(api_client)) == 3

	def test_rejected_request_is_not_logged(self, api_client: TestClient):
		resp = api_client.post("/predict", json={**_VALID_BODY, "ra": 400.0})
		assert resp.status_code == 422
		assert _fetch_logs(api_client) == []

	def test_missing_engine_degrades_to_no_logging(self, api_client: TestClient):
		fastapi_app(api_client).state.engine = None
		resp = api_client.post("/predict", json=_VALID_BODY)
		assert resp.status_code == 200
