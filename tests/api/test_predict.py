from fastapi.testclient import TestClient

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


class TestPredictEndpoint:
	def test_valid_request_returns_200(self, api_client: TestClient):
		resp = api_client.post("/predict", json=_VALID_BODY)
		assert resp.status_code == 200

	def test_response_has_required_fields(self, api_client: TestClient):
		resp = api_client.post("/predict", json=_VALID_BODY)
		data = resp.json()
		assert "predicted_class" in data
		assert "probabilities" in data
		assert "model_sha256" in data

	def test_predicted_class_is_valid(self, api_client: TestClient):
		resp = api_client.post("/predict", json=_VALID_BODY)
		assert resp.json()["predicted_class"] in {"STAR", "GALAXY", "QSO"}

	def test_probabilities_sum_to_one(self, api_client: TestClient):
		resp = api_client.post("/predict", json=_VALID_BODY)
		proba = resp.json()["probabilities"]
		assert abs(sum(proba.values()) - 1.0) < 1e-6

	def test_invalid_ra_returns_422(self, api_client: TestClient):
		body = {**_VALID_BODY, "ra": 400.0}
		resp = api_client.post("/predict", json=body)
		assert resp.status_code == 422

	def test_invalid_redshift_returns_422(self, api_client: TestClient):
		body = {**_VALID_BODY, "redshift": 99.0}
		resp = api_client.post("/predict", json=body)
		assert resp.status_code == 422

	def test_missing_field_returns_422(self, api_client: TestClient):
		body = {k: v for k, v in _VALID_BODY.items() if k != "dec"}
		resp = api_client.post("/predict", json=body)
		assert resp.status_code == 422


class TestBatchPredictEndpoint:
	def test_batch_returns_correct_count(self, api_client: TestClient):
		batch = {"objects": [_VALID_BODY, _VALID_BODY, _VALID_BODY]}
		resp = api_client.post("/predict/batch", json=batch)
		assert resp.status_code == 200
		assert len(resp.json()["predictions"]) == 3

	def test_empty_batch_returns_422(self, api_client: TestClient):
		resp = api_client.post("/predict/batch", json={"objects": []})
		assert resp.status_code == 422

	def test_batch_all_valid_classes(self, api_client: TestClient):
		batch = {"objects": [_VALID_BODY] * 5}
		resp = api_client.post("/predict/batch", json=batch)
		for pred in resp.json()["predictions"]:
			assert pred["predicted_class"] in {"STAR", "GALAXY", "QSO"}
