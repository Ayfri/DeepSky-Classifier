from fastapi.testclient import TestClient


class TestModelInfoEndpoint:
	def test_returns_200(self, api_client: TestClient):
		resp = api_client.get("/model/info")
		assert resp.status_code == 200

	def test_required_fields_present(self, api_client: TestClient):
		data = api_client.get("/model/info").json()
		assert "feature_columns" in data
		assert "labels" in data
		assert "model_sha256" in data

	def test_labels_are_known_classes(self, api_client: TestClient):
		data = api_client.get("/model/info").json()
		for label in data["labels"]:
			assert label in {"STAR", "GALAXY", "QSO"}

	def test_feature_columns_sorted(self, api_client: TestClient):
		data = api_client.get("/model/info").json()
		cols = data["feature_columns"]
		assert cols == sorted(cols)

	def test_without_model_returns_503(self):
		from src.api.main import app

		with TestClient(app, raise_server_exceptions=False) as client:
			client.app.state.model = None
			client.app.state.metadata = None
			resp = client.get("/model/info")
		assert resp.status_code == 503
