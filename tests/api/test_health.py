from fastapi.testclient import TestClient

from src.api.main import app


def _bare_client() -> TestClient:
	"""Client with NO model loaded — simulates cold-start failure."""
	client = TestClient(app, raise_server_exceptions=False)
	client.app.state.model = None
	client.app.state.metadata = None
	return client


class TestHealthEndpoint:
	def test_health_always_200(self):
		with _bare_client() as client:
			resp = client.get("/health")
		assert resp.status_code == 200
		assert resp.json() == {"status": "ok"}

	def test_readiness_503_without_model(self):
		with _bare_client() as client:
			resp = client.get("/readiness")
		assert resp.status_code == 503

	def test_readiness_200_with_model(self, api_client: TestClient):
		resp = api_client.get("/readiness")
		assert resp.status_code == 200
		assert resp.json()["status"] == "ready"
