from collections.abc import Generator
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from src.api.main import app


def _bare_client() -> TestClient:
	"""Client with NO model artefacts on disk — simulates cold-start failure."""
	return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def _empty_model_dir(tmp_path: Path) -> Generator[None]:
	"""Point settings at an empty dir so lifespan startup fails to find a model."""
	import src.core.settings as settings_module
	from src.core.settings import Settings

	settings_module._settings = Settings(model_dir=tmp_path)
	yield
	settings_module._settings = None


class TestHealthEndpoint:
	@pytest.mark.usefixtures("_empty_model_dir")
	def test_health_always_200(self):
		with _bare_client() as client:
			resp = client.get("/health")
		assert resp.status_code == 200
		assert resp.json() == {"status": "ok"}

	@pytest.mark.usefixtures("_empty_model_dir")
	def test_readiness_503_without_model(self):
		with _bare_client() as client:
			resp = client.get("/readiness")
		assert resp.status_code == 503

	def test_readiness_200_with_model(self, api_client: TestClient):
		resp = api_client.get("/readiness")
		assert resp.status_code == 200
		assert resp.json()["status"] == "ready"
