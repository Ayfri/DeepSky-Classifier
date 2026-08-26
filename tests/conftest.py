from collections.abc import Generator
import hashlib
import json
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.ensemble import RandomForestClassifier

from src.ml.features import BASELINE_FEATURES, LABEL_COLUMN

# Dataset fixtures


@pytest.fixture
def sample_curated_df() -> pd.DataFrame:
	"""Minimal curated DataFrame with one record per class."""
	rng = np.random.default_rng(42)
	n = 30
	data: dict[str, Any] = {col: rng.uniform(0, 25, n).tolist() for col in BASELINE_FEATURES}
	data["ra"] = rng.uniform(0, 360, n).tolist()
	data["dec"] = rng.uniform(-90, 90, n).tolist()
	data["redshift"] = rng.uniform(0, 2, n).tolist()
	data[LABEL_COLUMN] = ["STAR"] * 10 + ["GALAXY"] * 10 + ["QSO"] * 10
	data["objid"] = list(range(1, n + 1))
	data["source"] = ["sdss"] * n
	return pd.DataFrame(data)


# Model fixtures


@pytest.fixture
def trained_model(sample_curated_df: pd.DataFrame) -> RandomForestClassifier:
	"""Tiny RandomForest trained on synthetic data — for unit tests only."""
	from src.ml.features import select_features

	X, y = select_features(sample_curated_df)
	clf = RandomForestClassifier(n_estimators=5, random_state=42)
	clf.fit(X, y)
	return clf


@pytest.fixture
def tmp_model_dir(tmp_path: Path, trained_model: RandomForestClassifier) -> Path:
	"""Write a model artefact + metadata to a temp dir, return the dir."""
	model_path = tmp_path / "rf_classifier.joblib"
	joblib.dump(trained_model, model_path)

	sha = hashlib.sha256(model_path.read_bytes()).hexdigest()
	metadata: dict[str, Any] = {
		"feature_columns": sorted(BASELINE_FEATURES),
		"labels": ["GALAXY", "QSO", "STAR"],
		"model_sha256": sha,
		"n_estimators": 5,
		"timestamp": "2026-01-01T00:00:00+00:00",
		"dataset_sha256": "abc123",
		"metrics": {"accuracy": 0.9, "f1_macro": 0.9},
	}
	(tmp_path / "run_metadata.json").write_text(json.dumps(metadata), encoding="utf-8")
	return tmp_path


# API client fixture


@pytest.fixture
def api_client(tmp_model_dir: Path) -> Generator[TestClient]:
	"""TestClient with the model loaded from tmp_model_dir and a throwaway SQLite DB."""
	from src.api.main import _load_model_artefacts, app
	import src.core.settings as settings_module
	from src.core.settings import Settings
	from tests.api._helpers import fastapi_app

	settings = Settings(
		model_dir=tmp_model_dir, db_url=f"sqlite:///{tmp_model_dir / 'api_test.db'}"
	)
	settings_module._settings = settings

	clf, meta = _load_model_artefacts(tmp_model_dir)
	with TestClient(app) as client:
		app_state = fastapi_app(client).state
		app_state.model = clf
		app_state.metadata = meta
		yield client

	settings_module._settings = None
