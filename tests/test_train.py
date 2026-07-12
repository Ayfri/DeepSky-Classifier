import json
from pathlib import Path

import pandas as pd
import pytest

from src.ml.train import DEFAULT_FIGURES_DIR, train_classifier


class TestTrainClassifier:
	def test_produces_model_and_metadata(self, sample_curated_df: pd.DataFrame, tmp_path: Path):
		data_file = tmp_path / "curated.parquet"
		sample_curated_df.to_parquet(data_file, index=False)

		model_path = train_classifier(
			data_path=data_file, output_dir=tmp_path, figures_dir=tmp_path
		)

		assert model_path.exists()
		assert (tmp_path / "run_metadata.json").exists()

	def test_metadata_has_required_keys(self, sample_curated_df: pd.DataFrame, tmp_path: Path):
		data_file = tmp_path / "curated.parquet"
		sample_curated_df.to_parquet(data_file, index=False)
		train_classifier(data_path=data_file, output_dir=tmp_path, figures_dir=tmp_path)

		metadata = json.loads((tmp_path / "run_metadata.json").read_text(encoding="utf-8"))
		required = {
			"model_sha256",
			"dataset_sha256",
			"feature_columns",
			"labels",
			"metrics",
			"timestamp",
		}
		assert required.issubset(metadata.keys())

	def test_csv_format_supported(self, sample_curated_df: pd.DataFrame, tmp_path: Path):
		data_file = tmp_path / "curated.csv"
		sample_curated_df.to_csv(data_file, index=False)
		model_path = train_classifier(
			data_path=data_file, output_dir=tmp_path, figures_dir=tmp_path
		)
		assert model_path.exists()

	def test_unsupported_format_raises(self, tmp_path: Path):
		bad_file = tmp_path / "curated.json"
		bad_file.write_text("{}", encoding="utf-8")
		with pytest.raises(ValueError, match="Unsupported"):
			train_classifier(data_path=bad_file, output_dir=tmp_path, figures_dir=tmp_path)

	def test_model_can_predict(self, sample_curated_df: pd.DataFrame, tmp_path: Path):
		import joblib
		import numpy as np

		data_file = tmp_path / "curated.parquet"
		sample_curated_df.to_parquet(data_file, index=False)
		model_path = train_classifier(
			data_path=data_file, output_dir=tmp_path, figures_dir=tmp_path, n_estimators=10
		)

		clf = joblib.load(model_path)
		X = np.zeros((1, 8))
		pred = clf.predict(X)
		assert pred[0] in {"STAR", "GALAXY", "QSO"}

	def test_figures_never_touch_the_reporting_dir(
		self, sample_curated_df: pd.DataFrame, tmp_path: Path
	):
		"""Regression: figures_dir was hardcoded, so the suite overwrote the real reports/figures.

		A toy fixture produced a nonsense ROC (AUC below 0.5) that then sat in the repo next to a
		model claiming 98% F1.
		"""
		data_file = tmp_path / "curated.parquet"
		sample_curated_df.to_parquet(data_file, index=False)
		before = {path: path.read_bytes() for path in DEFAULT_FIGURES_DIR.glob("*.png")}

		train_classifier(data_path=data_file, output_dir=tmp_path, figures_dir=tmp_path)

		assert (tmp_path / "roc_curves.png").exists()
		assert (tmp_path / "confusion_matrix.png").exists()
		assert (tmp_path / "feature_importance.png").exists()
		assert {path: path.read_bytes() for path in DEFAULT_FIGURES_DIR.glob("*.png")} == before
