import json
from pathlib import Path

import pandas as pd

from src.ml.optimize import compare_feature_sets, run_hyperparameter_search

# Tiny grid + 2-fold CV to keep the unit test fast.
TINY_GRID = {"n_estimators": [5, 10], "max_depth": [None, 5]}


class TestHyperparameterSearch:
	def test_writes_results_and_best_params(self, sample_curated_df: pd.DataFrame, tmp_path: Path):
		data_file = tmp_path / "curated.parquet"
		sample_curated_df.to_parquet(data_file, index=False)

		results = run_hyperparameter_search(
			data_path=data_file,
			output_dir=tmp_path,
			cv=2,
			param_grid=TINY_GRID,
		)

		assert (tmp_path / "hyperparam_results.json").exists()
		assert results["best_params"]["n_estimators"] in TINY_GRID["n_estimators"]
		assert "f1_macro_delta" in results

	def test_report_is_valid_json(self, sample_curated_df: pd.DataFrame, tmp_path: Path):
		data_file = tmp_path / "curated.parquet"
		sample_curated_df.to_parquet(data_file, index=False)
		run_hyperparameter_search(
			data_path=data_file, output_dir=tmp_path, cv=2, param_grid=TINY_GRID
		)

		report = json.loads((tmp_path / "hyperparam_results.json").read_text(encoding="utf-8"))
		assert {"best_params", "default_metrics", "tuned_metrics"}.issubset(report.keys())


class TestCompareFeatureSets:
	def test_baseline_only_when_no_gaia(self, sample_curated_df: pd.DataFrame, tmp_path: Path):
		data_file = tmp_path / "curated.parquet"
		sample_curated_df.to_parquet(data_file, index=False)

		comparison = compare_feature_sets(data_path=data_file, n_estimators=5)

		assert comparison["gaia_enriched"] is None
		assert "f1_macro" in comparison["baseline"]
