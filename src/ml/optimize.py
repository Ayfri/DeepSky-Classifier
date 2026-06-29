from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV, train_test_split

from src.ml.evaluate import evaluate_model
from src.ml.features import GAIA_FEATURES, select_features
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

DEFAULT_MODEL_DIR = Path("models")
DEFAULT_TEST_SIZE = 0.2
DEFAULT_RANDOM_STATE = 42
DEFAULT_CV = 5
DEFAULT_SCORING = "f1_macro"

# Hyperparameter grid swept by GridSearchCV (C5.2.4).
PARAM_GRID: dict[str, list[Any]] = {
	"n_estimators": [50, 100, 200],
	"max_depth": [None, 10, 20],
	"min_samples_split": [2, 5, 10],
}


def _load_dataframe(data_path: Path) -> pd.DataFrame:
	match data_path.suffix:
		case ".parquet":
			return pd.read_parquet(data_path)
		case ".csv":
			return pd.read_csv(data_path)
		case _:
			raise ValueError(f"Unsupported data format: {data_path.suffix!r}")


def _fit_eval(
	X: pd.DataFrame,
	y: pd.Series,
	clf: RandomForestClassifier,
	test_size: float,
	random_state: int,
) -> dict[str, Any]:
	"""Fit on a stratified train split, return metrics on the held-out test split."""
	labels = sorted(y.unique().tolist())
	X_train, X_test, y_train, y_test = train_test_split(
		X, y, test_size=test_size, random_state=random_state, stratify=y
	)
	clf.fit(X_train, y_train)
	y_pred = pd.Series(clf.predict(X_test), index=y_test.index)
	return evaluate_model(y_test, y_pred, labels=labels)


def run_hyperparameter_search(
	data_path: Path,
	output_dir: Path = DEFAULT_MODEL_DIR,
	include_gaia: bool = False,
	cv: int = DEFAULT_CV,
	scoring: str = DEFAULT_SCORING,
	test_size: float = DEFAULT_TEST_SIZE,
	random_state: int = DEFAULT_RANDOM_STATE,
	param_grid: dict[str, list[Any]] | None = None,
) -> dict[str, Any]:
	"""Sweep RandomForest hyperparameters with GridSearchCV and compare against the default model.

	Demonstrates C5.2.4: tuning levers (hyperparameters), CV-based search, and a measured
	performance gain between the default and tuned estimators. Writes a JSON report.
	"""
	grid = param_grid if param_grid is not None else PARAM_GRID
	output_dir.mkdir(parents=True, exist_ok=True)

	df = _load_dataframe(data_path)
	X, y = select_features(df, include_gaia=include_gaia)
	X_train, X_test, y_train, y_test = train_test_split(
		X, y, test_size=test_size, random_state=random_state, stratify=y
	)
	labels = sorted(y.unique().tolist())

	# --- Default baseline (matches train.py defaults) ---
	default_clf = RandomForestClassifier(n_estimators=100, random_state=random_state, n_jobs=-1)
	default_clf.fit(X_train, y_train)
	default_pred = pd.Series(default_clf.predict(X_test), index=y_test.index)
	default_metrics = evaluate_model(y_test, default_pred, labels=labels)

	# --- Grid search ---
	logger.info(f"GridSearchCV: {grid} (cv={cv}, scoring={scoring})")
	search = GridSearchCV(
		RandomForestClassifier(random_state=random_state, n_jobs=-1),
		param_grid=grid,
		cv=cv,
		scoring=scoring,
		n_jobs=-1,
	)
	search.fit(X_train, y_train)

	tuned_clf = search.best_estimator_
	tuned_pred = pd.Series(tuned_clf.predict(X_test), index=y_test.index)
	tuned_metrics = evaluate_model(y_test, tuned_pred, labels=labels)

	delta_f1 = tuned_metrics["f1_macro"] - default_metrics["f1_macro"]
	logger.info(
		f"Default F1_macro={default_metrics['f1_macro']:.4f} -> "
		f"tuned F1_macro={tuned_metrics['f1_macro']:.4f} (delta={delta_f1:+.4f})"
	)

	results: dict[str, Any] = {
		"best_params": search.best_params_,
		"best_cv_score": float(search.best_score_),
		"cv": cv,
		"scoring": scoring,
		"include_gaia": include_gaia,
		"param_grid": grid,
		"default_metrics": {
			"accuracy": default_metrics["accuracy"],
			"f1_macro": default_metrics["f1_macro"],
			"f1_weighted": default_metrics["f1_weighted"],
		},
		"tuned_metrics": {
			"accuracy": tuned_metrics["accuracy"],
			"f1_macro": tuned_metrics["f1_macro"],
			"f1_weighted": tuned_metrics["f1_weighted"],
		},
		"f1_macro_delta": float(delta_f1),
		"feature_columns": sorted(X.columns.tolist()),
		"timestamp": datetime.now(UTC).isoformat(),
	}

	report_path = output_dir / "hyperparam_results.json"
	report_path.write_text(json.dumps(results, indent=2, default=str))
	logger.info(f"Hyperparameter results saved to {report_path}")
	return results


def compare_feature_sets(
	data_path: Path,
	n_estimators: int = 100,
	test_size: float = DEFAULT_TEST_SIZE,
	random_state: int = DEFAULT_RANDOM_STATE,
) -> dict[str, Any]:
	"""Compare SDSS-only vs Gaia-enriched feature sets (C5.2.4 model comparison).

	Skips the Gaia variant when Gaia columns are absent from the dataset.
	"""
	df = _load_dataframe(data_path)

	X_base, y_base = select_features(df, include_gaia=False)
	baseline = _fit_eval(
		X_base,
		y_base,
		RandomForestClassifier(n_estimators=n_estimators, random_state=random_state, n_jobs=-1),
		test_size,
		random_state,
	)

	comparison: dict[str, Any] = {
		"baseline": {
			"features": sorted(X_base.columns.tolist()),
			"accuracy": baseline["accuracy"],
			"f1_macro": baseline["f1_macro"],
		},
	}

	has_gaia = any(c in df.columns for c in GAIA_FEATURES)
	if has_gaia:
		X_gaia, y_gaia = select_features(df, include_gaia=True)
		enriched = _fit_eval(
			X_gaia,
			y_gaia,
			RandomForestClassifier(n_estimators=n_estimators, random_state=random_state, n_jobs=-1),
			test_size,
			random_state,
		)
		comparison["gaia_enriched"] = {
			"features": sorted(X_gaia.columns.tolist()),
			"accuracy": enriched["accuracy"],
			"f1_macro": enriched["f1_macro"],
		}
		comparison["f1_macro_delta"] = float(enriched["f1_macro"] - baseline["f1_macro"])
	else:
		logger.warning("No Gaia columns found; skipping Gaia-enriched comparison")
		comparison["gaia_enriched"] = None

	return comparison


def main() -> None:
	"""CLI entry point: run the hyperparameter sweep on a curated dataset."""
	import sys

	data = (
		Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/raw/sdss/curated_features.parquet")
	)
	run_hyperparameter_search(data_path=data)


if __name__ == "__main__":
	main()
