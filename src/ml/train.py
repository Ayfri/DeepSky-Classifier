import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

from src.ml.evaluate import evaluate_model
from src.ml.features import LABEL_COLUMN, select_features
from src.utils.logger import setup_logger


logger = setup_logger(__name__)

DEFAULT_MODEL_DIR = Path("models")
DEFAULT_TEST_SIZE = 0.2
DEFAULT_N_ESTIMATORS = 100
DEFAULT_RANDOM_STATE = 42


def _sha256_file(path: Path) -> str:
	h = hashlib.sha256()
	with open(path, "rb") as f:
		for chunk in iter(lambda: f.read(65536), b""):
			h.update(chunk)
	return h.hexdigest()


def _sha256_bytes(data: bytes) -> str:
	return hashlib.sha256(data).hexdigest()


def train_classifier(
	data_path: Path,
	output_dir: Path = DEFAULT_MODEL_DIR,
	n_estimators: int = DEFAULT_N_ESTIMATORS,
	test_size: float = DEFAULT_TEST_SIZE,
	random_state: int = DEFAULT_RANDOM_STATE,
	include_gaia: bool = False,
) -> Path:
	output_dir.mkdir(parents=True, exist_ok=True)

	logger.info(f"Loading curated data from {data_path}")
	match data_path.suffix:
		case ".parquet":
			df = pd.read_parquet(data_path)
		case ".csv":
			df = pd.read_csv(data_path)
		case _:
			raise ValueError(f"Unsupported data format: {data_path.suffix!r}")

	dataset_sha = _sha256_file(data_path)
	logger.info(f"Dataset SHA-256: {dataset_sha}")

	X, y = select_features(df, include_gaia=include_gaia)
	labels = sorted(y.unique().tolist())

	X_train, X_test, y_train, y_test = train_test_split(
		X, y,
		test_size=test_size,
		random_state=random_state,
		stratify=y,
	)

	logger.info(
		f"Train/test split: {len(X_train)} train, {len(X_test)} test "
		f"({len(labels)} classes: {labels})"
	)

	clf = RandomForestClassifier(
		n_estimators=n_estimators,
		verbose=1,
		random_state=random_state,
		n_jobs=-1,
	)
	clf.fit(X_train, y_train)

	y_pred = clf.predict(X_test)
	metrics = evaluate_model(y_test, y_pred, labels=labels)

	# --- Persist model artifact ---
	model_path = output_dir / "rf_classifier.joblib"
	joblib.dump(clf, model_path)
	model_sha = _sha256_file(model_path)
	logger.info(f"Model saved to {model_path} (SHA-256: {model_sha})")

	# --- Persist run metadata ---
	metadata: dict[str, Any] = {
		"dataset_path": str(data_path),
		"dataset_sha256": dataset_sha,
		"feature_columns": sorted(X.columns.tolist()),
		"include_gaia": include_gaia,
		"label_column": LABEL_COLUMN,
		"labels": labels,
		"metrics": metrics,
		"model_path": str(model_path),
		"model_sha256": model_sha,
		"n_estimators": n_estimators,
		"random_state": random_state,
		"test_size": test_size,
		"timestamp": datetime.now(timezone.utc).isoformat(),
		"train_rows": len(X_train),
		"test_rows": len(X_test),
	}

	metadata_path = output_dir / "run_metadata.json"
	metadata_path.write_text(json.dumps(metadata, indent=2, default=str))
	logger.info(f"Run metadata saved to {metadata_path}")

	return model_path


if __name__ == "__main__":
	import sys

	data = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/raw/sdss/curated_features.parquet")
	train_classifier(data_path=data)
