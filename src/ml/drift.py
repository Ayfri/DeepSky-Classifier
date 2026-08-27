import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import TYPE_CHECKING, Any

import joblib
import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from sklearn.model_selection import train_test_split
from sqlalchemy import Engine, select

from src.core.models import PredictionLog
from src.ml.features import select_features
from src.utils.logger import setup_logger

if TYPE_CHECKING:
	from sklearn.ensemble import RandomForestClassifier

logger = setup_logger(__name__)

# The decision is taken on the KS statistic, not the p-value: with thousands of served predictions
# any tiny distribution shift yields a crushed p-value, which measures confidence, not magnitude.
KS_STATISTIC_THRESHOLD = 0.15

# A weekly retrain that loses more than this much F1 macro is a regression worth a red workflow.
F1_MAX_DROP = 0.01

# Below this many served predictions the KS test is noise, not signal: skip instead of alerting.
MIN_SERVED_PREDICTIONS = 50


def reference_max_proba(model_dir: Path) -> np.ndarray:
	"""Max predicted probability per test-set object, recomputed from the training artefacts.

	Nothing is stored at train time: the split is reproducible from ``run_metadata.json``
	(``random_state``, ``test_size``, ``include_gaia``), so the reference distribution is rebuilt
	on demand from the exact dataset the model was trained on. A dataset SHA mismatch means the
	reference would be computed on different data, which invalidates the comparison entirely.
	"""
	metadata: dict[str, Any] = json.loads(
		(model_dir / "run_metadata.json").read_text(encoding="utf-8")
	)
	data_path = Path(metadata["dataset_path"])
	if not data_path.exists():
		raise FileNotFoundError(f"Training dataset not found: {data_path}")

	actual_sha = hashlib.sha256(data_path.read_bytes()).hexdigest()
	if actual_sha != metadata["dataset_sha256"]:
		raise ValueError(
			f"Dataset SHA mismatch: metadata says {metadata['dataset_sha256'][:12]}..., "
			f"file is {actual_sha[:12]}..."
		)

	match data_path.suffix:
		case ".parquet":
			df = pd.read_parquet(data_path)
		case ".csv":
			df = pd.read_csv(data_path)
		case _:
			raise ValueError(f"Unsupported data format: {data_path.suffix!r}")

	X, y = select_features(df, include_gaia=bool(metadata["include_gaia"]))
	_, X_test, _, _ = train_test_split(
		X,
		y,
		test_size=float(metadata["test_size"]),
		random_state=int(metadata["random_state"]),
		stratify=y,
	)

	clf: RandomForestClassifier = joblib.load(metadata["model_path"])
	return np.asarray(clf.predict_proba(X_test).max(axis=1))


def served_max_proba(engine: Engine, model_sha256: str | None = None) -> np.ndarray:
	"""Max probabilities of every served prediction, optionally for one model version only."""
	query = select(PredictionLog.max_proba)
	if model_sha256 is not None:
		query = query.where(PredictionLog.model_sha256 == model_sha256)
	with engine.connect() as conn:
		values = conn.execute(query).scalars().all()
	return np.asarray(values, dtype="float64")


def check_probability_drift(
	served: np.ndarray,
	reference: np.ndarray,
	threshold: float = KS_STATISTIC_THRESHOLD,
) -> dict[str, Any]:
	"""Two-sample Kolmogorov-Smirnov test between served and test-set max_proba distributions."""
	statistic, p_value = scipy_stats.ks_2samp(served, reference)
	result: dict[str, Any] = {
		"statistic": float(statistic),
		"p_value": float(p_value),
		"n_served": len(served),
		"n_reference": len(reference),
		"threshold": threshold,
		"drift_detected": bool(statistic > threshold),
	}
	logger.info(
		f"KS drift check: statistic={statistic:.4f} (threshold {threshold}), "
		f"served={len(served)}, reference={len(reference)}, "
		f"drift={'YES' if result['drift_detected'] else 'no'}"
	)
	return result


def compare_f1(
	previous_metadata: Path,
	current_metadata: Path,
	max_drop: float = F1_MAX_DROP,
) -> dict[str, Any]:
	"""Compare f1_macro between two ``run_metadata.json`` files, flag a drop beyond ``max_drop``."""
	previous = json.loads(previous_metadata.read_text(encoding="utf-8"))
	current = json.loads(current_metadata.read_text(encoding="utf-8"))
	previous_f1 = float(previous["metrics"]["f1_macro"])
	current_f1 = float(current["metrics"]["f1_macro"])
	drop = previous_f1 - current_f1
	result: dict[str, Any] = {
		"previous_f1": previous_f1,
		"current_f1": current_f1,
		"drop": drop,
		"max_drop": max_drop,
		# Tolerance absorbs float64 subtraction noise: a drop of exactly max_drop must pass.
		"regression": bool(drop - max_drop > 1e-9),
	}
	logger.info(
		f"F1 comparison: {previous_f1:.4f} -> {current_f1:.4f} (drop {drop:+.4f}, "
		f"max allowed {max_drop}), regression={'YES' if result['regression'] else 'no'}"
	)
	return result


def main() -> None:
	"""CLI entry point. Exit code 1 signals drift or regression: a red workflow is the alert."""
	parser = argparse.ArgumentParser(prog="deepsky-drift")
	sub = parser.add_subparsers(dest="command", required=True)

	proba = sub.add_parser("proba", help="Compare served max_proba to the test-set distribution")
	proba.add_argument("--model-dir", type=Path, default=Path("models"))
	proba.add_argument("--threshold", type=float, default=KS_STATISTIC_THRESHOLD)
	proba.add_argument("--min-served", type=int, default=MIN_SERVED_PREDICTIONS)

	f1 = sub.add_parser("f1", help="Compare f1_macro between two run_metadata.json files")
	f1.add_argument("previous", type=Path)
	f1.add_argument("current", type=Path)
	f1.add_argument("--max-drop", type=float, default=F1_MAX_DROP)

	args = parser.parse_args()

	if args.command == "f1":
		result = compare_f1(args.previous, args.current, max_drop=args.max_drop)
		sys.exit(1 if result["regression"] else 0)

	from src.core.database import get_engine

	metadata: dict[str, Any] = json.loads(
		(args.model_dir / "run_metadata.json").read_text(encoding="utf-8")
	)
	served = served_max_proba(get_engine(), model_sha256=metadata["model_sha256"])
	if len(served) < args.min_served:
		logger.info(
			f"Only {len(served)} served predictions for this model (minimum {args.min_served}), "
			"skipping the KS test"
		)
		sys.exit(0)

	reference = reference_max_proba(args.model_dir)
	result = check_probability_drift(served, reference, threshold=args.threshold)
	sys.exit(1 if result["drift_detected"] else 0)


if __name__ == "__main__":
	main()
