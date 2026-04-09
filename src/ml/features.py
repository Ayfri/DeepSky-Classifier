import pandas as pd

from src.utils.logger import setup_logger


logger = setup_logger(__name__)

BASELINE_FEATURES: list[str] = [
	"dec",
	"g",
	"i",
	"r",
	"ra",
	"redshift",
	"u",
	"z_mag",
]

GAIA_FEATURES: list[str] = [
	"gaia_parallax",
	"gaia_pmdec",
	"gaia_pmra",
]

LABEL_COLUMN = "class_label"


def select_features(
	df: pd.DataFrame,
	include_gaia: bool = False,
) -> tuple[pd.DataFrame, pd.Series]:
	"""Extract feature matrix X and label vector y from a curated DataFrame."""
	feature_cols = list(BASELINE_FEATURES)

	if include_gaia:
		available_gaia = [c for c in GAIA_FEATURES if c in df.columns]
		feature_cols.extend(available_gaia)

	missing = [c for c in feature_cols if c not in df.columns]
	if missing:
		raise KeyError(f"Missing feature columns: {missing}")

	if LABEL_COLUMN not in df.columns:
		raise KeyError(f"Missing label column: {LABEL_COLUMN!r}")

	X = df[sorted(feature_cols)].copy()
	y = df[LABEL_COLUMN].copy()

	na_before = len(X)
	mask = X.notna().all(axis=1)
	X = X[mask]
	y = y[mask]

	dropped = na_before - len(X)
	if dropped > 0:
		logger.warning(f"Dropped {dropped} rows with NaN in feature columns")

	logger.info(f"Feature matrix: {X.shape[0]} rows x {X.shape[1]} features")
	return X, y
