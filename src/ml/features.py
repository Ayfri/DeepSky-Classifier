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

# Gaia only solves a 5-parameter astrometric solution for sources it can resolve as point-like and
# bright enough, which extragalactic objects usually are not. So a NULL parallax is not a missing
# value to be repaired, it is evidence that the source is extragalactic. We keep those rows (the RF
# handles NaN natively since sklearn 1.4) and hand the model the missingness itself as a feature.
GAIA_ASTROMETRY_FLAG = "gaia_has_astrometry"

LABEL_COLUMN = "class_label"


def select_features(
	df: pd.DataFrame,
	include_gaia: bool = False,
) -> tuple[pd.DataFrame, pd.Series]:
	"""Extract feature matrix X and label vector y from a curated DataFrame.

	Rows with a NaN in a baseline (SDSS) feature are dropped, since those are genuine gaps. NaNs in
	the Gaia columns are kept on purpose, see ``GAIA_ASTROMETRY_FLAG``.
	"""
	frame = df.copy()
	feature_cols = list(BASELINE_FEATURES)
	gaia_cols: list[str] = []

	if include_gaia:
		gaia_cols = [c for c in GAIA_FEATURES if c in frame.columns]
		feature_cols.extend(gaia_cols)
		if gaia_cols:
			frame[GAIA_ASTROMETRY_FLAG] = frame[gaia_cols].notna().all(axis=1).astype("float64")
			feature_cols.append(GAIA_ASTROMETRY_FLAG)

	missing = [c for c in feature_cols if c not in frame.columns]
	if missing:
		raise KeyError(f"Missing feature columns: {missing}")

	if LABEL_COLUMN not in frame.columns:
		raise KeyError(f"Missing label column: {LABEL_COLUMN!r}")

	X = frame[sorted(feature_cols)].copy()
	y = frame[LABEL_COLUMN].copy()

	# Only baseline gaps are disqualifying; Gaia gaps carry signal and stay in.
	required_cols = [c for c in X.columns if c not in gaia_cols]
	na_before = len(X)
	mask = X[required_cols].notna().all(axis=1)
	X = X[mask]
	y = y[mask]

	dropped = na_before - len(X)
	if dropped > 0:
		logger.warning(f"Dropped {dropped} rows with NaN in baseline feature columns")

	if gaia_cols:
		without_astrometry = int((X[GAIA_ASTROMETRY_FLAG] == 0.0).sum())
		logger.info(
			f"Kept {without_astrometry}/{len(X)} rows without a Gaia astrometric solution "
			f"(NaN retained as signal)"
		)

	logger.info(f"Feature matrix: {X.shape[0]} rows x {X.shape[1]} features")
	return X, y
