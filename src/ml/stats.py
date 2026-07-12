from typing import Any

import pandas as pd
from scipy import stats as scipy_stats

from src.ml.features import LABEL_COLUMN
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

EXTRAGALACTIC_LABELS: list[str] = ["GALAXY", "QSO"]

# Smallest positive float64. A p-value this extreme underflows to exactly 0.0, and reporting "p = 0"
# is wrong: the test only ever bounds p from above. Report the bound instead.
P_VALUE_FLOOR = 5e-324


def _format_p_value(p_value: float) -> str:
	return f"< {P_VALUE_FLOOR:g} (underflow)" if p_value == 0.0 else f"{p_value:.6g}"


def redshift_kruskal(df: pd.DataFrame) -> dict[str, Any]:
	"""Kruskal-Wallis H-test: does redshift differ across class_label groups?"""
	if "redshift" not in df.columns or LABEL_COLUMN not in df.columns:
		raise KeyError(f"Missing required columns: 'redshift', {LABEL_COLUMN!r}")

	groups = [
		g["redshift"].dropna().to_numpy()
		for _, g in df.groupby(LABEL_COLUMN, observed=True)
		if g["redshift"].notna().any()
	]
	if len(groups) < 2:
		raise ValueError("Kruskal-Wallis requires at least 2 non-empty groups")

	statistic, p_value = scipy_stats.kruskal(*groups)

	result: dict[str, Any] = {
		"statistic": float(statistic),
		"p_value": float(p_value),
		"p_value_underflow": bool(p_value == 0.0),
		"n_groups": len(groups),
		"reject_null": bool(p_value < 0.05),
	}
	logger.info(f"Kruskal-Wallis on redshift: H={statistic:.4f}, p={_format_p_value(p_value)}")
	return result


def parallax_mannwhitney(df: pd.DataFrame) -> dict[str, Any] | None:
	"""Mann-Whitney U-test: does gaia_parallax differ between stars and extragalactic objects?"""
	if "gaia_parallax" not in df.columns or LABEL_COLUMN not in df.columns:
		logger.warning("gaia_parallax column absent, skipping Mann-Whitney test")
		return None

	stars = df.loc[df[LABEL_COLUMN] == "STAR", "gaia_parallax"].dropna().to_numpy()
	extragalactic = (
		df.loc[df[LABEL_COLUMN].isin(EXTRAGALACTIC_LABELS), "gaia_parallax"].dropna().to_numpy()
	)

	if len(stars) == 0 or len(extragalactic) == 0:
		logger.warning("Not enough gaia_parallax data for Mann-Whitney test, skipping")
		return None

	statistic, p_value = scipy_stats.mannwhitneyu(stars, extragalactic, alternative="two-sided")

	result: dict[str, Any] = {
		"statistic": float(statistic),
		"p_value": float(p_value),
		"p_value_underflow": bool(p_value == 0.0),
		"n_stars": len(stars),
		"n_extragalactic": len(extragalactic),
		"reject_null": bool(p_value < 0.05),
	}
	logger.info(f"Mann-Whitney on gaia_parallax: U={statistic:.4f}, p={_format_p_value(p_value)}")
	return result
