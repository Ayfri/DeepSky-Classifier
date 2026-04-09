import pandas as pd
import pytest

from src.ml.features import BASELINE_FEATURES, GAIA_FEATURES, LABEL_COLUMN, select_features


def _make_df(n: int = 10, include_gaia: bool = False) -> pd.DataFrame:
	data = {col: [float(j) for j in range(n)] for col in BASELINE_FEATURES}
	data[LABEL_COLUMN] = ["STAR"] * (n // 2) + ["GALAXY"] * (n - n // 2)

	if include_gaia:
		for col in GAIA_FEATURES:
			data[col] = [float(j) * 0.1 for j in range(n)]

	return pd.DataFrame(data)


class TestSelectFeatures:
	def test_baseline_shape(self):
		df = _make_df(20)
		X, y = select_features(df)
		assert X.shape == (20, len(BASELINE_FEATURES))
		assert len(y) == 20

	def test_columns_are_sorted(self):
		df = _make_df(5)
		X, _ = select_features(df)
		assert list(X.columns) == sorted(X.columns)

	def test_gaia_features_included(self):
		df = _make_df(10, include_gaia=True)
		X, _ = select_features(df, include_gaia=True)
		expected_cols = len(BASELINE_FEATURES) + len(GAIA_FEATURES)
		assert X.shape[1] == expected_cols

	def test_missing_column_raises(self):
		df = _make_df(5).drop(columns=["redshift"])
		with pytest.raises(KeyError, match="redshift"):
			select_features(df)

	def test_missing_label_raises(self):
		df = _make_df(5).drop(columns=[LABEL_COLUMN])
		with pytest.raises(KeyError, match=LABEL_COLUMN):
			select_features(df)

	def test_nan_rows_dropped(self):
		df = _make_df(10)
		df.loc[0, "u"] = float("nan")
		df.loc[1, "g"] = float("nan")
		X, y = select_features(df)
		assert len(X) == 8
		assert len(y) == 8
