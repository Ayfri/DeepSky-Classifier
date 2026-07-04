import pandas as pd
import pytest

from src.ml.stats import parallax_mannwhitney, redshift_kruskal


class TestRedshiftKruskal:
	def test_separated_groups_reject_null(self):
		df = pd.DataFrame(
			{
				"class_label": ["STAR"] * 20 + ["GALAXY"] * 20 + ["QSO"] * 20,
				"redshift": [0.0001] * 20 + [0.1] * 20 + [1.5] * 20,
			}
		)
		result = redshift_kruskal(df)
		assert result["p_value"] < 0.05
		assert result["reject_null"] is True
		assert result["n_groups"] == 3

	def test_similar_groups_do_not_reject_null(self):
		df = pd.DataFrame(
			{
				"class_label": ["STAR"] * 10 + ["GALAXY"] * 10,
				"redshift": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0] * 2,
			}
		)
		result = redshift_kruskal(df)
		assert result["p_value"] == pytest.approx(1.0)
		assert result["reject_null"] is False

	def test_nan_rows_are_dropped(self):
		df = pd.DataFrame(
			{
				"class_label": ["STAR", "STAR", "GALAXY", "GALAXY"],
				"redshift": [0.0, None, 0.5, 0.6],
			}
		)
		result = redshift_kruskal(df)
		assert result["n_groups"] == 2

	def test_missing_columns_raise(self):
		df = pd.DataFrame({"class_label": ["STAR"]})
		with pytest.raises(KeyError):
			redshift_kruskal(df)

	def test_single_group_raises(self):
		df = pd.DataFrame({"class_label": ["STAR"] * 5, "redshift": [0.1] * 5})
		with pytest.raises(ValueError, match="at least 2"):
			redshift_kruskal(df)


class TestParallaxMannWhitney:
	def test_separated_groups_reject_null(self):
		df = pd.DataFrame(
			{
				"class_label": ["STAR"] * 20 + ["GALAXY"] * 10 + ["QSO"] * 10,
				"gaia_parallax": [10.0] * 20 + [0.01] * 20,
			}
		)
		result = parallax_mannwhitney(df)
		assert result is not None
		assert result["p_value"] < 0.05
		assert result["reject_null"] is True
		assert result["n_stars"] == 20
		assert result["n_extragalactic"] == 20

	def test_missing_gaia_column_returns_none(self):
		df = pd.DataFrame({"class_label": ["STAR", "GALAXY"]})
		assert parallax_mannwhitney(df) is None

	def test_empty_group_returns_none(self):
		df = pd.DataFrame(
			{
				"class_label": ["STAR"] * 5,
				"gaia_parallax": [1.0] * 5,
			}
		)
		assert parallax_mannwhitney(df) is None
