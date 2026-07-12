from pathlib import Path

import pandas as pd
import pytest

from src.etl.persist import persist_dataframe


def test_persist_parquet_round_trip(tmp_path: Path) -> None:
	df = pd.DataFrame({"col_a": [1, 2], "col_b": [0.5, 1.5]})
	target = persist_dataframe(df, tmp_path / "out" / "dataset", fmt="parquet")
	assert target.suffix == ".parquet"
	loaded = pd.read_parquet(target)
	pd.testing.assert_frame_equal(df, loaded)


def test_persist_csv_round_trip(tmp_path: Path) -> None:
	df = pd.DataFrame({"col_a": [1, 2], "col_b": [0.5, 1.5]})
	target = persist_dataframe(df, tmp_path / "out" / "dataset", fmt="csv")
	assert target.suffix == ".csv"
	loaded = pd.read_csv(target)
	pd.testing.assert_frame_equal(df, loaded)


def test_persist_rejects_an_unsupported_format(tmp_path: Path) -> None:
	df = pd.DataFrame({"col_a": [1]})
	with pytest.raises(ValueError, match="Unsupported output format"):
		persist_dataframe(df, tmp_path / "dataset", fmt="feather")
