from pathlib import Path

import pandas as pd

from src.etl.persist import persist_dataframe


def test_persist_parquet_round_trip(tmp_path: Path) -> None:
	df = pd.DataFrame({"col_a": [1, 2], "col_b": [0.5, 1.5]})
	target = persist_dataframe(df, tmp_path / "out" / "dataset", fmt="parquet")
	assert target.suffix == ".parquet"
	loaded = pd.read_parquet(target)
	pd.testing.assert_frame_equal(df, loaded)
