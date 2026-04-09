import pandas as pd

from src.etl.crossmatch import merge_catalogs


def test_merge_catalogs_prefers_key_merge_when_identifier_exists() -> None:
	primary = pd.DataFrame(
		{
			"dec": [2.0, 4.0],
			"objid": [101, 202],
			"ra": [1.0, 3.0],
		},
	)
	secondary = pd.DataFrame(
		{
			"match_sep_arcsec": [0.3],
			"objid": [202],
			"parallax": [7.5],
			"source_id": [999],
		},
	)

	merged = merge_catalogs(primary, secondary, on="objid")

	assert "gaia_match_sep_arcsec" in merged.columns
	assert "gaia_parallax" in merged.columns
	assert "gaia_source_id" in merged.columns
	assert pd.isna(merged.loc[0, "gaia_source_id"])
	assert merged.loc[1, "gaia_source_id"] == 999
	assert merged.loc[1, "gaia_parallax"] == 7.5
