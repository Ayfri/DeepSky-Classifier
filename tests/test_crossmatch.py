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


def test_merge_catalogs_falls_back_to_sky_position_when_no_shared_identifier() -> None:
	"""Angular fallback: without a shared `objid`, rows are matched on their sky coordinates."""
	primary = pd.DataFrame(
		{
			"dec": [2.0, 40.0],
			"objid": [101, 202],
			"ra": [1.0, 30.0],
		},
	)
	# First source sits 0.36 arcsec from primary row 0; second is a full degree away from row 1.
	secondary = pd.DataFrame(
		{
			"dec": [2.0001, 41.0],
			"parallax": [7.5, 1.2],
			"ra": [1.0, 30.0],
			"source_id": [999, 888],
		},
	)

	merged = merge_catalogs(primary, secondary, on="objid", max_sep_arcsec=1.5)

	separation = merged.loc[0, "gaia_match_sep_arcsec"]

	assert merged.loc[0, "gaia_source_id"] == 999
	assert isinstance(separation, float)
	assert separation < 1.5
	assert pd.isna(merged.loc[1, "gaia_source_id"]), "a match beyond the radius must be rejected"
	assert pd.isna(merged.loc[1, "gaia_match_sep_arcsec"])
	assert "ra" in merged.columns
	assert "gaia_ra" not in merged.columns


def test_merge_catalogs_returns_primary_when_coordinates_are_missing() -> None:
	primary = pd.DataFrame({"objid": [101], "u": [18.2]})
	secondary = pd.DataFrame({"parallax": [7.5], "source_id": [999]})

	merged = merge_catalogs(primary, secondary, on="objid")

	assert merged.equals(primary)
