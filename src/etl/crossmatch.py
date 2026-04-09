import pandas as pd
from astropy import units as u
from astropy.coordinates import SkyCoord

from src.utils.logger import setup_logger


logger = setup_logger(__name__)

DEFAULT_MAX_SEPARATION_ARCSEC = 1.5


def _prefix_secondary_columns(
	columns: pd.Index,
	protected_columns: set[str] | None = None,
) -> dict[str, str]:
	protected = protected_columns or set()
	return {
		column: f"gaia_{column}"
		for column in columns
		if column not in protected and not column.startswith("gaia_")
	}


def merge_catalogs(
	primary: pd.DataFrame,
	secondary: pd.DataFrame,
	on: str = "objid",
	how: str = "left",
	max_sep_arcsec: float = DEFAULT_MAX_SEPARATION_ARCSEC,
) -> pd.DataFrame:
	"""Cross-match catalog rows by sky position and attach Gaia features."""
	if secondary.empty:
		logger.warning("Secondary catalog is empty, returning primary unchanged")
		return primary

	if on in primary.columns and on in secondary.columns:
		secondary_payload = secondary.rename(
			columns=_prefix_secondary_columns(secondary.columns, protected_columns={on}),
		)
		merged = primary.merge(secondary_payload, on=on, how=how, sort=False)
		match_column = next(
			(
				column
				for column in secondary_payload.columns
				if column != on and secondary_payload[column].notna().any()
			),
			None,
		)
		match_count = int(merged[match_column].notna().sum()) if match_column else 0
		logger.info(
			f"Cross-match complete: {len(primary)} primary rows -> {len(merged)} merged rows "
			f"({match_count} key matches on {on})"
		)
		return merged

	if how != "left":
		logger.warning("Cross-match currently supports left joins only; forcing left")

	required_columns = {"dec", "ra"}
	if not required_columns.issubset(primary.columns) or not required_columns.issubset(
		secondary.columns,
	):
		logger.warning(
			"Missing coordinate columns for sky cross-match, returning primary unchanged",
		)
		return primary

	primary_coords = SkyCoord(
		ra=primary["ra"].astype(float).to_numpy() * u.deg,
		dec=primary["dec"].astype(float).to_numpy() * u.deg,
	)
	secondary_coords = SkyCoord(
		ra=secondary["ra"].astype(float).to_numpy() * u.deg,
		dec=secondary["dec"].astype(float).to_numpy() * u.deg,
	)

	match_idx, separations, _ = primary_coords.match_to_catalog_sky(secondary_coords)
	matched_mask = separations.arcsec <= max_sep_arcsec

	secondary_payload = secondary.reset_index(drop=True).rename(
		columns=_prefix_secondary_columns(
			secondary.columns,
			protected_columns={"dec", "ra"},
		),
	)
	secondary_payload = secondary_payload.drop(columns=["ra", "dec"], errors="ignore")

	merged = primary.reset_index(drop=True).copy()
	for column in secondary_payload.columns:
		merged[column] = pd.NA

	if matched_mask.any():
		matched_rows = secondary_payload.iloc[match_idx[matched_mask]].reset_index(
			drop=True,
		)
		merged.loc[matched_mask, secondary_payload.columns] = matched_rows.to_numpy()

	merged["gaia_match_sep_arcsec"] = pd.NA
	merged.loc[matched_mask, "gaia_match_sep_arcsec"] = separations.arcsec[
		matched_mask,
	]

	logger.info(
		f"Cross-match complete: {len(primary)} primary rows -> {len(merged)} merged rows"
	)
	return merged
