"""Federated pipeline: extracts from multiple catalogs, cross-matches, and curates."""
from pathlib import Path

import pandas as pd
from tqdm.auto import tqdm

from src.core.config import PipelineConfig
from src.core.schemas import CuratedFeatureRecord, SDSSRawRecord
from src.etl.catalogs.gaia import GaiaExtractor
from src.etl.catalogs.sdss import SDSSExtractor
from src.etl.crossmatch import merge_catalogs
from src.etl.persist import persist_dataframe
from src.etl.validate import validate_dataframe
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def run_federated_pipeline(
	config: PipelineConfig | None = None,
	enrich_gaia: bool = True,
) -> Path | None:
	config = config or PipelineConfig()
	total_steps = 4 + int(enrich_gaia)

	with tqdm(
		total=total_steps,
		desc="[Pipeline] Federated flow",
		unit="step",
		dynamic_ncols=True,
	) as progress:
		# --- Extract SDSS baseline ---
		sdss = SDSSExtractor(data_release=config.data_release)
		raw_sdss = sdss.extract(
			limit_per_class=config.limit_per_class,
			labels=config.target_labels,
		)
		progress.set_postfix_str("sdss extracted")
		progress.update(1)

		if raw_sdss.empty:
			logger.error("SDSS extraction returned no data, aborting")
			return None

		persist_dataframe(
			raw_sdss,
			config.output_dir / "sdss_raw",
			fmt=config.output_format,
		)
		progress.set_postfix_str("sdss persisted")
		progress.update(1)

		# --- Validate SDSS records ---
		valid_sdss, quarantine = validate_dataframe(raw_sdss, SDSSRawRecord)

		if not quarantine.empty:
			persist_dataframe(
				quarantine,
				config.quarantine_dir / "sdss_quarantine",
				fmt=config.output_format,
			)

		if valid_sdss.empty:
			logger.error("No SDSS rows survived validation")
			return None

		curated = valid_sdss.copy()
		curated["source"] = "sdss"

		# --- Enrich with Gaia astrometry ---
		if enrich_gaia:
			gaia = GaiaExtractor()
			gaia_df = gaia.extract(targets=valid_sdss)
			progress.set_postfix_str("gaia extracted")
			progress.update(1)

			if not gaia_df.empty:
				persist_dataframe(
					gaia_df,
					config.output_dir / "gaia_raw",
					fmt=config.output_format,
				)
				curated = merge_catalogs(curated, gaia_df, on="objid")
				logger.info(
					f"Gaia enrichment: "
					f"{curated['gaia_source_id'].notna().sum()} rows matched"
				)
		else:
			progress.set_postfix_str("gaia skipped")
			progress.update(1)

		progress.set_postfix_str("curating")

		# --- Validate curated contract ---
		curated_valid, curated_quarantine = validate_dataframe(
			curated, CuratedFeatureRecord,
		)

		if not curated_quarantine.empty:
			persist_dataframe(
				curated_quarantine,
				config.quarantine_dir / "curated_quarantine",
				fmt=config.output_format,
			)

		if curated_valid.empty:
			logger.error("No curated rows survived validation")
			return None

		curated_path = persist_dataframe(
			curated_valid,
			config.output_dir / "curated_features",
			fmt=config.output_format,
		)
		progress.update(1)
		return curated_path


if __name__ == "__main__":
	output = run_federated_pipeline()
	if output:
		logger.info(f"Federated pipeline complete: {output}")
