"""SDSS-only ingestion pipeline (baseline path)."""
from pathlib import Path

from src.core.config import PipelineConfig
from src.core.schemas import SDSSRawRecord
from src.etl.catalogs.sdss import SDSSExtractor
from src.etl.persist import persist_dataframe
from src.etl.validate import validate_dataframe
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


def run_sdss_pipeline(config: PipelineConfig | None = None) -> Path | None:
	config = config or PipelineConfig()

	extractor = SDSSExtractor(data_release=config.data_release)
	raw_df = extractor.extract(
		limit_per_class=config.limit_per_class,
		labels=config.target_labels,
	)

	if raw_df.empty:
		logger.error("Extraction returned no data, aborting pipeline")
		return None

	valid_df, quarantine_df = validate_dataframe(raw_df, SDSSRawRecord)

	if not quarantine_df.empty:
		persist_dataframe(
			quarantine_df,
			config.quarantine_dir / "sdss_quarantine",
			fmt=config.output_format,
		)

	if valid_df.empty:
		logger.error("No rows survived validation, aborting pipeline")
		return None

	return persist_dataframe(
		valid_df,
		config.output_dir / "sdss_raw",
		fmt=config.output_format,
	)


if __name__ == "__main__":
	output = run_sdss_pipeline()
	if output:
		logger.info(f"Pipeline complete: {output}")
