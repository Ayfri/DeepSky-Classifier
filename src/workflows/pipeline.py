"""Prefect-orchestrated DeepSky pipeline: extract -> curate -> train."""
from pathlib import Path

import pandas as pd
from prefect import flow, task

from src.core.config import PipelineConfig
from src.core.integrity import compute_sha256
from src.core.schemas import CuratedFeatureRecord, SDSSRawRecord
from src.etl.catalogs.gaia import GaiaExtractor
from src.etl.catalogs.sdss import SDSSExtractor
from src.etl.crossmatch import merge_catalogs
from src.etl.persist import persist_dataframe
from src.etl.validate import validate_dataframe
from src.ml.train import train_classifier
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


@task(retries=3, retry_delay_seconds=60, name="extract_sdss")
def extract_sdss_task(config: PipelineConfig) -> pd.DataFrame:
	extractor = SDSSExtractor(data_release=config.data_release)
	return extractor.extract(
		limit_per_class=config.limit_per_class,
		labels=config.target_labels,
	)


@task(retries=2, retry_delay_seconds=30, name="extract_gaia")
def extract_gaia_task(sdss_df: pd.DataFrame) -> pd.DataFrame:
	extractor = GaiaExtractor()
	return extractor.extract(targets=sdss_df)


@task(name="validate_sdss")
def validate_sdss_task(
	df: pd.DataFrame,
	config: PipelineConfig,
) -> pd.DataFrame:
	valid, quarantine = validate_dataframe(df, SDSSRawRecord)
	if not quarantine.empty:
		persist_dataframe(
			quarantine,
			config.quarantine_dir / "sdss_quarantine",
			fmt=config.output_format,
		)
	return valid


@task(name="enrich_gaia")
def enrich_gaia_task(
	sdss_df: pd.DataFrame,
	gaia_df: pd.DataFrame,
	config: PipelineConfig,
) -> pd.DataFrame:
	if gaia_df.empty:
		return sdss_df

	persist_dataframe(gaia_df, config.output_dir / "gaia_raw", fmt=config.output_format)
	return merge_catalogs(sdss_df, gaia_df, on="objid")


@task(name="curate_features")
def curate_features_task(
	df: pd.DataFrame,
	config: PipelineConfig,
) -> Path | None:
	df = df.copy()
	if "source" not in df.columns:
		df["source"] = "sdss"

	valid, quarantine = validate_dataframe(df, CuratedFeatureRecord)
	if not quarantine.empty:
		persist_dataframe(
			quarantine,
			config.quarantine_dir / "curated_quarantine",
			fmt=config.output_format,
		)

	if valid.empty:
		return None

	path = persist_dataframe(
		valid,
		config.output_dir / "curated_features",
		fmt=config.output_format,
	)
	sha = compute_sha256(path)
	logger.info(f"Curated dataset SHA-256: {sha}")
	return path


@task(name="train_model")
def train_model_task(data_path: Path, include_gaia: bool = False) -> Path:
	return train_classifier(data_path=data_path, include_gaia=include_gaia)


@flow(
	name="deepsky_full_pipeline",
	description="End-to-end: SDSS extraction -> Gaia enrichment -> curation -> training",
	log_prints=True,
)
def deepsky_pipeline(
	config: PipelineConfig | None = None,
	enrich_gaia: bool = True,
	train: bool = True,
) -> dict[str, str | None]:
	config = config or PipelineConfig()
	results: dict[str, str | None] = {}

	# --- Extract ---
	raw_sdss = extract_sdss_task(config)
	if raw_sdss.empty:
		print("SDSS extraction returned no data")
		return {"status": "failed", "reason": "empty_extraction"}

	persist_dataframe(raw_sdss, config.output_dir / "sdss_raw", fmt=config.output_format)

	# --- Validate ---
	valid_sdss = validate_sdss_task(raw_sdss, config)
	if valid_sdss.empty:
		print("No SDSS rows survived validation")
		return {"status": "failed", "reason": "validation_empty"}

	# --- Enrich ---
	enriched = valid_sdss
	if enrich_gaia:
		gaia_df = extract_gaia_task(valid_sdss)
		enriched = enrich_gaia_task(valid_sdss, gaia_df, config)

	# --- Curate ---
	curated_path = curate_features_task(enriched, config)
	if curated_path is None:
		print("Curation produced no valid rows")
		return {"status": "failed", "reason": "curation_empty"}

	results["curated_path"] = str(curated_path)

	# --- Train ---
	if train:
		model_path = train_model_task(curated_path, include_gaia=enrich_gaia)
		results["model_path"] = str(model_path)

	results["status"] = "success"
	print(f"Pipeline complete: {results}")
	return results


if __name__ == "__main__":
	deepsky_pipeline()
