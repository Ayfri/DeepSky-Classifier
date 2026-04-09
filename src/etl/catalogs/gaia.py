import time

import pandas as pd
import pyvo.dal.tap as pyvo_tap
from astropy.table import Table
from pyvo.dal import TAPService
from tqdm.auto import tqdm

from src.etl.catalogs.base import CatalogExtractor
from src.utils.logger import setup_logger


logger = setup_logger(__name__)

GAIA_TAP_URL = "https://gea.esac.esa.int/tap-server/tap"
GAIA_JOB_POLL_TIMEOUT_SECONDS = 30.0
GAIA_JOB_WAIT_TIMEOUT_SECONDS = 60.0
DEFAULT_BATCH_SIZE = 250
DEFAULT_MAX_SEPARATION_ARCSEC = 1.5
UPLOAD_TABLE_NAME = "sdss_targets"

pyvo_tap.DEFAULT_JOB_POLL_TIMEOUT = GAIA_JOB_POLL_TIMEOUT_SECONDS

GAIA_SOURCE_QUERY = """
SELECT TOP {limit}
	source_id,
	ra,
	dec,
	parallax,
	pmra,
	pmdec,
	phot_g_mean_mag
FROM gaiadr3.gaia_source
WHERE parallax IS NOT NULL
	AND pmra IS NOT NULL
	AND pmdec IS NOT NULL
ORDER BY source_id
"""

GAIA_TARGET_QUERY = """
SELECT
	targets.objid,
	gaia.source_id,
	gaia.parallax,
	gaia.pmdec,
	gaia.pmra,
	gaia.phot_g_mean_mag,
	DISTANCE(targets.ra, targets.dec, gaia.ra, gaia.dec) * 3600.0 AS match_sep_arcsec
FROM TAP_UPLOAD.{upload_table_name} AS targets
JOIN gaiadr3.gaia_source AS gaia
	ON DISTANCE(targets.ra, targets.dec, gaia.ra, gaia.dec) < {radius_deg}
WHERE gaia.parallax IS NOT NULL
	AND gaia.pmra IS NOT NULL
	AND gaia.pmdec IS NOT NULL
"""

JOB_POLL_INTERVAL_SECONDS = 5
JOB_TERMINAL_PHASES = frozenset({"ABORTED", "COMPLETED", "ERROR"})


def _get_job_identifier(job: object) -> str:
	job_id = getattr(job, "job_id", None)
	if job_id is not None:
		return str(job_id)

	job_id = getattr(job, "jobid", None)
	if job_id is not None:
		return str(job_id)

	raise AttributeError("PyVO TAP job does not expose a job identifier")


def _arcsec_to_degrees(value: float) -> float:
	return value / 3600.0


def _build_target_query(
	upload_table_name: str,
	max_sep_arcsec: float,
) -> str:
	return GAIA_TARGET_QUERY.format(
		upload_table_name=upload_table_name,
		radius_deg=_arcsec_to_degrees(max_sep_arcsec),
	)


def _prepare_target_upload(targets: pd.DataFrame) -> Table:
	required_columns = ["objid", "ra", "dec"]
	missing = [column for column in required_columns if column not in targets.columns]
	if missing:
		raise KeyError(f"Missing target columns for Gaia upload: {missing}")

	upload = (
		targets.loc[:, required_columns]
		.dropna(subset=["objid", "ra", "dec"])
		.drop_duplicates(subset=["objid"])
		.astype({"dec": "float64", "objid": "int64", "ra": "float64"})
		.sort_values("objid")
		.reset_index(drop=True)
	)
	return Table.from_pandas(upload)


def _select_nearest_matches(matches: pd.DataFrame) -> pd.DataFrame:
	if matches.empty:
		return matches

	return (
		matches.sort_values(["objid", "match_sep_arcsec", "source_id"])
		.drop_duplicates(subset=["objid"], keep="first")
		.reset_index(drop=True)
	)


def _iter_target_batches(upload: Table, batch_size: int) -> list[tuple[int, int, Table]]:
	total_rows = len(upload)
	return [
		(index, start, upload[start:start + batch_size])
		for index, start in enumerate(range(0, total_rows, batch_size), start=1)
	]


class GaiaExtractor(CatalogExtractor):
	catalog_name = "gaia"

	def __init__(self, tap_url: str = GAIA_TAP_URL):
		self.service = TAPService(tap_url)

	def extract(
		self,
		batch_size: int = DEFAULT_BATCH_SIZE,
		limit: int = 10000,
		targets: pd.DataFrame | None = None,
		max_sep_arcsec: float = DEFAULT_MAX_SEPARATION_ARCSEC,
		**kwargs: object,
	) -> pd.DataFrame:
		if targets is not None:
			return self._extract_for_targets(
				targets,
				batch_size=batch_size,
				max_sep_arcsec=max_sep_arcsec,
			)

		query = GAIA_SOURCE_QUERY.format(limit=limit)
		logger.info(f"[Gaia] Submitting async TAP job (limit={limit})")
		return self._run_query(query)

	def _extract_for_targets(
		self,
		targets: pd.DataFrame,
		batch_size: int,
		max_sep_arcsec: float,
	) -> pd.DataFrame:
		upload = _prepare_target_upload(targets)
		if len(upload) == 0:
			logger.warning("[Gaia] No valid SDSS targets available for upload")
			return pd.DataFrame()

		if batch_size < 1:
			raise ValueError("Gaia batch_size must be >= 1")

		batches = _iter_target_batches(upload, batch_size=batch_size)
		query = _build_target_query(UPLOAD_TABLE_NAME, max_sep_arcsec)
		logger.info(
			f"[Gaia] Querying {len(upload)} SDSS targets in {len(batches)} batches "
			f"(batch_size={batch_size}, max_sep_arcsec={max_sep_arcsec})"
		)
		collected: list[pd.DataFrame] = []
		with tqdm(
			total=len(batches),
			desc="[Gaia] Target batches",
			unit="batch",
			dynamic_ncols=True,
		) as bar:
			for batch_index, start, batch_upload in batches:
				end = start + len(batch_upload)
				bar.set_postfix_str(
					f"targets={start + 1}-{end}/{len(upload)}",
				)
				df = self._run_query(
					query,
					job_label=f"[Gaia] TAP job {batch_index}/{len(batches)}",
					uploads={UPLOAD_TABLE_NAME: batch_upload},
				)
				if not df.empty:
					collected.append(df)
				bar.update(1)

		if not collected:
			logger.warning("[Gaia] No Gaia matches were returned across all batches")
			return pd.DataFrame()

		matches = _select_nearest_matches(pd.concat(collected, ignore_index=True))
		logger.info(
			f"[Gaia] Retrieved {sum(len(df) for df in collected)} candidate matches; "
			f"kept {len(matches)} nearest Gaia rows"
		)
		return matches

	def _run_query(
		self,
		query: str,
		job_label: str = "[Gaia] TAP job",
		uploads: dict[str, Table] | None = None,
	) -> pd.DataFrame:
		try:
			job = self.service.submit_job(query, uploads=uploads)
			job.run()
			current_phase = job.phase

			with tqdm(
				total=1,
				desc=job_label,
				unit="job",
				dynamic_ncols=True,
			) as bar:
				while current_phase not in JOB_TERMINAL_PHASES:
					bar.set_postfix_str(f"phase={current_phase}")
					time.sleep(JOB_POLL_INTERVAL_SECONDS)
					try:
						current_phase = self.service.get_job(_get_job_identifier(job)).phase
					except Exception as exc:
						logger.warning(f"[Gaia] Refreshing job state failed: {exc}")
						continue

				bar.set_postfix_str(f"phase={current_phase}")
				bar.update(1)

			if current_phase != "COMPLETED":
				logger.error(f"[Gaia] Job ended with phase: {current_phase}")
				return pd.DataFrame()

			results = job.fetch_result().to_table()
			df = results.to_pandas()
			logger.info(f"[Gaia] Retrieved {len(df)} records")
			return df

		except Exception as exc:
			logger.error(f"[Gaia] TAP extraction failed: {exc}")
			return pd.DataFrame()
