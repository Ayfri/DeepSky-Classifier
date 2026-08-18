from pathlib import Path
import time
from typing import override

from astropy.table import Table
import httpx2
import pandas as pd
from pyvo.dal import TAPService
import pyvo.dal.tap as pyvo_tap
import requests
from tqdm.auto import tqdm

from src.etl.catalogs.base import CatalogExtractor
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

GAIA_TAP_URL = "https://gea.esac.esa.int/tap-server/tap"
GAIA_LOGIN_URL = "https://gea.esac.esa.int/tap-server/login"
GAIA_LOGIN_TIMEOUT_SECONDS = 30.0
GAIA_JOB_POLL_TIMEOUT_SECONDS = 120.0
GAIA_JOB_WAIT_TIMEOUT_SECONDS = 60.0
# The identifier join is cheap, so targets go up in a few large batches rather than hundreds of tiny
# ones (6000 targets resolve in a single ~3s job).
DEFAULT_BATCH_SIZE = 5000
DEFAULT_MAX_SEPARATION_ARCSEC = 1.5
UPLOAD_TABLE_NAME = "sdss_targets"

pyvo_tap.DEFAULT_JOB_POLL_TIMEOUT = int(GAIA_JOB_POLL_TIMEOUT_SECONDS)

# Joins the consortium's precomputed SDSS DR13 x Gaia DR3 cross-match instead of running our own
# cone search. The cone search scanned gaia_source (1.8e9 rows) per batch and kept getting killed by
# the archive; this is an indexed identifier join that returns in seconds. It is also the stricter
# match: the consortium uses the full astrometric covariance and a chance-alignment figure of merit,
# where a blind 1.5" cone picks up spurious neighbours.
# Crucially there is NO `parallax IS NOT NULL` filter: extragalactic sources rarely get a 5-parameter
# astrometric solution, so that filter silently deleted ~80% of the galaxies. A missing parallax is
# itself informative (see `gaia_has_astrometry` in src/ml/features.py) and must reach the model.
GAIA_TARGET_QUERY = """
SELECT
	targets.objid,
	gaia.source_id,
	gaia.parallax,
	gaia.pmdec,
	gaia.pmra,
	gaia.phot_g_mean_mag,
	xmatch.angular_distance AS match_sep_arcsec
FROM TAP_UPLOAD.{upload_table_name} AS targets
JOIN gaiadr3.sdssdr13_best_neighbour AS xmatch
	ON xmatch.original_ext_source_id = targets.objid
JOIN gaiadr3.gaia_source AS gaia
	ON gaia.source_id = xmatch.source_id
WHERE xmatch.angular_distance < {max_sep_arcsec}
"""

JOB_POLL_INTERVAL_SECONDS = 5
JOB_TERMINAL_PHASES = frozenset({"ABORTED", "COMPLETED", "ERROR"})
FETCH_RESULT_MAX_ATTEMPTS = 3
FETCH_RESULT_RETRY_SECONDS = 5


def _fetch_result_with_retry(job: object) -> Table:
	"""Retry ``fetch_result`` since long-poll connections occasionally drop mid-request."""
	last_exc: Exception | None = None
	for attempt in range(1, FETCH_RESULT_MAX_ATTEMPTS + 1):
		try:
			return job.fetch_result().to_table()  # type: ignore[attr-defined]
		except Exception as exc:
			last_exc = exc
			if attempt < FETCH_RESULT_MAX_ATTEMPTS:
				logger.warning(f"[Gaia] fetch_result attempt {attempt} failed: {exc}")
				time.sleep(FETCH_RESULT_RETRY_SECONDS)

	raise last_exc  # type: ignore[misc]


def _get_job_identifier(job: object) -> str:
	job_id = getattr(job, "job_id", None)
	if job_id is not None:
		return str(job_id)

	job_id = getattr(job, "jobid", None)
	if job_id is not None:
		return str(job_id)

	raise AttributeError("PyVO TAP job does not expose a job identifier")


def _create_authenticated_session(username: str, password: str) -> requests.Session:
	"""Log in to the Gaia archive to raise the anonymous-user upload quota.

	The login POST itself uses httpx2; the resulting cookies are bridged onto a
	``requests.Session`` because pyvo's ``TAPService`` hardcodes requests-specific
	behaviour internally (``stream=True``, ``requests.RequestException``).
	"""
	with httpx2.Client(timeout=GAIA_LOGIN_TIMEOUT_SECONDS) as client:
		response = client.post(GAIA_LOGIN_URL, data={"username": username, "password": password})
		response.raise_for_status()
		cookies = dict(response.cookies)

	if not cookies:
		raise RuntimeError("Gaia login did not return a session cookie")

	session = requests.Session()
	session.cookies.update(cookies)
	logger.info(f"[Gaia] Authenticated as {username!r}")
	return session


def _build_target_query(
	upload_table_name: str,
	max_sep_arcsec: float,
) -> str:
	return GAIA_TARGET_QUERY.format(
		upload_table_name=upload_table_name,
		max_sep_arcsec=max_sep_arcsec,
	)


def _prepare_target_upload(targets: pd.DataFrame) -> Table:
	"""Upload objid only: the cross-match is an identifier join, positions are no longer needed."""
	if "objid" not in targets.columns:
		raise KeyError("Missing target column for Gaia upload: ['objid']")

	upload = (
		targets.loc[:, ["objid"]]
		.dropna(subset=["objid"])
		.drop_duplicates(subset=["objid"])
		.astype({"objid": "int64"})
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
		(index, start, upload[start : start + batch_size])
		for index, start in enumerate(range(0, total_rows, batch_size), start=1)
	]


class GaiaExtractor(CatalogExtractor):
	catalog_name = "gaia"

	def __init__(
		self,
		tap_url: str = GAIA_TAP_URL,
		username: str | None = None,
		password: str | None = None,
	) -> None:
		session = (
			_create_authenticated_session(username, password) if username and password else None
		)
		self.service = TAPService(tap_url, session=session)

	@override
	def extract(
		self,
		targets: pd.DataFrame | None = None,
		batch_size: int = DEFAULT_BATCH_SIZE,
		max_sep_arcsec: float = DEFAULT_MAX_SEPARATION_ARCSEC,
		checkpoint_dir: Path | None = None,
		**kwargs: object,
	) -> pd.DataFrame:
		if targets is None:
			raise ValueError(
				"GaiaExtractor.extract requires targets: this catalog is cross-matched, not queried standalone"
			)

		return self._extract_for_targets(
			targets,
			batch_size=batch_size,
			max_sep_arcsec=max_sep_arcsec,
			checkpoint_dir=checkpoint_dir,
		)

	def _extract_for_targets(
		self,
		targets: pd.DataFrame,
		batch_size: int,
		max_sep_arcsec: float,
		checkpoint_dir: Path | None = None,
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
		if checkpoint_dir:
			checkpoint_dir.mkdir(parents=True, exist_ok=True)

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
				checkpoint_path = (
					checkpoint_dir / f"batch_{batch_index:03d}.parquet" if checkpoint_dir else None
				)
				if checkpoint_path and checkpoint_path.exists():
					df = pd.read_parquet(checkpoint_path)
					logger.info(
						f"[Gaia] Batch {batch_index}/{len(batches)} resumed from checkpoint "
						f"({len(df)} rows)"
					)
				else:
					try:
						df = self._run_query(
							query,
							job_label=f"[Gaia] TAP job {batch_index}/{len(batches)}",
							uploads={UPLOAD_TABLE_NAME: batch_upload},
						)
					except Exception:
						logger.exception(
							f"[Gaia] Batch {batch_index}/{len(batches)} failed, "
							"will retry on next run"
						)
						bar.update(1)
						continue
					if checkpoint_path:
						df.to_parquet(checkpoint_path)

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
		"""Run one TAP job to completion. Raises on any failure (empty results are not an error)."""
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
			raise RuntimeError(f"[Gaia] Job ended with phase: {current_phase}")

		table = _fetch_result_with_retry(job)
		df = table.to_pandas()
		logger.info(f"[Gaia] Retrieved {len(df)} records")
		return df
