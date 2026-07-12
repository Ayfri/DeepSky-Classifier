from typing import override

from astroquery.sdss import SDSS
import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from src.core.models import CelestialBody
from src.etl.catalogs.base import CatalogExtractor
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

DEFAULT_N_PLATES = 60
DEFAULT_RANDOM_STATE = 42
SDSS_QUERY_TIMEOUT_SECONDS = 300


class SDSSExtractor(CatalogExtractor):
	catalog_name = "sdss"

	def __init__(
		self,
		data_release: int = 17,
		random_state: int = DEFAULT_RANDOM_STATE,
	) -> None:
		self.data_release = data_release
		self.random_state = random_state

	@override
	def extract(
		self,
		limit_per_class: int = 2000,
		labels: list[str] | None = None,
		n_plates: int = DEFAULT_N_PLATES,
		**kwargs: object,
	) -> pd.DataFrame:
		target_labels = labels or ["GALAXY", "QSO", "STAR"]
		logger.info(f"[SDSS] Extracting {limit_per_class} objects per class: {target_labels}")

		plates = self._sample_plates(n_plates)
		if not plates:
			logger.error("[SDSS] Could not sample any plate")
			return pd.DataFrame()

		collected: list[pd.DataFrame] = []
		for label in tqdm(
			target_labels,
			desc="[SDSS] Catalog classes",
			unit="class",
			dynamic_ncols=True,
		):
			df = self._fetch_class(label, limit_per_class, plates)
			if not df.empty:
				collected.append(df)

		if not collected:
			logger.error("[SDSS] No data retrieved")
			return pd.DataFrame()

		return pd.concat(collected, ignore_index=True)

	def _sample_plates(self, n_plates: int) -> list[int]:
		"""Draw a reproducible random subset of spectroscopic plates (first stage of the sample)."""
		try:
			result = SDSS.query_sql(
				CelestialBody.PLATE_QUERY,
				data_release=self.data_release,
				timeout=SDSS_QUERY_TIMEOUT_SECONDS,
			)
		except Exception:
			logger.exception("[SDSS] Plate listing failed")
			return []

		if not result:
			return []

		available = result.to_pandas()["plate"].to_numpy()
		size = min(n_plates, len(available))
		rng = np.random.default_rng(self.random_state)
		plates = sorted(int(p) for p in rng.choice(available, size=size, replace=False))
		logger.info(f"[SDSS] Sampled {len(plates)} plates out of {len(available)} available")
		return plates

	def _fetch_class(self, label: str, limit: int, plates: list[int]) -> pd.DataFrame:
		logger.info(f"[SDSS] Fetching class: {label}")
		query = CelestialBody.build_sdss_query(label=label, plates=plates)

		try:
			result = SDSS.query_sql(
				query,
				data_release=self.data_release,
				timeout=SDSS_QUERY_TIMEOUT_SECONDS,
			)
		except Exception:
			logger.exception("[SDSS] Extraction error for %s", label)
			return pd.DataFrame()

		if not result:
			logger.warning(f"[SDSS] Class {label} returned no results")
			return pd.DataFrame()

		pool = result.to_pandas()
		if len(pool) <= limit:
			logger.warning(
				f"[SDSS] Class {label}: pool of {len(pool)} rows is at or below the requested "
				f"{limit}; keeping all of them (raise n_plates for a larger pool)"
			)
			return pool

		# Second stage: draw the requested rows uniformly from the plate pool, never by scan order.
		sample = pool.sample(n=limit, random_state=self.random_state).reset_index(drop=True)
		logger.info(f"[SDSS] Class {label}: sampled {len(sample)} rows from a pool of {len(pool)}")
		return sample
