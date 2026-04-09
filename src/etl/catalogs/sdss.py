import pandas as pd
from astropy.table import Table
from astroquery.sdss import SDSS
from tqdm.auto import tqdm

from src.core.models import CelestialBody
from src.etl.catalogs.base import CatalogExtractor
from src.utils.logger import setup_logger


logger = setup_logger(__name__)


class SDSSExtractor(CatalogExtractor):
	catalog_name = "sdss"

	def __init__(self, data_release: int = 17):
		self.data_release = data_release

	def extract(
		self,
		limit_per_class: int = 2000,
		labels: list[str] | None = None,
		**kwargs: object,
	) -> pd.DataFrame:
		target_labels = labels or ["GALAXY", "QSO", "STAR"]
		logger.info(f"[SDSS] Extracting {limit_per_class} objects per class: {target_labels}")

		collected: list[pd.DataFrame] = []
		for label in tqdm(
			target_labels,
			desc="[SDSS] Catalog classes",
			unit="class",
			dynamic_ncols=True,
		):
			df = self._fetch_class(label, limit_per_class)
			if not df.empty:
				collected.append(df)

		if not collected:
			logger.error("[SDSS] No data retrieved")
			return pd.DataFrame()

		return pd.concat(collected, ignore_index=True)

	def _fetch_class(self, label: str, limit: int) -> pd.DataFrame:
		logger.info(f"[SDSS] Fetching class: {label}")
		query = CelestialBody.build_sdss_query(limit=limit, label=label)

		try:
			result: Table | None = SDSS.query_sql(
				query, data_release=self.data_release,
			)
			if result:
				return result.to_pandas()
			logger.warning(f"[SDSS] Class {label} returned no results")
			return pd.DataFrame()
		except Exception as exc:
			logger.error(f"[SDSS] Extraction error for {label}: {exc}")
			return pd.DataFrame()
